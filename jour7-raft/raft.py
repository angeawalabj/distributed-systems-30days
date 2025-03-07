"""
Jour 7 — Raft : Algorithme de consensus
=========================================
Raft résout les limites du Bully (Jour 6) :

  Bully                     Raft
  ─────────────────────     ─────────────────────
  O(N²) messages            O(N) messages
  Split-brain possible      Impossible (quorum)
  Yo-Yo problem             Terms (mandats) l'évitent
  ID = priorité fixe        Vote : n'importe qui peut gagner
  Pas de log partagé        Log répliqué + cohérence forte

Les 3 sous-problèmes que Raft résout séparément :
  1. Leader Election  (avec terms + vote majoritaire)
  2. Log Replication  (AppendEntries + commit majoritaire)
  3. Safety           (un seul leader par term, jamais de perte)

Rôles d'un nœud Raft :
  FOLLOWER  → État par défaut. Attend les heartbeats du leader.
  CANDIDATE → Lance une élection quand le leader est silencieux.
  LEADER    → Dirige le cluster, réplique le log.

Concepts clés :
  Term       : mandat numéroté. Chaque élection démarre un nouveau term.
               Un leader d'un term inférieur est ignoré → pas de zombie.
  Quorum     : majorité (N/2 + 1). Une décision nécessite ce seuil.
               Garantit qu'il ne peut y avoir qu'un seul leader par term.
  Log entry  : commande + term + index. Commitée quand majorité l'a.
"""

import time
import threading
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


# ─── RÔLES ───────────────────────────────────────────────────────────────────

class Role(Enum):
    FOLLOWER  = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER    = "LEADER"


# ─── ENTRÉE DE LOG ───────────────────────────────────────────────────────────

@dataclass
class EntreeLog:
    """
    Une commande dans le log répliqué.
    index  : position dans le log (1-based)
    term   : mandat pendant lequel cette entrée a été créée
    commande : la donnée à répliquer (str pour simplifier)
    """
    index:    int
    term:     int
    commande: str

    def __str__(self):
        return f"[{self.index}|t{self.term}] {self.commande}"


# ─── RPCs RAFT ────────────────────────────────────────────────────────────────

@dataclass
class RequestVote:
    """Envoyé par un CANDIDATE pour demander des votes."""
    term:           int   # Term du candidat
    candidat_id:    int
    last_log_index: int   # Dernier index dans son log
    last_log_term:  int   # Term de cette dernière entrée


@dataclass
class RequestVoteReponse:
    term:         int
    vote_accorde: bool


@dataclass
class AppendEntries:
    """
    Envoyé par le LEADER pour :
      - Répliquer des entrées de log (entries non vide)
      - Heartbeat (entries vide)
    """
    term:          int
    leader_id:     int
    prev_log_index: int        # Index juste avant les nouvelles entrées
    prev_log_term:  int        # Term de prev_log_index
    entries:       list[EntreeLog]
    leader_commit: int         # Index de commit du leader


@dataclass
class AppendEntriesReponse:
    term:    int
    succes:  bool
    # Pour accélérer la resynchronisation après conflit
    conflit_index: int = 0
    conflit_term:  int = 0


# ─── NŒUD RAFT ────────────────────────────────────────────────────────────────

class NoeudRaft:
    """
    Implémentation complète d'un nœud Raft.

    États persistants (survivent aux crashs en prod) :
      current_term, voted_for, log

    États volatils :
      commit_index, last_applied, role
      (leader uniquement) next_index, match_index
    """

    # Timeouts (en secondes)
    ELECTION_TIMEOUT_MIN = 0.6
    ELECTION_TIMEOUT_MAX = 1.2
    HEARTBEAT_INTERVAL   = 0.2

    def __init__(self, noeud_id: int, cluster: "Cluster"):
        self.id      = noeud_id
        self.cluster = cluster

        # ── État persistant ──────────────────────────────────
        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        self.log: list[EntreeLog] = []   # index 0-based, entry.index 1-based

        # ── État volatil ─────────────────────────────────────
        self.commit_index: int = 0      # Plus grand index committé
        self.last_applied: int = 0      # Plus grand index appliqué à la SM
        self.role: Role = Role.FOLLOWER

        # ── État leader (réinitialisé à chaque élection) ─────
        # next_index[j]  : prochain index à envoyer à j
        # match_index[j] : plus grand index confirmé chez j
        self.next_index:  dict[int, int] = {}
        self.match_index: dict[int, int] = {}

        # ── Synchronisation ───────────────────────────────────
        self._lock               = threading.RLock()
        self._election_reset     = threading.Event()
        self._actif              = False
        self._election_timer: Optional[threading.Timer] = None

        # ── Machine à états (log appliqué) ───────────────────
        self.state_machine: dict[str, str] = {}

        # ── Journal des événements ────────────────────────────
        self.journal: list[tuple[float, str]] = []

    # ── Logging ──────────────────────────────────────────────────────────────

    def _log(self, msg: str):
        t = time.time() - self.cluster.t0
        entree = (t, f"[N{self.id}|t{self.current_term}|{self.role.value[:3]}] {msg}")
        self.journal.append(entree)
        self.cluster._log_global(entree)

    # ── Cycle de vie ─────────────────────────────────────────────────────────

    def demarrer(self):
        self._actif = True
        self._log("Démarrage → FOLLOWER")
        self._reset_election_timer()

    def arreter(self):
        with self._lock:
            self._actif = False
            self.role = Role.FOLLOWER
            if self._election_timer:
                self._election_timer.cancel()
        self._log("💀 Arrêté")

    def ressusciter(self):
        with self._lock:
            self._actif = True
            self.role = Role.FOLLOWER
            # On garde current_term, voted_for, log (persistants !)
        self._log("🔄 Remis en ligne")
        self._reset_election_timer()

    # ── Election Timer ────────────────────────────────────────────────────────

    def _reset_election_timer(self):
        """
        Remet à zéro le timer d'élection.
        Appelé à chaque heartbeat reçu ou vote accordé.

        Le timeout aléatoire est la clé de Raft :
        évite que tous les nœuds lancent une élection simultanément.
        """
        with self._lock:
            if not self._actif:
                return
            if self._election_timer:
                self._election_timer.cancel()
            timeout = random.uniform(
                self.ELECTION_TIMEOUT_MIN,
                self.ELECTION_TIMEOUT_MAX
            )
            self._election_timer = threading.Timer(timeout, self._declencher_election)
            self._election_timer.daemon = True
            self._election_timer.start()

    def _declencher_election(self):
        """Timeout écoulé sans nouvelles du leader → devenir CANDIDATE."""
        with self._lock:
            if not self._actif or self.role == Role.LEADER:
                return
            # Nouveau term
            self.current_term += 1
            self.role = Role.CANDIDATE
            self.voted_for = self.id   # On vote pour soi-même
            term = self.current_term

        self._log(f"⏰ Timeout → CANDIDATE (term={term})")
        self._lancer_vote(term)

    # ── Élection ─────────────────────────────────────────────────────────────

    def _lancer_vote(self, term: int):
        """
        Envoie RequestVote à tous les autres nœuds.
        Attend les réponses, comptabilise, se proclame leader si majorité.
        """
        votes = [1]  # On a notre propre vote
        votes_lock = threading.Lock()
        quorum = self.cluster.quorum()

        with self._lock:
            last_idx  = len(self.log)
            last_term = self.log[-1].term if self.log else 0

        rpc = RequestVote(
            term=term,
            candidat_id=self.id,
            last_log_index=last_idx,
            last_log_term=last_term,
        )

        self._log(f"🗳️  RequestVote term={term} à {self.cluster.autres(self.id)}")

        evenements = []
        for nid in self.cluster.autres(self.id):
            evt = threading.Event()
            evenements.append((nid, evt))
            threading.Thread(
                target=self._envoyer_request_vote,
                args=(nid, rpc, votes, votes_lock, quorum, term, evt),
                daemon=True
            ).start()

        # Attendre toutes les réponses (ou timeout)
        for _, evt in evenements:
            evt.wait(timeout=0.3)

    def _envoyer_request_vote(
        self, dest_id, rpc, votes, votes_lock, quorum, term, done_evt
    ):
        try:
            reponse = self.cluster.envoyer_request_vote(dest_id, rpc)
            if reponse is None:
                return

            with self._lock:
                # Si on voit un term plus grand → redevenir follower
                if reponse.term > self.current_term:
                    self._devenir_follower(reponse.term)
                    return

                # N'est-on encore CANDIDATE pour ce term ?
                if self.role != Role.CANDIDATE or self.current_term != term:
                    return

            if reponse.vote_accorde:
                with votes_lock:
                    votes[0] += 1
                    total = votes[0]
                self._log(f"  ← Vote de N{dest_id} ({total}/{quorum} requis)")
                if total >= quorum:
                    self._devenir_leader(term)
        finally:
            done_evt.set()

    # ── Transitions de rôle ───────────────────────────────────────────────────

    def _devenir_leader(self, term: int):
        with self._lock:
            if self.role != Role.CANDIDATE or self.current_term != term:
                return  # Trop tard, situation a changé
            self.role = Role.LEADER
            # Initialiser next_index et match_index
            last_idx = len(self.log)
            for nid in self.cluster.autres(self.id):
                self.next_index[nid]  = last_idx + 1
                self.match_index[nid] = 0

        self._log(f"👑 LEADER élu (term={term}, quorum={self.cluster.quorum()})")
        # Annuler le timer d'élection
        if self._election_timer:
            self._election_timer.cancel()
        # Démarrer les heartbeats
        threading.Thread(target=self._boucle_heartbeat, daemon=True).start()

    def _devenir_follower(self, term: int):
        """Redevenir follower quand on voit un term supérieur."""
        self._log(f"↩️  FOLLOWER (term plus récent détecté: {term})")
        self.current_term = term
        self.role         = Role.FOLLOWER
        self.voted_for    = None
        self._reset_election_timer()

    # ── Heartbeat et réplication ──────────────────────────────────────────────

    def _boucle_heartbeat(self):
        """Le leader envoie des AppendEntries périodiquement."""
        while self._actif:
            with self._lock:
                if self.role != Role.LEADER:
                    break
            self._repliquer_vers_tous()
            time.sleep(self.HEARTBEAT_INTERVAL)

    def _repliquer_vers_tous(self):
        """Envoie AppendEntries à chaque follower (heartbeat ou réplication)."""
        for nid in self.cluster.autres(self.id):
            threading.Thread(
                target=self._repliquer_vers,
                args=(nid,),
                daemon=True
            ).start()

    def _repliquer_vers(self, dest_id: int):
        with self._lock:
            if self.role != Role.LEADER or not self._actif:
                return
            ni         = self.next_index.get(dest_id, 1)
            prev_idx   = ni - 1
            prev_term  = self.log[prev_idx - 1].term if prev_idx > 0 and prev_idx <= len(self.log) else 0
            entries    = self.log[ni - 1:]   # Entrées à envoyer
            term       = self.current_term
            commit_idx = self.commit_index

        rpc = AppendEntries(
            term=term,
            leader_id=self.id,
            prev_log_index=prev_idx,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=commit_idx,
        )

        reponse = self.cluster.envoyer_append_entries(dest_id, rpc)
        if reponse is None:
            return

        with self._lock:
            if reponse.term > self.current_term:
                self._devenir_follower(reponse.term)
                return
            if self.role != Role.LEADER:
                return

            if reponse.succes:
                if entries:
                    self.match_index[dest_id] = prev_idx + len(entries)
                    self.next_index[dest_id]  = self.match_index[dest_id] + 1
                    self._maj_commit_index()
            else:
                # Recul d'un pas (optimisation possible mais on garde simple)
                self.next_index[dest_id] = max(1, ni - 1)

    def _maj_commit_index(self):
        """
        Avance commit_index si une majorité de nœuds a répliqué une entrée.
        C'est la garantie de durabilité de Raft.
        """
        quorum = self.cluster.quorum()
        n = len(self.log)
        for idx in range(n, self.commit_index, -1):
            if self.log[idx - 1].term != self.current_term:
                break
            # Compter combien de nœuds ont cet index
            replicates = 1 + sum(
                1 for mi in self.match_index.values() if mi >= idx
            )
            if replicates >= quorum:
                if idx > self.commit_index:
                    self._log(f"✅ Commit index avancé à {idx} ({replicates}/{quorum} nœuds)")
                    self.commit_index = idx
                    self._appliquer_log()
                break

    def _appliquer_log(self):
        """Applique les entrées committées à la state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entree = self.log[self.last_applied - 1]
            # State machine simple : "SET key=val"
            if entree.commande.startswith("SET "):
                parts = entree.commande[4:].split("=", 1)
                if len(parts) == 2:
                    self.state_machine[parts[0]] = parts[1]

    # ── Réception des RPCs ────────────────────────────────────────────────────

    def recevoir_request_vote(self, rpc: RequestVote) -> RequestVoteReponse:
        with self._lock:
            if not self._actif:
                return RequestVoteReponse(term=self.current_term, vote_accorde=False)

            # Mettre à jour le term si nécessaire
            if rpc.term > self.current_term:
                self._devenir_follower(rpc.term)

            # Refuser si term candidat < notre term
            if rpc.term < self.current_term:
                return RequestVoteReponse(term=self.current_term, vote_accorde=False)

            # Vérification du vote
            dejà_voté = self.voted_for is not None and self.voted_for != rpc.candidat_id

            # Vérification de la fraîcheur du log (log up-to-date check)
            mon_last_term  = self.log[-1].term if self.log else 0
            mon_last_index = len(self.log)
            log_ok = (
                rpc.last_log_term > mon_last_term or
                (rpc.last_log_term == mon_last_term and rpc.last_log_index >= mon_last_index)
            )

            if not dejà_voté and log_ok:
                self.voted_for = rpc.candidat_id
                self._reset_election_timer()
                self._log(f"✅ Vote accordé à N{rpc.candidat_id} (term={rpc.term})")
                return RequestVoteReponse(term=self.current_term, vote_accorde=True)
            else:
                raison = "déjà voté" if dejà_voté else "log pas à jour"
                self._log(f"❌ Vote refusé à N{rpc.candidat_id} ({raison})")
                return RequestVoteReponse(term=self.current_term, vote_accorde=False)

    def recevoir_append_entries(self, rpc: AppendEntries) -> AppendEntriesReponse:
        with self._lock:
            if not self._actif:
                return AppendEntriesReponse(term=self.current_term, succes=False)

            if rpc.term > self.current_term:
                self._devenir_follower(rpc.term)

            # Rejeter si term obsolète
            if rpc.term < self.current_term:
                return AppendEntriesReponse(term=self.current_term, succes=False)

            # C'est un message valide du leader → reset timer
            self.role = Role.FOLLOWER
            self._reset_election_timer()

            # Vérifier la cohérence du log au point d'insertion
            if rpc.prev_log_index > 0:
                if rpc.prev_log_index > len(self.log):
                    return AppendEntriesReponse(
                        term=self.current_term, succes=False,
                        conflit_index=len(self.log) + 1
                    )
                if self.log[rpc.prev_log_index - 1].term != rpc.prev_log_term:
                    conflit_term = self.log[rpc.prev_log_index - 1].term
                    # Trouver le premier index de ce term conflictuel
                    ci = rpc.prev_log_index
                    while ci > 1 and self.log[ci - 2].term == conflit_term:
                        ci -= 1
                    return AppendEntriesReponse(
                        term=self.current_term, succes=False,
                        conflit_index=ci, conflit_term=conflit_term
                    )

            # Insérer les nouvelles entrées (en supprimant les conflits)
            for i, entree in enumerate(rpc.entries):
                idx = rpc.prev_log_index + i + 1
                if idx <= len(self.log):
                    if self.log[idx - 1].term != entree.term:
                        # Conflit → tronquer le log ici
                        self.log = self.log[:idx - 1]
                        self.log.append(entree)
                else:
                    self.log.append(entree)

            # Mettre à jour commit_index
            if rpc.leader_commit > self.commit_index:
                self.commit_index = min(rpc.leader_commit, len(self.log))
                self._appliquer_log()

            if rpc.entries:
                self._log(f"📥 AppendEntries: +{len(rpc.entries)} entrées (commit={self.commit_index})")

            return AppendEntriesReponse(term=self.current_term, succes=True)

    # ── API cliente ───────────────────────────────────────────────────────────

    def soumettre(self, commande: str) -> bool:
        """
        Soumet une commande au leader.
        Retourne True si la commande est acceptée (pas encore committée).
        """
        with self._lock:
            if self.role != Role.LEADER:
                return False
            entree = EntreeLog(
                index=len(self.log) + 1,
                term=self.current_term,
                commande=commande,
            )
            self.log.append(entree)
            self._log(f"📝 Commande soumise : {entree}")
        # Répliquer immédiatement
        self._repliquer_vers_tous()
        return True

    def statut(self) -> str:
        with self._lock:
            icone = {"LEADER": "👑", "CANDIDATE": "🗳️ ", "FOLLOWER": "🟢"}.get(
                self.role.value, "?"
            )
            mort = "💀 MORT  " if not self._actif else ""
            return (
                f"N{self.id} {mort}{icone} {self.role.value:<10} "
                f"term={self.current_term:<3} "
                f"log={len(self.log):<3} "
                f"commit={self.commit_index:<3} "
                f"SM={dict(list(self.state_machine.items())[:2])}"
            )


# ─── CLUSTER ─────────────────────────────────────────────────────────────────

class Cluster:
    """Simule le réseau entre les nœuds Raft."""

    def __init__(self, n: int, latence_ms: float = 15, taux_perte: float = 0.0):
        self.t0          = time.time()
        self.latence     = latence_ms / 1000
        self.taux_perte  = taux_perte
        self.noeuds: dict[int, NoeudRaft] = {}
        self._journal:   list[tuple[float, str]] = []
        self._jlock      = threading.Lock()

        for i in range(1, n + 1):
            n_obj = NoeudRaft(i, self)
            self.noeuds[i] = n_obj

    def _log_global(self, e):
        with self._jlock:
            self._journal.append(e)

    def quorum(self) -> int:
        return len(self.noeuds) // 2 + 1

    def autres(self, nid: int) -> list[int]:
        return [i for i in self.noeuds if i != nid]

    def demarrer(self):
        self.t0 = time.time()
        for n in self.noeuds.values():
            n.demarrer()

    def arreter_tous(self):
        for n in self.noeuds.values():
            n._actif = False
            if n._election_timer:
                n._election_timer.cancel()

    def leader(self) -> Optional[NoeudRaft]:
        for n in self.noeuds.values():
            if n.role == Role.LEADER and n._actif:
                return n
        return None

    def attendre_leader(self, timeout=6.0) -> Optional[NoeudRaft]:
        debut = time.time()
        while time.time() - debut < timeout:
            l = self.leader()
            if l:
                # Attendre que tous les vivants le reconnaissent
                vivants = [n for n in self.noeuds.values() if n._actif]
                if all(n.current_term == l.current_term for n in vivants):
                    time.sleep(0.1)
                    return l
            time.sleep(0.05)
        return self.leader()

    def _simuler_reseau(self, fn):
        """Wrapper : délai + perte."""
        if random.random() < self.taux_perte:
            return None
        time.sleep(self.latence + random.uniform(0, self.latence * 0.5))
        return fn()

    def envoyer_request_vote(self, dest_id: int, rpc: RequestVote):
        dest = self.noeuds.get(dest_id)
        if not dest or not dest._actif:
            return None
        return self._simuler_reseau(lambda: dest.recevoir_request_vote(rpc))

    def envoyer_append_entries(self, dest_id: int, rpc: AppendEntries):
        dest = self.noeuds.get(dest_id)
        if not dest or not dest._actif:
            return None
        return self._simuler_reseau(lambda: dest.recevoir_append_entries(rpc))

    def afficher_journal(self, n=30):
        with self._jlock:
            entrees = sorted(self._journal, key=lambda e: e[0])[-n:]
        print(f"\n  {'t':>6}  Événement")
        print("  " + "─"*58)
        for t, msg in entrees:
            print(f"  +{t:5.2f}s  {msg}")

    def afficher_etat(self):
        print(f"\n  État du cluster (quorum={self.quorum()}) :")
        print("  " + "─"*60)
        for nid in sorted(self.noeuds):
            print(f"  {self.noeuds[nid].statut()}")
