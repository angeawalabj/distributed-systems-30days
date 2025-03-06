"""
Jour 6 — Bully Algorithm : Élection de leader
===============================================
Problème : Un cluster a besoin d'un leader unique (coordinateur)
pour prendre des décisions centralisées (qui écrit, qui répartit
le travail, qui gère les verrous distribués...).

Quand le leader tombe (Jour 4 : Heartbeat le détecte),
les nœuds doivent en élire un nouveau automatiquement —
sans intervention humaine, sans point de coordination externe.

Algorithme du Bully (Garcia-Molina, 1982) :
  Principe : le nœud avec le plus grand ID "intimide" (bully)
  les autres et s'impose comme leader.

  Trois types de messages :
    ELECTION    → "Je lance une élection, es-tu plus grand que moi ?"
    OK          → "Oui, je suis plus grand, je prends la main"
    COORDINATOR → "J'ai gagné, je suis le nouveau leader"

  Règles :
    1. N'importe quel nœud peut lancer une élection
    2. Un nœud envoie ELECTION à tous les nœuds avec ID > lui
    3. Si quelqu'un répond OK → le lanceur abandonne
    4. Si personne ne répond → le lanceur s'autoproclame COORDINATOR
    5. Le COORDINATOR est broadcast à tous

  Propriété garantie :
    Le nœud vivant avec le plus grand ID devient toujours leader.
"""

import time
import threading
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


# ─── MESSAGES ────────────────────────────────────────────────────────────────

class TypeMessage(Enum):
    ELECTION    = "ELECTION"
    OK          = "OK"
    COORDINATOR = "COORDINATOR"
    HEARTBEAT   = "HEARTBEAT"   # Pour détecter la mort du leader (Jour 4 !)


@dataclass
class Message:
    type:        TypeMessage
    expediteur:  int           # ID du nœud émetteur
    destinataire: int          # ID du nœud destinataire
    ts:          float = field(default_factory=time.time)


# ─── ÉTAT D'UN NŒUD ──────────────────────────────────────────────────────────

class EtatNoeud(Enum):
    NORMAL    = "NORMAL"      # Fonctionnement standard
    ELECTION  = "ELECTION"    # Participe à une élection en cours
    LEADER    = "LEADER"      # Est le leader actuel
    MORT      = "MORT"        # Simulé hors-ligne


# ─── NŒUD DU CLUSTER ─────────────────────────────────────────────────────────

class Noeud:
    """
    Simule un nœud dans le cluster Bully.

    Chaque nœud connaît tous les autres (réseau complet).
    La communication passe par le Réseau simulé (pas de sockets réels).

    Subtilités implémentées :
      • Timeout sur les réponses OK    → si personne ne répond, on gagne
      • Réélection si le leader meurt  → intégration Heartbeat (Jour 4)
      • Nœud qui se remet en ligne     → relance une élection
      • Messages perdus simulables     → réalisme réseau
    """

    TIMEOUT_OK         = 0.4   # Attente d'un OK après ELECTION (secondes)
    TIMEOUT_COORDINATOR = 0.8  # Attente du COORDINATOR après OK
    INTERVALLE_HB      = 0.3   # Fréquence des heartbeats vers le leader
    TIMEOUT_LEADER     = 0.9   # Si le leader ne répond plus → réélection

    def __init__(self, noeud_id: int, reseau: "Reseau"):
        self.id       = noeud_id
        self.reseau   = reseau
        self.etat     = EtatNoeud.NORMAL
        self.leader_id: Optional[int] = None

        self._lock           = threading.RLock()
        self._actif          = False
        self._en_election    = False
        self._attend_coordinator = False
        self._dernier_hb_leader  = 0.0

        # Journal local des événements
        self.journal: list[tuple[float, str]] = []
        self._t0 = 0.0  # Sera fixé par le réseau

    def _log(self, msg: str):
        t = time.time() - self._t0
        entree = (t, f"[N{self.id}] {msg}")
        self.journal.append(entree)
        self.reseau._log_global(entree)

    # ── Cycle de vie ──────────────────────────────────────────────────────────

    def demarrer(self):
        self._actif = True
        self._t0 = self.reseau.t0
        threading.Thread(target=self._boucle_principale, daemon=True,
                         name=f"node-{self.id}").start()

    def arreter(self):
        with self._lock:
            self._actif = False
            self.etat = EtatNoeud.MORT
        self._log("💀 Arrêté (simulé mort)")

    def ressusciter(self):
        """Remet le nœud en ligne et lance immédiatement une élection."""
        with self._lock:
            self._actif = True
            self.etat   = EtatNoeud.NORMAL
            self.leader_id = None
            self._en_election = False
        self._log("🔄 Remis en ligne → lance une élection")
        threading.Thread(target=self._boucle_principale, daemon=True,
                         name=f"node-{self.id}").start()
        self.lancer_election(raison="remise en ligne")

    def _boucle_principale(self):
        """Surveille le leader et déclenche une réélection si nécessaire."""
        # Petite attente initiale aléatoire pour éviter tempête au démarrage
        time.sleep(random.uniform(0.05, 0.15))

        while self._actif:
            time.sleep(0.1)
            with self._lock:
                if self.etat == EtatNoeud.LEADER:
                    # Le leader envoie des heartbeats à tout le monde
                    self._envoyer_heartbeats()
                elif self.etat == EtatNoeud.NORMAL and self.leader_id is not None:
                    # Vérifier que le leader est toujours vivant
                    silence = time.time() - self._dernier_hb_leader
                    if silence > self.TIMEOUT_LEADER:
                        self._log(f"⚠️  Leader N{self.leader_id} silencieux depuis {silence:.2f}s → élection")
                        self.etat = EtatNoeud.NORMAL
                        self.leader_id = None
                        # Lancer hors du lock
                        threading.Thread(target=self.lancer_election,
                                         args=("leader mort",), daemon=True).start()

    def _envoyer_heartbeats(self):
        """Le leader signale qu'il est vivant (Jour 4)."""
        for nid in self.reseau.noeuds:
            if nid != self.id:
                self.reseau.envoyer(Message(
                    type=TypeMessage.HEARTBEAT,
                    expediteur=self.id,
                    destinataire=nid,
                ))

    # ── Algorithme du Bully ───────────────────────────────────────────────────

    def lancer_election(self, raison: str = ""):
        """
        Point d'entrée de l'algorithme.
        Envoie ELECTION à tous les nœuds avec un ID supérieur.
        """
        with self._lock:
            if self._en_election:
                return  # Déjà en cours
            self._en_election = True
            self.etat = EtatNoeud.ELECTION

        raison_str = f" ({raison})" if raison else ""
        self._log(f"🗳️  Lance ELECTION{raison_str}")

        # Cherche les nœuds avec ID > soi
        superieurs = [nid for nid in self.reseau.noeuds if nid > self.id]

        if not superieurs:
            # Je suis le plus grand → je gagne directement
            self._se_proclamer_coordinateur()
            return

        # Envoie ELECTION à tous les supérieurs
        for nid in superieurs:
            self.reseau.envoyer(Message(
                type=TypeMessage.ELECTION,
                expediteur=self.id,
                destinataire=nid,
            ))

        # Attend un OK pendant TIMEOUT_OK
        recu_ok = threading.Event()
        self.reseau._registre_ok[self.id] = recu_ok

        if recu_ok.wait(timeout=self.TIMEOUT_OK):
            # Quelqu'un de plus grand a répondu → on attend le COORDINATOR
            self._log(f"📨 OK reçu → j'attends le COORDINATOR")
            self._attend_coordinator = True
            # Si le COORDINATOR n'arrive pas → relancer
            threading.Thread(target=self._timeout_coordinator, daemon=True).start()
        else:
            # Personne n'a répondu → je gagne
            self._log(f"⏰ Timeout OK — personne de plus grand → je gagne")
            self._se_proclamer_coordinateur()

    def _timeout_coordinator(self):
        """Si le COORDINATOR n'arrive pas dans les temps, relancer."""
        time.sleep(self.TIMEOUT_COORDINATOR)
        with self._lock:
            if self._attend_coordinator and self._actif:
                self._log("⏰ Timeout COORDINATOR → relance élection")
                self._attend_coordinator = False
                self._en_election = False
        if self._actif:
            self.lancer_election(raison="timeout coordinator")

    def _se_proclamer_coordinateur(self):
        """S'autoproclame leader et broadcast le message COORDINATOR."""
        with self._lock:
            self.etat      = EtatNoeud.LEADER
            self.leader_id = self.id
            self._en_election = False
            self._attend_coordinator = False

        self._log(f"👑 Je suis le nouveau COORDINATEUR (leader)")

        # Broadcast COORDINATOR à tout le monde
        for nid in self.reseau.noeuds:
            if nid != self.id:
                self.reseau.envoyer(Message(
                    type=TypeMessage.COORDINATOR,
                    expediteur=self.id,
                    destinataire=nid,
                ))

    # ── Réception des messages ────────────────────────────────────────────────

    def recevoir(self, msg: Message):
        """Dispatch des messages entrants."""
        if not self._actif:
            return  # Nœud mort → ignore tout

        if msg.type == TypeMessage.ELECTION:
            self._on_election(msg)
        elif msg.type == TypeMessage.OK:
            self._on_ok(msg)
        elif msg.type == TypeMessage.COORDINATOR:
            self._on_coordinator(msg)
        elif msg.type == TypeMessage.HEARTBEAT:
            self._on_heartbeat(msg)

    def _on_election(self, msg: Message):
        """Reçoit un ELECTION d'un nœud inférieur → répond OK et lance sa propre élection."""
        self._log(f"📩 ELECTION reçu de N{msg.expediteur} → répond OK")
        self.reseau.envoyer(Message(
            type=TypeMessage.OK,
            expediteur=self.id,
            destinataire=msg.expediteur,
        ))
        # Lance sa propre élection si pas déjà en cours
        if not self._en_election:
            threading.Thread(target=self.lancer_election,
                             args=(f"provoqué par N{msg.expediteur}",),
                             daemon=True).start()

    def _on_ok(self, msg: Message):
        """Reçoit un OK → signale l'événement d'attente."""
        self._log(f"📩 OK reçu de N{msg.expediteur}")
        evt = self.reseau._registre_ok.get(self.id)
        if evt:
            evt.set()

    def _on_coordinator(self, msg: Message):
        """Reçoit l'annonce du nouveau leader."""
        with self._lock:
            self.leader_id         = msg.expediteur
            self.etat              = EtatNoeud.NORMAL
            self._en_election      = False
            self._attend_coordinator = False
            self._dernier_hb_leader = time.time()
        self._log(f"👑 Nouveau leader : N{msg.expediteur}")

    def _on_heartbeat(self, msg: Message):
        """Reçoit un heartbeat du leader → reset le timer."""
        with self._lock:
            if msg.expediteur == self.leader_id:
                self._dernier_hb_leader = time.time()

    def statut(self) -> str:
        with self._lock:
            role = "👑 LEADER" if self.etat == EtatNoeud.LEADER else \
                   "💀 MORT"   if self.etat == EtatNoeud.MORT   else \
                   "🗳️  ÉLECTION" if self.etat == EtatNoeud.ELECTION else "🟢 NORMAL"
            leader = f"(leader=N{self.leader_id})" if self.leader_id and self.etat != EtatNoeud.LEADER else ""
            return f"N{self.id:<2} {role:<14} {leader}"


# ─── RÉSEAU SIMULÉ ────────────────────────────────────────────────────────────

class Reseau:
    """
    Simule le réseau entre les nœuds.
    Ajoute un délai de transmission et peut simuler des pertes.
    """

    def __init__(self, latence_ms: float = 20, taux_perte: float = 0.0):
        self.noeuds:  dict[int, Noeud] = {}
        self.latence  = latence_ms / 1000
        self.taux_perte = taux_perte
        self._registre_ok: dict[int, threading.Event] = {}
        self._journal: list[tuple[float, str]] = []
        self._journal_lock = threading.Lock()
        self.t0 = time.time()

    def _log_global(self, entree: tuple[float, str]):
        with self._journal_lock:
            self._journal.append(entree)

    def ajouter(self, noeud: Noeud):
        self.noeuds[noeud.id] = noeud
        noeud._t0 = self.t0

    def envoyer(self, msg: Message):
        """Envoie un message avec délai simulé (et perte optionnelle)."""
        if random.random() < self.taux_perte:
            return  # Message perdu

        def livrer():
            time.sleep(self.latence + random.uniform(0, self.latence))
            destinataire = self.noeuds.get(msg.destinataire)
            if destinataire:
                destinataire.recevoir(msg)

        threading.Thread(target=livrer, daemon=True).start()

    def afficher_journal(self, n: int = 40):
        with self._journal_lock:
            entrees = sorted(self._journal, key=lambda e: e[0])[-n:]
        print(f"\n  {'t':>6}  Événement")
        print("  " + "─"*56)
        for t, msg in entrees:
            print(f"  +{t:5.2f}s  {msg}")

    def afficher_etat(self):
        print(f"\n  État du cluster :")
        print("  " + "─"*40)
        for nid in sorted(self.noeuds):
            print(f"  {self.noeuds[nid].statut()}")
