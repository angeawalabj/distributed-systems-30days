"""
Jour 10 — Gossip Protocol (Épidémique)
========================================
Problème du Jour 9 : le Read Repair ne guérit que les clés lues.
Les données "froides" restent périmées indéfiniment.
Problème plus large : comment 1000 nœuds synchronisent-ils leur état
sans coordinateur central ? Un broadcast naïf coûte O(N) messages
par nœud → O(N²) total. Intenable à grande échelle.

Solution : Gossip (protocole épidémique)
  Chaque nœud, à chaque round, choisit k voisins ALÉATOIRES
  et échange son état avec eux.

  Propriété remarquable :
    Après O(log N) rounds, TOUS les nœuds ont l'information.
    Coût total : O(N log N) messages — scalable à l'infini.

  Preuve intuitive (modèle SIR épidémique) :
    Round 1 : 1 nœud informé  → parle à k → k+1 nœuds informés
    Round 2 : k+1 nœuds        → chacun parle à k → croissance exponentielle
    Round r : ~(1+k)^r nœuds informés
    → Atteint N quand r ≈ log_{1+k}(N) = O(log N)

  Utilisé par : Cassandra (membership), Consul, DynamoDB (ring),
                Bitcoin (propagation), CockroachDB (node liveness)

Variantes :
  Push      : A envoie son état à B
  Pull      : A demande l'état à B
  Push-Pull : A envoie ET demande (convergence 2x plus rapide)
"""

import time
import threading
import random
import math
from dataclasses import dataclass, field
from typing import Any, Optional


# ─── ENTRÉE DE L'ÉTAT GOSSIP ─────────────────────────────────────────────────

@dataclass
class EntreeGossip:
    """
    Une paire clé/valeur dans l'état gossipé.
    Inclut un numéro de génération (heartbeat counter) pour
    détecter quelle version est la plus récente.
    """
    cle:        str
    valeur:     Any
    version:    int    # Incrémenté à chaque mise à jour
    noeud_src:  int    # Nœud qui a créé cette valeur
    ts:         float  # Timestamp de création

    def est_plus_recent_que(self, autre: "EntreeGossip") -> bool:
        return self.version > autre.version


# ─── NŒUD GOSSIP ─────────────────────────────────────────────────────────────

class NoeudGossip:
    """
    Nœud participant au protocole Gossip.

    Maintient :
      - Son état local (clés/valeurs avec versions)
      - La liste des membres du cluster (membership table)
      - Un compteur de rounds pour les statistiques

    À chaque round :
      1. Choisit k voisins au hasard
      2. Leur envoie son état (push)
      3. Reçoit leur état (pull)
      4. Merge : garde la version la plus récente pour chaque clé
    """

    def __init__(self, noeud_id: int, k: int = 3):
        self.id       = noeud_id
        self.k        = k           # Fanout : nombre de voisins par round
        self._actif   = False
        self._reseau: Optional["ReseauGossip"] = None

        # État local : clé → EntreeGossip
        self._etat: dict[str, EntreeGossip] = {}
        self._lock = threading.Lock()

        # Membership : nœuds connus avec leur dernier heartbeat
        self._membres: dict[int, float] = {noeud_id: time.time()}
        self._membres_lock = threading.Lock()

        # Statistiques
        self.stats = {
            "rounds":          0,
            "messages_envoyes": 0,
            "messages_recus":   0,
            "mises_a_jour":     0,   # Nb de clés mises à jour via gossip
        }

        # Génération locale (pour les mises à jour de cet nœud)
        self._generation: dict[str, int] = {}

    # ── Cycle de vie ──────────────────────────────────────────────────────────

    def demarrer(self, reseau: "ReseauGossip", intervalle: float = 0.2):
        self._reseau = reseau
        self._actif  = True
        threading.Thread(
            target=self._boucle_gossip,
            args=(intervalle,),
            daemon=True,
            name=f"gossip-{self.id}"
        ).start()

    def arreter(self):
        self._actif = False

    def _boucle_gossip(self, intervalle: float):
        while self._actif:
            time.sleep(intervalle)
            self._round_gossip()
            self._mettre_a_jour_heartbeat()

    # ── Opérations locales ────────────────────────────────────────────────────

    def ecrire_local(self, cle: str, valeur: Any):
        """
        Écrit une valeur localement.
        Elle sera propagée automatiquement par le gossip.
        """
        with self._lock:
            version_actuelle = self._generation.get(cle, 0) + 1
            self._generation[cle] = version_actuelle
            self._etat[cle] = EntreeGossip(
                cle=cle,
                valeur=valeur,
                version=version_actuelle,
                noeud_src=self.id,
                ts=time.time(),
            )

    def lire_local(self, cle: str) -> Optional[Any]:
        with self._lock:
            entree = self._etat.get(cle)
            return entree.valeur if entree else None

    def snapshot(self) -> dict[str, Any]:
        """Retourne une copie de l'état actuel."""
        with self._lock:
            return {k: e.valeur for k, e in self._etat.items()}

    def connait(self, cle: str) -> bool:
        with self._lock:
            return cle in self._etat

    # ── Round Gossip ──────────────────────────────────────────────────────────

    def _round_gossip(self):
        """
        Un round Push-Pull :
          1. Choisir k voisins aléatoires parmi les membres connus
          2. Leur envoyer notre état (push)
          3. Recevoir leur état en retour (pull)
          4. Merger
        """
        if not self._reseau:
            return

        voisins = self._choisir_voisins()
        if not voisins:
            return

        self.stats["rounds"] += 1

        with self._lock:
            mon_etat = dict(self._etat)

        for nid in voisins:
            # Push : on envoie notre état
            # Pull : on reçoit leur état en retour
            etat_distant = self._reseau.echanger(self.id, nid, mon_etat)
            if etat_distant is not None:
                self.stats["messages_envoyes"] += 1
                self.stats["messages_recus"]   += 1
                self._merger(etat_distant)

    def _choisir_voisins(self) -> list[int]:
        """
        Choix aléatoire de k voisins parmi les membres connus.
        C'est l'aléatoire qui donne la propriété O(log N).
        Un nœud mort depuis longtemps sera retiré des membres.
        """
        with self._membres_lock:
            candidats = [
                nid for nid in self._membres
                if nid != self.id and self._reseau and
                self._reseau.peut_communiquer(self.id, nid)
            ]
        return random.sample(candidats, min(self.k, len(candidats)))

    def _merger(self, etat_distant: dict[str, "EntreeGossip"]):
        """
        Merge de deux états : Last-Writer-Wins par version.
        On ne garde que les entrées plus récentes que ce qu'on a.
        """
        mises_a_jour = 0
        with self._lock:
            for cle, entree_dist in etat_distant.items():
                locale = self._etat.get(cle)
                if locale is None or entree_dist.est_plus_recent_que(locale):
                    self._etat[cle] = entree_dist
                    mises_a_jour += 1
        self.stats["mises_a_jour"] += mises_a_jour

    def _mettre_a_jour_heartbeat(self):
        """Gossipe son propre heartbeat pour signaler qu'on est vivant."""
        with self._membres_lock:
            self._membres[self.id] = time.time()
        # Ce heartbeat sera gossipé au prochain round via l'état
        self.ecrire_local(f"__hb__{self.id}", time.time())

    def recevoir_etat(self, etat_expediteur: dict[str, "EntreeGossip"]) -> dict[str, "EntreeGossip"]:
        """
        Appelé quand un voisin nous contacte.
        On merger son état et on lui retourne le nôtre (push-pull).
        """
        self.stats["messages_recus"] += 1
        self._merger(etat_expediteur)

        with self._membres_lock:
            # Le voisin est vivant puisqu'il nous a contactés
            pass

        with self._lock:
            return dict(self._etat)

    def nb_cles_connues(self, prefixe: str = "") -> int:
        with self._lock:
            if prefixe:
                return sum(1 for k in self._etat if k.startswith(prefixe))
            return len(self._etat)


# ─── RÉSEAU GOSSIP ────────────────────────────────────────────────────────────

class ReseauGossip:
    """
    Simule le réseau entre les nœuds.
    Gère la latence, les partitions et les statistiques globales.
    """

    def __init__(self, latence_ms: float = 5.0, taux_perte: float = 0.0):
        self.noeuds:   dict[int, NoeudGossip] = {}
        self.latence   = latence_ms / 1000
        self.taux_perte = taux_perte
        self._partitions: list[set[int]] = []
        self._part_lock  = threading.Lock()
        self.t0          = time.time()

        # Historique de propagation : {cle: {noeud_id: temps_de_découverte}}
        self.propagation: dict[str, dict[int, float]] = {}
        self._prop_lock  = threading.Lock()

    def ajouter(self, noeud: NoeudGossip):
        self.noeuds[noeud.id] = noeud
        # Informer tous les membres existants de ce nouveau nœud
        with noeud._membres_lock:
            for nid in self.noeuds:
                noeud._membres[nid] = time.time()
        for n in self.noeuds.values():
            with n._membres_lock:
                n._membres[noeud.id] = time.time()

    def peut_communiquer(self, a: int, b: int) -> bool:
        with self._part_lock:
            if not self._partitions:
                return True
            for groupe in self._partitions:
                if a in groupe and b in groupe:
                    return True
            return False

    def partitionner(self, *groupes: set[int]):
        with self._part_lock:
            self._partitions = list(groupes)

    def guerir(self):
        with self._part_lock:
            self._partitions = []

    def echanger(
        self,
        expediteur_id: int,
        destinataire_id: int,
        etat: dict[str, "EntreeGossip"],
    ) -> Optional[dict[str, "EntreeGossip"]]:
        """Échange d'état push-pull avec latence simulée."""
        if random.random() < self.taux_perte:
            return None
        time.sleep(self.latence * random.uniform(0.5, 1.5))
        dest = self.noeuds.get(destinataire_id)
        if not dest or not dest._actif:
            return None
        return dest.recevoir_etat(etat)

    def demarrer_tous(self, intervalle: float = 0.15):
        self.t0 = time.time()
        for n in self.noeuds.values():
            n.demarrer(self, intervalle)

    def arreter_tous(self):
        for n in self.noeuds.values():
            n.arreter()

    def suivre_propagation(self, cle: str):
        """Démarre le suivi de propagation d'une clé."""
        with self._prop_lock:
            self.propagation[cle] = {}

    def enregistrer_propagation(self, cle: str, noeud_id: int):
        """Enregistre quand un nœud découvre une clé."""
        with self._prop_lock:
            if cle in self.propagation and noeud_id not in self.propagation[cle]:
                self.propagation[cle][noeud_id] = time.time() - self.t0

    def taux_propagation(self, cle: str) -> float:
        """Pourcentage de nœuds ayant la clé."""
        with self._prop_lock:
            if cle not in self.propagation:
                return 0.0
            return len(self.propagation[cle]) / len(self.noeuds) * 100

    def attendre_propagation(self, cle: str, seuil: float = 1.0, timeout: float = 10.0) -> float:
        """
        Attend que `seuil` fraction des nœuds aient la clé.
        Retourne le temps écoulé.
        Polling sur les états réels des nœuds.
        """
        debut = time.time()
        while time.time() - debut < timeout:
            nb = sum(1 for n in self.noeuds.values() if n.connait(cle))
            if nb >= len(self.noeuds) * seuil:
                return time.time() - debut
            time.sleep(0.05)
        return time.time() - debut

    def stats_globales(self) -> dict:
        total_msgs = sum(n.stats["messages_envoyes"] for n in self.noeuds.values())
        total_rounds = sum(n.stats["rounds"] for n in self.noeuds.values())
        return {"messages_total": total_msgs, "rounds_total": total_rounds}
