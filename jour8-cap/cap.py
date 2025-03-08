"""
Jour 8 — Théorème CAP (Brewer, 2000)
======================================
Un système distribué ne peut garantir simultanément que 2 des 3 :

  C — Consistency   : tous les nœuds voient la même donnée au même instant
  A — Availability  : chaque requête reçoit une réponse (pas d'erreur)
  P — Partition tol.: le système fonctionne malgré une coupure réseau

  La partition réseau (P) est INÉVITABLE en production.
  → Le vrai choix est : CP ou AP ?

      CP (Cohérence + Partition)
        Refuse les écritures si pas de quorum.
        Retourne une erreur plutôt que des données incohérentes.
        Exemples : HBase, Zookeeper, etcd, CockroachDB

      AP (Disponibilité + Partition)
        Accepte toujours les requêtes, même si les nœuds divergent.
        Répond avec des données potentiellement obsolètes.
        Exemples : Cassandra, DynamoDB, CouchDB, Riak

  Nuance importante (Théorème PACELC, 2012) :
    En l'absence de partition, le trade-off est :
      Latence (L) vs Cohérence (C)
    → Même sans partition, il y a des compromis.

On implémente les deux pour voir la différence concrète.
"""

import time
import threading
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Any


# ─── RÉSULTAT D'UNE OPÉRATION ────────────────────────────────────────────────

class StatutOp(Enum):
    OK        = "OK"
    ERREUR    = "ERREUR"       # CP : refuse sous partition
    OBSOLETE  = "OBSOLETE"     # AP : répond avec données périmées


@dataclass
class ResultatOp:
    statut:   StatutOp
    valeur:   Any              = None
    message:  str              = ""
    noeud_id: int              = 0
    latence_ms: float          = 0.0
    version:  int              = 0    # Numéro de version de la donnée


# ─── PARTITION RÉSEAU SIMULÉE ────────────────────────────────────────────────

class PartitionReseau:
    """
    Simule une coupure réseau entre groupes de nœuds.
    Les nœuds dans des partitions différentes ne se voient plus.
    """

    def __init__(self):
        # Liste de sets de nœuds qui se voient entre eux
        # Ex: [{1,2}, {3,4,5}] → 1 et 2 se voient, 3/4/5 se voient,
        #     mais le groupe {1,2} ne voit pas {3,4,5}
        self._partitions: list[set[int]] = []
        self._actif = False
        self._lock = threading.Lock()

    def partitionner(self, *groupes: set[int]):
        """Crée une partition réseau."""
        with self._lock:
            self._partitions = list(groupes)
            self._actif = True

    def guerir(self):
        """Répare la partition."""
        with self._lock:
            self._partitions = []
            self._actif = False

    def peut_communiquer(self, noeud_a: int, noeud_b: int) -> bool:
        """Vérifie si deux nœuds peuvent se parler."""
        with self._lock:
            if not self._actif:
                return True
            for groupe in self._partitions:
                if noeud_a in groupe and noeud_b in groupe:
                    return True
            return False


# ════════════════════════════════════════════════════════════════
# BASE DE DONNÉES CP
# Cohérence forte — refuse les écritures sans quorum
# ════════════════════════════════════════════════════════════════

class NoeudCP:
    """
    Nœud d'une base de données CP (type etcd / CockroachDB).

    Écriture : nécessite quorum → refuse si partition isole le nœud
    Lecture  : lit toujours depuis le leader (cohérence linéarisable)

    Sous partition :
      - Si le nœud fait partie de la majorité → accepte
      - Si le nœud est isolé (minorité) → retourne ERREUR
    """

    def __init__(self, noeud_id: int, tous_ids: list[int], partition: PartitionReseau):
        self.id        = noeud_id
        self.tous_ids  = tous_ids
        self.partition = partition
        self._store: dict[str, tuple[Any, int]] = {}  # key → (valeur, version)
        self._lock = threading.Lock()
        self.ops_journal: list[str] = []

    def _quorum(self) -> int:
        return len(self.tous_ids) // 2 + 1

    def _noeuds_visibles(self) -> list[int]:
        """Nœuds avec lesquels ce nœud peut communiquer."""
        return [nid for nid in self.tous_ids
                if self.partition.peut_communiquer(self.id, nid)]

    def _a_quorum(self) -> bool:
        return len(self._noeuds_visibles()) >= self._quorum()

    def ecrire(self, cle: str, valeur: Any) -> ResultatOp:
        debut = time.perf_counter()

        if not self._a_quorum():
            # CP : REFUSE — cohérence > disponibilité
            visibles = self._noeuds_visibles()
            msg = (f"Partition détectée : {len(visibles)}/{len(self.tous_ids)} nœuds "
                   f"visibles, quorum={self._quorum()} requis. ÉCRITURE REFUSÉE.")
            self.ops_journal.append(f"WRITE {cle}={valeur} → ERREUR ({msg})")
            return ResultatOp(
                statut=StatutOp.ERREUR,
                message=msg,
                noeud_id=self.id,
                latence_ms=(time.perf_counter() - debut) * 1000,
            )

        # A le quorum → écrire et répliquer
        with self._lock:
            version_actuelle = self._store.get(cle, (None, 0))[1]
            nouvelle_version = version_actuelle + 1
            self._store[cle] = (valeur, nouvelle_version)

        # Simuler la réplication (synchrone en CP)
        time.sleep(0.01)

        self.ops_journal.append(f"WRITE {cle}={valeur} → OK (v{nouvelle_version})")
        return ResultatOp(
            statut=StatutOp.OK,
            valeur=valeur,
            noeud_id=self.id,
            latence_ms=(time.perf_counter() - debut) * 1000,
            version=nouvelle_version,
        )

    def lire(self, cle: str) -> ResultatOp:
        debut = time.perf_counter()

        # CP : lecture forte → nécessite aussi le quorum
        if not self._a_quorum():
            msg = f"Partition : lecture refusée (quorum={self._quorum()} non atteint)"
            self.ops_journal.append(f"READ {cle} → ERREUR")
            return ResultatOp(
                statut=StatutOp.ERREUR,
                message=msg,
                noeud_id=self.id,
                latence_ms=(time.perf_counter() - debut) * 1000,
            )

        with self._lock:
            entree = self._store.get(cle)

        valeur, version = entree if entree else (None, 0)
        self.ops_journal.append(f"READ {cle} → {valeur} (v{version})")
        return ResultatOp(
            statut=StatutOp.OK,
            valeur=valeur,
            noeud_id=self.id,
            latence_ms=(time.perf_counter() - debut) * 1000,
            version=version,
        )

    def etat(self) -> dict:
        with self._lock:
            return {k: v for k, (v, _) in self._store.items()}


# ════════════════════════════════════════════════════════════════
# BASE DE DONNÉES AP
# Disponibilité totale — accepte toujours, éventuellement cohérente
# ════════════════════════════════════════════════════════════════

class NoeudAP:
    """
    Nœud d'une base de données AP (type Cassandra / DynamoDB).

    Écriture : acceptée localement TOUJOURS, répliquée en async
    Lecture  : répond depuis le local TOUJOURS, même si obsolète

    Sous partition :
      - Chaque nœud continue à servir les requêtes
      - Les nœuds isolés divergent (données différentes)
      - À la guérison : résolution de conflits nécessaire
        (ici : Last-Write-Wins par timestamp)
    """

    def __init__(self, noeud_id: int, tous_ids: list[int], partition: PartitionReseau):
        self.id        = noeud_id
        self.tous_ids  = tous_ids
        self.partition = partition
        # key → (valeur, timestamp, version_vecteur_simplifié)
        self._store: dict[str, tuple[Any, float, int]] = {}
        self._lock = threading.Lock()
        self.ops_journal: list[str] = []
        self._pairs: dict[int, "NoeudAP"] = {}  # Référence aux autres nœuds

    def enregistrer_pairs(self, pairs: dict[int, "NoeudAP"]):
        self._pairs = {k: v for k, v in pairs.items() if k != self.id}

    def ecrire(self, cle: str, valeur: Any) -> ResultatOp:
        """AP : écrit TOUJOURS localement, réplique en arrière-plan."""
        debut = time.perf_counter()
        ts = time.time()

        with self._lock:
            version_actuelle = self._store.get(cle, (None, 0, 0))[2]
            nouvelle_version = version_actuelle + 1
            self._store[cle] = (valeur, ts, nouvelle_version)

        # Réplication asynchrone vers les nœuds visibles
        threading.Thread(
            target=self._repliquer_async,
            args=(cle, valeur, ts, nouvelle_version),
            daemon=True
        ).start()

        self.ops_journal.append(f"WRITE {cle}={valeur} → OK (v{nouvelle_version}, async)")
        return ResultatOp(
            statut=StatutOp.OK,
            valeur=valeur,
            noeud_id=self.id,
            latence_ms=(time.perf_counter() - debut) * 1000,
            version=nouvelle_version,
        )

    def _repliquer_async(self, cle: str, valeur: Any, ts: float, version: int):
        """Réplication asynchrone — peut échouer sous partition."""
        for nid, pair in self._pairs.items():
            if self.partition.peut_communiquer(self.id, nid):
                pair._recevoir_replication(cle, valeur, ts, version)

    def _recevoir_replication(self, cle: str, valeur: Any, ts: float, version: int):
        """
        Reçoit une réplication d'un pair.
        Last-Write-Wins (LWW) : le timestamp le plus récent gagne.
        C'est simple mais peut perdre des données simultanées !
        (Vector Clocks au Jour 14 règlent ça proprement)
        """
        with self._lock:
            entree_actuelle = self._store.get(cle)
            if entree_actuelle is None or ts > entree_actuelle[1]:
                self._store[cle] = (valeur, ts, version)

    def lire(self, cle: str) -> ResultatOp:
        """AP : répond TOUJOURS depuis le local, même si obsolète."""
        debut = time.perf_counter()

        # Vérifier si on est isolé (pour signaler l'obsolescence)
        visibles = [nid for nid in self.tous_ids
                    if self.partition.peut_communiquer(self.id, nid)]
        est_isole = len(visibles) < len(self.tous_ids) // 2 + 1

        with self._lock:
            entree = self._store.get(cle)

        valeur, _, version = entree if entree else (None, 0, 0)

        statut = StatutOp.OBSOLETE if est_isole else StatutOp.OK
        msg    = "⚠️ Données potentiellement obsolètes (nœud isolé)" if est_isole else ""

        self.ops_journal.append(
            f"READ {cle} → {valeur} (v{version})"
            + (" [OBSOLETE?]" if est_isole else "")
        )
        return ResultatOp(
            statut=statut,
            valeur=valeur,
            message=msg,
            noeud_id=self.id,
            latence_ms=(time.perf_counter() - debut) * 1000,
            version=version,
        )

    def reconcilier(self, pairs: list["NoeudAP"]):
        """
        Après guérison de la partition : synchronisation Last-Write-Wins.
        En prod : c'est le "anti-entropy" / "read repair" de Cassandra.
        """
        for pair in pairs:
            if pair.id == self.id:
                continue
            with pair._lock:
                for cle, (val, ts, ver) in pair._store.items():
                    with self._lock:
                        local = self._store.get(cle)
                        if local is None or ts > local[1]:
                            self._store[cle] = (val, ts, ver)

    def etat(self) -> dict:
        with self._lock:
            return {k: v for k, (v, _, _) in self._store.items()}


# ─── CLUSTER HELPERS ─────────────────────────────────────────────────────────

def creer_cluster_cp(n: int, partition: PartitionReseau) -> list[NoeudCP]:
    ids = list(range(1, n + 1))
    return [NoeudCP(i, ids, partition) for i in ids]

def creer_cluster_ap(n: int, partition: PartitionReseau) -> list[NoeudAP]:
    ids = list(range(1, n + 1))
    noeuds = [NoeudAP(i, ids, partition) for i in ids]
    pairs = {n.id: n for n in noeuds}
    for n in noeuds:
        n.enregistrer_pairs(pairs)
    return noeuds
