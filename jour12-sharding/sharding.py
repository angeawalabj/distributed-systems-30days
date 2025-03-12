"""
Jour 12 — Sharding (Partitionnement)
======================================
Problème : une seule base de données atteint ses limites.
  → 10 milliards de lignes : les index ne tiennent plus en RAM
  → 100 000 écritures/seconde : le disque sature
  → 10 To de données : impossible sur un seul serveur

Solution : diviser les données en SHARDS (fragments) répartis
sur plusieurs serveurs. Chaque shard contient un sous-ensemble
des données. Les requêtes sont routées vers le bon shard.

Trois stratégies de sharding :

  1. Hash Sharding (le plus courant)
     shard = hash(clé) % N
     ✅ Distribution uniforme
     ❌ Requêtes par plage impossibles (range queries)
     Exemple : MongoDB, Cassandra par défaut

  2. Range Sharding
     Shard 1 : A–M, Shard 2 : N–Z
     ✅ Range queries efficaces (SELECT WHERE id BETWEEN 100 AND 200)
     ❌ Risque de hotspot (un shard reçoit tout le trafic)
     Exemple : HBase, Bigtable, CockroachDB

  3. Directory Sharding
     Une table de lookup indique quel shard contient quelle clé
     ✅ Flexibilité maximale (migration sans rehashing)
     ❌ Le directory devient un SPOF, latence supplémentaire
     Exemple : Pinterest, certaines configs de MongoDB

Problème fondamental du hash sharding : RESHARDING
  Si N passe de 3 à 4 → hash(clé) % 3 ≠ hash(clé) % 4
  → Presque toutes les clés changent de shard
  → Solution : Consistent Hashing (Jour 11) !
"""

import hashlib
import bisect
import time
import threading
import random
from dataclasses import dataclass, field
from typing import Any, Optional


# ─── ENREGISTREMENT DE BASE ───────────────────────────────────────────────────

@dataclass
class Enregistrement:
    cle:     str
    valeur:  Any
    ts:      float = field(default_factory=time.time)
    shard_id: int  = -1


# ─── SHARD PHYSIQUE ───────────────────────────────────────────────────────────

class Shard:
    """
    Simule un serveur de base de données (un shard).
    Maintient un store local, compte les opérations.
    """

    def __init__(self, shard_id: int, latence_ms: float = 5.0):
        self.id       = shard_id
        self.latence  = latence_ms / 1000
        self._store: dict[str, Enregistrement] = {}
        self._lock    = threading.RLock()
        self.stats    = {"lectures": 0, "ecritures": 0, "suppressions": 0}

    def ecrire(self, cle: str, valeur: Any) -> Enregistrement:
        time.sleep(self.latence * random.uniform(0.5, 1.5))
        rec = Enregistrement(cle=cle, valeur=valeur, shard_id=self.id)
        with self._lock:
            self._store[cle] = rec
            self.stats["ecritures"] += 1
        return rec

    def lire(self, cle: str) -> Optional[Enregistrement]:
        time.sleep(self.latence * random.uniform(0.3, 0.8))
        with self._lock:
            self.stats["lectures"] += 1
            return self._store.get(cle)

    def supprimer(self, cle: str) -> bool:
        with self._lock:
            self.stats["suppressions"] += 1
            return self._store.pop(cle, None) is not None

    def scanner(self, predicate=None) -> list[Enregistrement]:
        """Scan complet du shard (utilisé pour les range queries)."""
        with self._lock:
            recs = list(self._store.values())
        return [r for r in recs if predicate is None or predicate(r)]

    def nb_enregistrements(self) -> int:
        with self._lock:
            return len(self._store)

    def taille_estimee_mb(self) -> float:
        """Estimation grossière : 200 octets par enregistrement."""
        return self.nb_enregistrements() * 200 / 1_000_000


# ══════════════════════════════════════════════════════════════════
# STRATÉGIE 1 : HASH SHARDING
# ══════════════════════════════════════════════════════════════════

class HashSharding:
    """
    Distribue les clés par hash modulo N.
    Simple, uniforme, mais range queries impossibles.
    """

    def __init__(self, shards: list[Shard]):
        self.shards = shards
        self.N      = len(shards)

    def _shard_pour(self, cle: str) -> Shard:
        h = int(hashlib.md5(cle.encode()).hexdigest(), 16)
        return self.shards[h % self.N]

    def ecrire(self, cle: str, valeur: Any) -> tuple[int, Enregistrement]:
        shard = self._shard_pour(cle)
        return shard.id, shard.ecrire(cle, valeur)

    def lire(self, cle: str) -> Optional[Enregistrement]:
        return self._shard_pour(cle).lire(cle)

    def supprimer(self, cle: str) -> bool:
        return self._shard_pour(cle).supprimer(cle)

    def distribution(self) -> dict[int, int]:
        return {s.id: s.nb_enregistrements() for s in self.shards}

    def cles_reaffectees_si_ajout(self, toutes_cles: list[str]) -> tuple[int, float]:
        """Calcule combien de clés changeraient si on ajoutait 1 shard."""
        N_nouveau = self.N + 1
        changes = 0
        for cle in toutes_cles:
            h = int(hashlib.md5(cle.encode()).hexdigest(), 16)
            ancien = h % self.N
            nouveau = h % N_nouveau
            if ancien != nouveau:
                changes += 1
        return changes, changes / len(toutes_cles) * 100


# ══════════════════════════════════════════════════════════════════
# STRATÉGIE 2 : RANGE SHARDING
# ══════════════════════════════════════════════════════════════════

@dataclass
class Plage:
    min_val: str    # Borne inférieure (incluse)
    max_val: str    # Borne supérieure (exclue, "" = infini)
    shard_id: int


class RangeSharding:
    """
    Distribue les clés par plage alphabétique/numérique.
    Range queries très efficaces : aller directement au bon shard.
    Risque de hotspot si les écritures se concentrent sur une plage.
    """

    def __init__(self, shards: list[Shard], plages: list[Plage]):
        assert len(plages) == len(shards)
        self.shards = {s.id: s for s in shards}
        self.plages = sorted(plages, key=lambda p: p.min_val)

    def _shard_pour(self, cle: str) -> Shard:
        for plage in self.plages:
            if cle >= plage.min_val and (plage.max_val == "" or cle < plage.max_val):
                return self.shards[plage.shard_id]
        # Fallback : dernier shard
        return self.shards[self.plages[-1].shard_id]

    def ecrire(self, cle: str, valeur: Any) -> tuple[int, Enregistrement]:
        shard = self._shard_pour(cle)
        return shard.id, shard.ecrire(cle, valeur)

    def lire(self, cle: str) -> Optional[Enregistrement]:
        return self._shard_pour(cle).lire(cle)

    def range_query(self, min_cle: str, max_cle: str) -> list[Enregistrement]:
        """
        Range query efficace : on interroge seulement les shards couvrant la plage.
        Avec hash sharding, on devrait scanner TOUS les shards.
        """
        shards_concernes = set()
        for plage in self.plages:
            # Ce shard est concerné si ses bornes chevauchent [min_cle, max_cle]
            if plage.min_val <= max_cle and (plage.max_val == "" or plage.max_val > min_cle):
                shards_concernes.add(plage.shard_id)

        resultats = []
        for sid in shards_concernes:
            shard = self.shards[sid]
            resultats.extend(
                shard.scanner(lambda r: min_cle <= r.cle <= max_cle)
            )
        return sorted(resultats, key=lambda r: r.cle)

    def distribution(self) -> dict[int, int]:
        return {sid: s.nb_enregistrements() for sid, s in self.shards.items()}


# ══════════════════════════════════════════════════════════════════
# STRATÉGIE 3 : DIRECTORY SHARDING
# ══════════════════════════════════════════════════════════════════

class DirectorySharding:
    """
    Un registre central (directory) sait quel shard contient quelle clé.
    Maximum de flexibilité : on peut migrer des clés individuellement.
    Le directory est le SPOF : doit être répliqué en production.
    """

    def __init__(self, shards: list[Shard]):
        self.shards    = {s.id: s for s in shards}
        self._directory: dict[str, int] = {}   # clé → shard_id
        self._lock     = threading.RLock()
        self.stats     = {"hits": 0, "misses": 0}

    def _assigner_shard(self, cle: str) -> int:
        """Round-robin par défaut pour les nouvelles clés."""
        return len(self._directory) % len(self.shards)

    def ecrire(self, cle: str, valeur: Any) -> tuple[int, Enregistrement]:
        with self._lock:
            if cle not in self._directory:
                self._directory[cle] = self._assigner_shard(cle)
            shard_id = self._directory[cle]
        return shard_id, self.shards[shard_id].ecrire(cle, valeur)

    def lire(self, cle: str) -> Optional[Enregistrement]:
        with self._lock:
            shard_id = self._directory.get(cle)
        if shard_id is None:
            self.stats["misses"] += 1
            return None
        self.stats["hits"] += 1
        return self.shards[shard_id].lire(cle)

    def migrer(self, cle: str, nouveau_shard_id: int) -> bool:
        """
        Migration d'une clé vers un autre shard.
        Opération chirurgicale impossible avec hash sharding.
        """
        with self._lock:
            ancien_sid = self._directory.get(cle)
            if ancien_sid is None or ancien_sid == nouveau_shard_id:
                return False
            rec = self.shards[ancien_sid].lire(cle)
            if rec is None:
                return False
            self.shards[nouveau_shard_id].ecrire(cle, rec.valeur)
            self.shards[ancien_sid].supprimer(cle)
            self._directory[cle] = nouveau_shard_id
        return True

    def distribution(self) -> dict[int, int]:
        return {sid: s.nb_enregistrements() for sid, s in self.shards.items()}


# ──────────────────────────────────────────────────────────────────
# CONSISTENT HASH SHARDING (Rappel Jour 11 appliqué au sharding)
# ──────────────────────────────────────────────────────────────────

class ConsistentHashSharding:
    """
    Hash sharding avec l'anneau du Jour 11.
    Résout le problème du resharding : seulement K/N clés bougent.
    """

    def __init__(self, shards: list[Shard], vnodes: int = 150):
        self.shards   = {s.id: s for s in shards}
        self._vnodes  = vnodes
        self._anneau: dict[int, int] = {}    # position → shard_id
        self._positions: list[int] = []
        for s in shards:
            self._ajouter_au_cercle(s.id)

    def _hash(self, valeur: str) -> int:
        return int(hashlib.md5(valeur.encode()).hexdigest(), 16) % (2**32)

    def _ajouter_au_cercle(self, shard_id: int):
        for i in range(self._vnodes):
            pos = self._hash(f"shard-{shard_id}#vnode{i}")
            self._anneau[pos] = shard_id
            bisect.insort(self._positions, pos)

    def ajouter_shard(self, shard: Shard):
        self.shards[shard.id] = shard
        self._ajouter_au_cercle(shard.id)

    def _shard_pour(self, cle: str) -> Shard:
        pos = self._hash(cle)
        idx = bisect.bisect_left(self._positions, pos) % len(self._positions)
        return self.shards[self._anneau[self._positions[idx]]]

    def ecrire(self, cle: str, valeur: Any) -> tuple[int, Enregistrement]:
        shard = self._shard_pour(cle)
        return shard.id, shard.ecrire(cle, valeur)

    def lire(self, cle: str) -> Optional[Enregistrement]:
        return self._shard_pour(cle).lire(cle)

    def distribution(self) -> dict[int, int]:
        return {sid: s.nb_enregistrements() for sid, s in self.shards.items()}

    def cles_reaffectees_si_ajout(self, toutes_cles: list[str], nouveau_id: int) -> tuple[int, float]:
        avant  = {cle: self._shard_pour(cle).id for cle in toutes_cles}
        shard_tmp = Shard(nouveau_id)
        self.ajouter_shard(shard_tmp)
        apres  = {cle: self._shard_pour(cle).id for cle in toutes_cles}
        # Retirer le shard temporaire
        positions_a_suppr = [
            pos for pos, sid in self._anneau.items() if sid == nouveau_id
        ]
        for pos in positions_a_suppr:
            del self._anneau[pos]
            self._positions.remove(pos)
        del self.shards[nouveau_id]
        changes = sum(1 for c in toutes_cles if avant[c] != apres[c])
        return changes, changes / len(toutes_cles) * 100
