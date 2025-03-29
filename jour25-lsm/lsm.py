"""
Jour 25 — LSM Tree & SSTables (RocksDB / Cassandra / LevelDB)
==============================================================
Problème : comment stocker des centaines de millions de clés sur disque
avec des écritures rapides et des lectures acceptables ?

  B-Tree (PostgreSQL, MySQL) :
    ✅ Lectures rapides O(log N) — accès direct à la page
    ❌ Écritures lentes — mise à jour in-place = random I/O
    ❌ Write amplification élevée (mise à jour feuilles + pages internes)

  LSM Tree (Log-Structured Merge Tree) :
    ✅ Écritures ultra-rapides — toujours séquentiel (append)
    ✅ Haute throughput d'écriture (10× B-Tree en écriture pure)
    ❌ Lectures plus complexes — chercher dans plusieurs niveaux
    ❌ Compaction nécessaire périodiquement (CPU + I/O en background)

Architecture LSM Tree :

  Niveau 0 : MemTable (en mémoire, trié par clef, RedBlack tree)
    → Écriture toujours en mémoire : O(log N), pas de disque

  Niveau 1+ : SSTables (Sorted String Tables, sur disque, immutables)
    → Quand la MemTable est pleine : flush sur disque → SSTable L0
    → Compaction : fusionner SSTables adjacentes → SSTables plus grandes

  Pour lire une clef :
    1. Chercher dans MemTable
    2. Chercher dans L0 (plusieurs SSTables possibles)
    3. Chercher dans L1, L2... (un seul SSTable par niveau en théorie)
    → Bloom filter avant chaque niveau : "cette clef est-elle probablement ici ?"

Bloom Filter :
  Structure probabiliste : teste l'appartenance d'un élément à un ensemble.
  Faux positifs possibles (taux configurable).
  Faux négatifs impossibles.
  Si "absent" → certainement absent → évite la lecture du SSTable.
  O(1) en espace et en temps.

Write Amplification :
  Chaque écriture est écrite plusieurs fois lors de la compaction.
  Level 0 → Level 1 → Level 2 → ...
  Chaque niveau est 10× plus grand que le précédent.

Utilisé par : RocksDB (Meta, LinkedIn), Cassandra (Apache),
              LevelDB (Google), ScyllaDB, TiKV (PingCAP).
"""

from __future__ import annotations
import os
import json
import time
import math
import hashlib
import threading
from dataclasses import dataclass, field
from typing import Optional, Iterator, Any
from collections import defaultdict


# ─── BLOOM FILTER ─────────────────────────────────────────────────────────────

class BloomFilter:
    """
    Filtre de Bloom : test d'appartenance probabiliste.
    - Faux positifs possibles  (taux configurable)
    - Faux négatifs impossibles (si dit "absent" → certainement absent)
    - O(1) en espace (m bits) et O(k) en temps (k fonctions de hash)

    Formule pour taux de faux positifs cible fp_rate :
      m = -n * ln(fp_rate) / ln(2)²    (nb de bits)
      k = m/n * ln(2)                   (nb de hash functions)
    """

    def __init__(self, nb_elements: int, fp_rate: float = 0.01):
        self.n       = nb_elements
        self.fp_rate = fp_rate
        # Taille optimale du tableau de bits
        self.m = max(1, int(-nb_elements * math.log(fp_rate) / (math.log(2) ** 2)))
        # Nombre optimal de fonctions de hash
        self.k = max(1, int((self.m / nb_elements) * math.log(2)))
        self._bits = bytearray(self.m // 8 + 1)
        self.nb_inseres = 0

    def ajouter(self, cle: str):
        for pos in self._positions(cle):
            self._bits[pos // 8] |= (1 << (pos % 8))
        self.nb_inseres += 1

    def contient(self, cle: str) -> bool:
        """Retourne True si probablement présent, False si certainement absent."""
        return all(self._bits[p // 8] & (1 << (p % 8)) for p in self._positions(cle))

    def _positions(self, cle: str) -> list[int]:
        positions = []
        cle_bytes = cle.encode()
        for i in range(self.k):
            h = int(hashlib.md5(cle_bytes + i.to_bytes(2, "big")).hexdigest(), 16)
            positions.append(h % self.m)
        return positions

    def taux_fp_reel(self) -> float:
        """Taux de faux positifs estimé basé sur le remplissage."""
        if self.n == 0:
            return 0.0
        return (1 - math.exp(-self.k * self.nb_inseres / self.m)) ** self.k

    def taille_bits(self) -> int:
        return self.m


# ─── ENTRÉE ──────────────────────────────────────────────────────────────────

@dataclass(order=True)
class Entree:
    cle:       str
    valeur:    Optional[Any]   # None = tombstone (suppression)
    sequence:  int             # Monotone, pour résoudre les conflits
    ts:        float = field(default_factory=time.time, compare=False)

    @property
    def est_tombstone(self) -> bool:
        return self.valeur is None

    def serialiser(self) -> dict:
        return {"cle": self.cle, "valeur": self.valeur, "sequence": self.sequence}

    @staticmethod
    def deserialiser(d: dict) -> "Entree":
        return Entree(cle=d["cle"], valeur=d["valeur"], sequence=d["sequence"])


# ─── MEMTABLE ─────────────────────────────────────────────────────────────────

class MemTable:
    """
    Table en mémoire triée par clef.
    Implémentée avec un dict ordonné (Python 3.7+).
    En production : Red-Black tree ou Skip list pour O(log N) inserts.
    Capacité maximum : taille_max_bytes.
    """

    def __init__(self, taille_max_octets: int = 4 * 1024 * 1024):
        self._donnees: dict[str, Entree] = {}
        self.taille_max  = taille_max_octets
        self._taille_est = 0
        self._sequence   = 0
        self._lock       = threading.Lock()

    def ecrire(self, cle: str, valeur: Any) -> int:
        with self._lock:
            self._sequence += 1
            entree = Entree(cle=cle, valeur=valeur, sequence=self._sequence)
            self._donnees[cle] = entree
            self._taille_est += len(cle) + 64   # estimation
            return self._sequence

    def supprimer(self, cle: str) -> int:
        """Écrire un tombstone (marqueur de suppression)."""
        return self.ecrire(cle, None)

    def lire(self, cle: str) -> Optional[Entree]:
        with self._lock:
            return self._donnees.get(cle)

    def est_pleine(self) -> bool:
        return self._taille_est >= self.taille_max

    def taille(self) -> int:
        return len(self._donnees)

    def taille_octets(self) -> int:
        return self._taille_est

    def iter_trie(self) -> list[Entree]:
        """Retourne les entrées triées par clef (pour flush en SSTable)."""
        with self._lock:
            return sorted(self._donnees.values(), key=lambda e: e.cle)


# ─── SSTABLE ──────────────────────────────────────────────────────────────────

class SSTable:
    """
    Sorted String Table : fichier immutable trié par clef.
    Contient :
      - Les données triées (clef → valeur)
      - Un index de blocs pour la recherche rapide
      - Un Bloom filter pour les lookups rapides
    """

    def __init__(self, chemin: str, niveau: int = 0):
        self.chemin       = chemin
        self.niveau       = niveau
        self.bloom        = None
        self._index: dict[str, int] = {}   # cle_debut_bloc → offset
        self._donnees: list[Entree] = []
        self._min_cle: Optional[str] = None
        self._max_cle: Optional[str] = None
        self.sequence_max = 0
        self.nb_entrees   = 0
        self.nb_tombstones = 0

    # ── Écriture ─────────────────────────────────────────────────────────────

    @staticmethod
    def depuis_memtable(entrees: list[Entree], chemin: str,
                        niveau: int = 0) -> "SSTable":
        sst = SSTable(chemin, niveau)
        if not entrees:
            return sst

        sst._donnees    = entrees
        sst._min_cle    = entrees[0].cle
        sst._max_cle    = entrees[-1].cle
        sst.sequence_max = max(e.sequence for e in entrees)
        sst.nb_entrees  = len(entrees)
        sst.nb_tombstones = sum(1 for e in entrees if e.est_tombstone)

        # Bloom filter
        sst.bloom = BloomFilter(nb_elements=len(entrees), fp_rate=0.01)
        for e in entrees:
            sst.bloom.ajouter(e.cle)

        # Sauvegarder sur disque
        sst._sauvegarder()
        return sst

    def _sauvegarder(self):
        os.makedirs(os.path.dirname(self.chemin), exist_ok=True)
        data = {
            "niveau":       self.niveau,
            "sequence_max": self.sequence_max,
            "entrees":      [e.serialiser() for e in self._donnees],
        }
        with open(self.chemin, "w") as f:
            json.dump(data, f)

    # ── Lecture ───────────────────────────────────────────────────────────────

    @staticmethod
    def charger(chemin: str) -> "SSTable":
        sst = SSTable(chemin)
        with open(chemin) as f:
            data = json.load(f)
        sst.niveau       = data["niveau"]
        sst.sequence_max = data["sequence_max"]
        sst._donnees     = [Entree.deserialiser(d) for d in data["entrees"]]

        if sst._donnees:
            sst._min_cle     = sst._donnees[0].cle
            sst._max_cle     = sst._donnees[-1].cle
            sst.nb_entrees   = len(sst._donnees)
            sst.nb_tombstones = sum(1 for e in sst._donnees if e.est_tombstone)

        # Reconstruire le bloom filter
        sst.bloom = BloomFilter(nb_elements=max(1, len(sst._donnees)), fp_rate=0.01)
        for e in sst._donnees:
            sst.bloom.ajouter(e.cle)
        return sst

    def peut_contenir(self, cle: str) -> bool:
        """Vérification rapide de la plage de clefs."""
        if self._min_cle is None:
            return False
        return self._min_cle <= cle <= self._max_cle

    def chercher(self, cle: str) -> Optional[Entree]:
        """Recherche binaire dans le SSTable trié."""
        if not self.peut_contenir(cle):
            return None
        if self.bloom and not self.bloom.contient(cle):
            return None   # Certainement absent

        # Recherche binaire
        lo, hi = 0, len(self._donnees) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if self._donnees[mid].cle == cle:
                return self._donnees[mid]
            elif self._donnees[mid].cle < cle:
                lo = mid + 1
            else:
                hi = mid - 1
        return None

    def taille_octets(self) -> int:
        try:
            return os.path.getsize(self.chemin)
        except FileNotFoundError:
            return 0


# ─── COMPACTION ───────────────────────────────────────────────────────────────

def compacter(tables: list[SSTable], chemin_sortie: str,
              niveau: int) -> SSTable:
    """
    Fusion de N SSTables triées en une seule.
    Merge-sort : avancer dans chaque SSTable simultanément.
    En cas de clef dupliquée : garder l'entrée avec le plus grand sequence.
    Les tombstones sont propagés (et éliminés si c'est la compaction finale).
    """
    fusion: dict[str, Entree] = {}

    for sst in tables:
        for entree in sst._donnees:
            if entree.cle not in fusion:
                fusion[entree.cle] = entree
            else:
                if entree.sequence > fusion[entree.cle].sequence:
                    fusion[entree.cle] = entree

    entrees_triees = sorted(fusion.values(), key=lambda e: e.cle)
    # Éliminer les tombstones lors de la compaction finale (dernier niveau)
    entrees_triees = [e for e in entrees_triees if not e.est_tombstone]

    return SSTable.depuis_memtable(entrees_triees, chemin_sortie, niveau)


# ─── MOTEUR LSM ───────────────────────────────────────────────────────────────

class MoteurLSM:
    """
    Moteur de stockage LSM Tree complet.
    Architecture :
      - 1 MemTable active (écriture)
      - 1 MemTable immutable (en cours de flush)
      - N niveaux de SSTables (L0, L1, L2...)
    """

    TAILLE_MEMTABLE  = 512 * 1024    # 512 KB
    MAX_L0_SSTABLES  = 4             # Déclenche compaction L0→L1
    RATIO_NIVEAUX    = 10            # Chaque niveau est 10× plus grand

    def __init__(self, repertoire: str):
        os.makedirs(repertoire, exist_ok=True)
        self._rep          = repertoire
        self._memtable     = MemTable(self.TAILLE_MEMTABLE)
        self._niveaux: list[list[SSTable]] = [[] for _ in range(7)]
        self._lock         = threading.RLock()
        self._seq          = 0
        self.stats = {
            "ecritures": 0, "lectures": 0,
            "bloom_evites": 0,
            "compactions": 0,
            "flushes": 0,
        }
        self._charger_sstables()

    def _charger_sstables(self):
        """Charger les SSTables existantes au démarrage."""
        for niveau in range(7):
            rep_niveau = os.path.join(self._rep, f"L{niveau}")
            if not os.path.exists(rep_niveau):
                continue
            for nom in sorted(os.listdir(rep_niveau)):
                if nom.endswith(".sst"):
                    chemin = os.path.join(rep_niveau, nom)
                    sst = SSTable.charger(chemin)
                    self._niveaux[niveau].append(sst)

    # ── API publique ──────────────────────────────────────────────────────────

    def ecrire(self, cle: str, valeur: Any):
        with self._lock:
            self.stats["ecritures"] += 1
            self._memtable.ecrire(cle, valeur)
            if self._memtable.est_pleine():
                self._flush()

    def supprimer(self, cle: str):
        with self._lock:
            self._memtable.supprimer(cle)

    def lire(self, cle: str) -> Optional[Any]:
        with self._lock:
            self.stats["lectures"] += 1

            # 1. Chercher dans la MemTable (la plus récente)
            entree = self._memtable.lire(cle)
            if entree is not None:
                return None if entree.est_tombstone else entree.valeur

            # 2. Chercher dans les niveaux (du plus récent au plus ancien)
            for niveau, sstables in enumerate(self._niveaux):
                for sst in reversed(sstables):   # Plus récent en premier
                    # Bloom filter : éviter les lectures inutiles
                    if sst.bloom and not sst.bloom.contient(cle):
                        self.stats["bloom_evites"] += 1
                        continue
                    entree = sst.chercher(cle)
                    if entree is not None:
                        return None if entree.est_tombstone else entree.valeur

            return None

    def flush(self):
        """Forcer le flush de la MemTable même si pas pleine."""
        with self._lock:
            if self._memtable.taille() > 0:
                self._flush()

    def compacter_l0(self):
        """Compacter les SSTables de L0 vers L1."""
        with self._lock:
            if len(self._niveaux[0]) >= 2:
                self._compacter_niveau(0)

    def nb_sstables(self) -> dict[str, int]:
        return {f"L{i}": len(ssts) for i, ssts in enumerate(self._niveaux) if ssts}

    def taille_totale(self) -> int:
        return sum(sst.taille_octets()
                   for niveau in self._niveaux
                   for sst in niveau)

    # ── Opérations internes ───────────────────────────────────────────────────

    def _flush(self):
        """Vider la MemTable vers un nouveau SSTable L0."""
        entrees = self._memtable.iter_trie()
        if not entrees:
            return

        self._seq += 1
        rep_l0 = os.path.join(self._rep, "L0")
        os.makedirs(rep_l0, exist_ok=True)
        chemin = os.path.join(rep_l0, f"{self._seq:06d}.sst")
        sst = SSTable.depuis_memtable(entrees, chemin, niveau=0)
        self._niveaux[0].append(sst)
        self._memtable = MemTable(self.TAILLE_MEMTABLE)
        self.stats["flushes"] += 1

        # Déclencher compaction si L0 est plein
        if len(self._niveaux[0]) >= self.MAX_L0_SSTABLES:
            self._compacter_niveau(0)

    def _compacter_niveau(self, niveau: int):
        """Fusionner les SSTables du niveau N vers N+1."""
        if not self._niveaux[niveau]:
            return

        self._seq += 1
        tables_a_fusionner = list(self._niveaux[niveau])
        if self._niveaux[niveau + 1]:
            tables_a_fusionner += self._niveaux[niveau + 1]

        rep_dest = os.path.join(self._rep, f"L{niveau + 1}")
        os.makedirs(rep_dest, exist_ok=True)
        chemin_sortie = os.path.join(rep_dest, f"{self._seq:06d}.sst")

        sst_compacte = compacter(tables_a_fusionner, chemin_sortie, niveau + 1)

        # Supprimer les anciens fichiers
        for sst in tables_a_fusionner:
            try:
                os.remove(sst.chemin)
            except FileNotFoundError:
                pass

        self._niveaux[niveau]     = []
        self._niveaux[niveau + 1] = [sst_compacte]
        self.stats["compactions"] += 1
