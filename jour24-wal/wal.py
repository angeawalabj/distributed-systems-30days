"""
Jour 24 — Write-Ahead Log (WAL) & Storage Engine
==================================================
Problème : une base de données écrit en mémoire (rapide) puis sur disque (lent).
Si le système crashe entre les deux → données perdues ou corrompues.

  SANS WAL :
    1. Modifier les pages en mémoire  (rapide, O(1))
    2. Écrire les pages sur disque    (lent, peut crasher ici ←)
    → Crash = état incohérent entre mémoire et disque

  AVEC WAL :
    1. Écrire l'opération dans le WAL (append séquentiel = rapide)
    2. Retourner "commit OK" au client
    3. Plus tard : écrire les pages sur disque (checkpoint)
    → Crash = rejouer le WAL pour retrouver l'état cohérent

Le WAL est la fondation de la durabilité dans TOUS les moteurs de BDD :
  PostgreSQL   : WAL (Write-Ahead Logging)
  MySQL InnoDB : Redo Log
  SQLite       : WAL mode ou journal mode
  RocksDB      : Write-Ahead Log
  Kafka        : le topic = un WAL append-only

Propriétés clés :
  Append-only  : on n'écrit qu'à la fin (séquentiel = rapide sur disque SSD/HDD)
  Séquencé     : chaque entrée a un LSN (Log Sequence Number) monotone
  Durable      : fsync() avant de répondre "commit OK"
  Idempotent   : rejouer le WAL deux fois = même résultat

Crash recovery :
  1. Chercher le dernier checkpoint (état de référence)
  2. Rejouer toutes les entrées WAL après ce checkpoint
  3. Annuler les transactions non commitées (UNDO)
  4. Appliquer les transactions commitées (REDO)

LSN = Log Sequence Number : numéro monotone qui identifie une position dans le WAL.
      Utilisé pour savoir quelles entrées ont déjà été appliquées au checkpoint.
"""

from __future__ import annotations
import os
import json
import time
import threading
import struct
import hashlib
from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum


class TypeEntree(Enum):
    BEGIN      = "BEGIN"      # Début de transaction
    WRITE      = "WRITE"      # Écriture d'une valeur
    DELETE     = "DELETE"     # Suppression d'une clef
    COMMIT     = "COMMIT"     # Transaction commitée (durable)
    ABORT      = "ABORT"      # Transaction annulée
    CHECKPOINT = "CHECKPOINT" # Point de référence pour la recovery


@dataclass
class EntreeWAL:
    lsn:        int           # Log Sequence Number (position unique)
    type:       TypeEntree
    tx_id:      str           # ID de transaction
    cle:        Optional[str] = None
    valeur:     Optional[Any] = None
    valeur_avant: Optional[Any] = None  # Pour UNDO
    timestamp:  float = field(default_factory=time.time)

    def serialiser(self) -> bytes:
        """Sérialisation binaire : length-prefixed JSON + CRC32."""
        data = json.dumps({
            "lsn":         self.lsn,
            "type":        self.type.value,
            "tx_id":       self.tx_id,
            "cle":         self.cle,
            "valeur":      self.valeur,
            "valeur_avant": self.valeur_avant,
            "timestamp":   self.timestamp,
        }, ensure_ascii=False).encode()
        crc = int(hashlib.md5(data).hexdigest()[:8], 16)
        # Format : [4 bytes longueur][4 bytes CRC][N bytes données]
        return struct.pack(">II", len(data), crc) + data

    @staticmethod
    def deserialiser(raw: bytes) -> tuple["EntreeWAL", int]:
        """Retourne (entree, nb_bytes_lus)."""
        if len(raw) < 8:
            raise ValueError("Entrée tronquée")
        longueur, crc_attendu = struct.unpack(">II", raw[:8])
        if len(raw) < 8 + longueur:
            raise ValueError("Données incomplètes")
        data = raw[8:8 + longueur]
        crc_calcule = int(hashlib.md5(data).hexdigest()[:8], 16)
        if crc_calcule != crc_attendu:
            raise ValueError(f"Corruption CRC : {crc_calcule} ≠ {crc_attendu}")
        d = json.loads(data)
        return EntreeWAL(
            lsn         = d["lsn"],
            type        = TypeEntree(d["type"]),
            tx_id       = d["tx_id"],
            cle         = d.get("cle"),
            valeur      = d.get("valeur"),
            valeur_avant = d.get("valeur_avant"),
            timestamp   = d["timestamp"],
        ), 8 + longueur


# ─── WAL SUR DISQUE ──────────────────────────────────────────────────────────

class WAL:
    """
    Write-Ahead Log : fichier append-only sur disque.
    Chaque écriture est suivie d'un fsync() pour garantir la durabilité.
    """

    def __init__(self, chemin: str):
        self.chemin    = chemin
        self._lsn      = 0
        self._lock     = threading.Lock()
        self._fh       = None
        self._ouvrir()

    def _ouvrir(self):
        self._fh = open(self.chemin, "ab")  # append binary
        # Récupérer le dernier LSN si le fichier existe déjà
        self._lsn = self._dernier_lsn() + 1

    def _dernier_lsn(self) -> int:
        entrees = self.lire_tout()
        return entrees[-1].lsn if entrees else 0

    def ecrire(self, type_: TypeEntree, tx_id: str,
               cle: str = None, valeur: Any = None,
               valeur_avant: Any = None) -> EntreeWAL:
        with self._lock:
            entree = EntreeWAL(
                lsn          = self._lsn,
                type         = type_,
                tx_id        = tx_id,
                cle          = cle,
                valeur       = valeur,
                valeur_avant = valeur_avant,
            )
            raw = entree.serialiser()
            self._fh.write(raw)
            self._fh.flush()
            os.fsync(self._fh.fileno())   # ← garantit la durabilité
            self._lsn += 1
            return entree

    def lire_depuis(self, lsn_debut: int = 0) -> list[EntreeWAL]:
        """Lire toutes les entrées depuis un LSN donné."""
        return [e for e in self.lire_tout() if e.lsn >= lsn_debut]

    def lire_tout(self) -> list[EntreeWAL]:
        """Lire et parser tout le fichier WAL."""
        entrees = []
        try:
            with open(self.chemin, "rb") as f:
                raw = f.read()
            pos = 0
            while pos < len(raw):
                entree, nb = EntreeWAL.deserialiser(raw[pos:])
                entrees.append(entree)
                pos += nb
        except (FileNotFoundError, ValueError):
            pass
        return entrees

    def taille_octets(self) -> int:
        try:
            return os.path.getsize(self.chemin)
        except FileNotFoundError:
            return 0

    def fermer(self):
        if self._fh:
            self._fh.close()
            self._fh = None

    def supprimer(self):
        self.fermer()
        try:
            os.remove(self.chemin)
        except FileNotFoundError:
            pass


# ─── CHECKPOINT ──────────────────────────────────────────────────────────────

@dataclass
class Checkpoint:
    lsn:       int     # LSN du checkpoint
    etat:      dict    # Snapshot de l'état à ce LSN
    timestamp: float = field(default_factory=time.time)

    def sauvegarder(self, chemin: str):
        with open(chemin, "w") as f:
            json.dump({"lsn": self.lsn, "etat": self.etat,
                       "timestamp": self.timestamp}, f)

    @staticmethod
    def charger(chemin: str) -> Optional["Checkpoint"]:
        try:
            with open(chemin) as f:
                d = json.load(f)
            return Checkpoint(lsn=d["lsn"], etat=d["etat"],
                               timestamp=d["timestamp"])
        except (FileNotFoundError, json.JSONDecodeError):
            return None


# ─── MOTEUR DE STOCKAGE ───────────────────────────────────────────────────────

class MoteurStockage:
    """
    Mini moteur de stockage clé-valeur avec WAL.
    Implémente ACID via :
      A (Atomicité)   : BEGIN/COMMIT/ABORT
      C (Cohérence)   : validation avant commit
      I (Isolation)   : verrou par transaction (simplifié)
      D (Durabilité)  : WAL + fsync avant commit
    """

    def __init__(self, repertoire: str):
        os.makedirs(repertoire, exist_ok=True)
        self._rep           = repertoire
        self._chemin_wal    = os.path.join(repertoire, "wal.log")
        self._chemin_ckpt   = os.path.join(repertoire, "checkpoint.json")
        self._wal           = WAL(self._chemin_wal)
        self._donnees: dict = {}          # Données commitées en mémoire
        self._tx_en_cours: dict = {}      # tx_id → {cle: (valeur, valeur_avant)}
        self._lock          = threading.RLock()
        self._dernier_ckpt_lsn = 0

        # Recovery au démarrage
        self._recovery()

    # ── API publique ──────────────────────────────────────────────────────────

    def begin(self, tx_id: str):
        """Démarre une transaction."""
        with self._lock:
            self._tx_en_cours[tx_id] = {}
            self._wal.ecrire(TypeEntree.BEGIN, tx_id)

    def ecrire(self, tx_id: str, cle: str, valeur: Any):
        """Écrire dans une transaction (buffering en mémoire)."""
        with self._lock:
            if tx_id not in self._tx_en_cours:
                raise ValueError(f"Transaction {tx_id} non démarrée")
            valeur_avant = self._donnees.get(cle)
            self._tx_en_cours[tx_id][cle] = (valeur, valeur_avant)
            self._wal.ecrire(TypeEntree.WRITE, tx_id,
                             cle=cle, valeur=valeur, valeur_avant=valeur_avant)

    def supprimer(self, tx_id: str, cle: str):
        """Supprimer une clef dans une transaction."""
        with self._lock:
            if tx_id not in self._tx_en_cours:
                raise ValueError(f"Transaction {tx_id} non démarrée")
            valeur_avant = self._donnees.get(cle)
            self._tx_en_cours[tx_id][cle] = (None, valeur_avant)
            self._wal.ecrire(TypeEntree.DELETE, tx_id,
                             cle=cle, valeur_avant=valeur_avant)

    def commit(self, tx_id: str):
        """
        Commiter une transaction :
          1. Écrire COMMIT dans le WAL (avec fsync)
          2. Appliquer les changements en mémoire
        """
        with self._lock:
            if tx_id not in self._tx_en_cours:
                raise ValueError(f"Transaction {tx_id} non démarrée")
            # Écriture durable (fsync)
            self._wal.ecrire(TypeEntree.COMMIT, tx_id)
            # Appliquer en mémoire
            for cle, (valeur, _) in self._tx_en_cours[tx_id].items():
                if valeur is None:
                    self._donnees.pop(cle, None)
                else:
                    self._donnees[cle] = valeur
            del self._tx_en_cours[tx_id]

    def abort(self, tx_id: str):
        """Annuler une transaction (rollback)."""
        with self._lock:
            if tx_id not in self._tx_en_cours:
                return
            self._wal.ecrire(TypeEntree.ABORT, tx_id)
            del self._tx_en_cours[tx_id]

    def lire(self, cle: str) -> Optional[Any]:
        with self._lock:
            return self._donnees.get(cle)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._donnees)

    # ── Checkpoint ────────────────────────────────────────────────────────────

    def checkpoint(self):
        """
        Sauvegarder l'état courant sur disque.
        Après un checkpoint, on n'a besoin de rejouer que les entrées WAL
        postérieures au checkpoint LSN.
        """
        with self._lock:
            lsn_actuel = self._wal._lsn - 1
            ckpt = Checkpoint(lsn=lsn_actuel, etat=dict(self._donnees))
            ckpt.sauvegarder(self._chemin_ckpt)
            self._dernier_ckpt_lsn = lsn_actuel
            self._wal.ecrire(TypeEntree.CHECKPOINT, "system")
            return ckpt

    # ── Recovery après crash ──────────────────────────────────────────────────

    def _recovery(self):
        """
        Recovery ARIES-style simplifié :
          1. Charger le dernier checkpoint
          2. Rejouer les WRITE/DELETE des transactions commitées
          3. Ignorer les transactions non commitées (ABORT implicite)
        """
        ckpt = Checkpoint.charger(self._chemin_ckpt)
        if ckpt:
            self._donnees = dict(ckpt.etat)
            lsn_debut = ckpt.lsn + 1
            self._dernier_ckpt_lsn = ckpt.lsn
        else:
            lsn_debut = 0

        entrees = self._wal.lire_depuis(lsn_debut)
        if not entrees:
            return

        # Identifier les transactions commitées
        tx_commitees = {e.tx_id for e in entrees if e.type == TypeEntree.COMMIT}
        tx_abortees  = {e.tx_id for e in entrees if e.type == TypeEntree.ABORT}

        # REDO : appliquer les changements des transactions commitées
        for e in entrees:
            if e.tx_id not in tx_commitees:
                continue  # UNDO implicite : ignorer les non-commitées
            if e.type == TypeEntree.WRITE and e.cle:
                self._donnees[e.cle] = e.valeur
            elif e.type == TypeEntree.DELETE and e.cle:
                self._donnees.pop(e.cle, None)

    def fermer(self):
        self._wal.fermer()

    def nettoyer(self):
        """Supprimer tous les fichiers (pour les tests)."""
        self.fermer()
        for f in [self._chemin_wal, self._chemin_ckpt]:
            try:
                os.remove(f)
            except FileNotFoundError:
                pass
