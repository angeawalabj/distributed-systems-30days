"""
Jour 30 — "La Synthèse Finale" — TrueTime & Spanner
=====================================================
Google Spanner (2012) : la première base de données SQL qui est aussi
NewSQL — scalable horizontalement ET ACID globalement.

Le défi impossible :
  CAP theorem dit : vous ne pouvez pas avoir C + A + P simultanément.
  Spanner dit     : "Tenez mon café."

Comment ? En résolvant le vrai problème : LE TEMPS.

Dans un système distribué, "maintenant" n'existe pas.
Chaque machine a sa propre horloge, légèrement décalée.
Si deux transactions s'exécutent "en même temps" sur deux continents,
laquelle vient en premier ?

Solution naïve : Network Time Protocol (NTP)
  Précision : ±100ms à ±1s selon le réseau
  Insuffisant : deux transactions à 50ms d'intervalle sont indiscernables

Solution Spanner : TrueTime
  GPS + horloges atomiques dans chaque datacenter Google
  API retourne [earliest, latest] au lieu d'un instant précis
  Incertitude : ε = ±4ms (garanti)
  
  TrueTime.now() → TrueTimeInterval(earliest=t-ε, latest=t+ε)
  
  Règle d'or : si deux intervalles ne se chevauchent pas,
               l'ordre temporel est réel et garanti.
  
  Spanner attend que l'intervalle soit passé avant de committer.
  → Commit wait : s'assurer que t_commit < t_release pour tout observateur
  → Latence de commit = 2ε en moyenne (8ms)
  → En échange : external consistency garantie globalement

External Consistency :
  Si transaction T1 committe avant que T2 démarre,
  alors timestamp(T1) < timestamp(T2) — TOUJOURS, sur toute la planète.
  
  C'est plus fort que la sérialisabilité classique :
  c'est la sérialisabilité + ordre causal global réel.

Consensus sous-jacent : Paxos (variante multi-Paxos)
  Chaque shard = groupe Paxos (5 répliques, quorum de 3)
  Spanserver : leader Paxos par shard → log répliqué → MVCC timestamps
  
Shard + Timestamp + Paxos = Spanner
  Shard (jour 12)    → scalabilité horizontale
  MVCC  (jour 26)    → lectures sans verrou au timestamp T
  Paxos (≈ Raft j7)  → consensus au sein de chaque shard
  TrueTime           → ordre global cohérent entre shards
"""

from __future__ import annotations
import time, random, math, threading, hashlib
from dataclasses import dataclass, field
from typing import Any, Optional
from collections import defaultdict
from enum import Enum


# ─── TRUETIME ────────────────────────────────────────────────────────────────

@dataclass
class IntervalleTemps:
    """
    TrueTime retourne un intervalle [earliest, latest], pas un instant.
    earliest = maintenant - ε  (garanti : le vrai temps est >= earliest)
    latest   = maintenant + ε  (garanti : le vrai temps est <= latest)
    """
    earliest: float   # timestamp Unix
    latest:   float

    @property
    def milieu(self) -> float:
        return (self.earliest + self.latest) / 2

    @property
    def epsilon_ms(self) -> float:
        return (self.latest - self.earliest) * 1000 / 2

    def apres(self, autre: "IntervalleTemps") -> bool:
        """True si self est CERTAINEMENT après autre (intervalles disjoints)."""
        return self.earliest > autre.latest

    def avant(self, autre: "IntervalleTemps") -> bool:
        """True si self est CERTAINEMENT avant autre."""
        return self.latest < autre.earliest

    def chevauche(self, autre: "IntervalleTemps") -> bool:
        """True si on ne peut pas déterminer l'ordre."""
        return not (self.apres(autre) or self.avant(autre))


class TrueTime:
    """
    API TrueTime de Google — simulée.
    En production : GPS + oscillateurs au rubidium dans chaque datacenter.
    L'incertitude ε garantit que l'horloge est dans [now-ε, now+ε].
    """
    EPSILON_MS = 4.0   # ±4ms — valeur réelle de Google en 2012

    def __init__(self, epsilon_ms: float = None, derive_ms: float = 0.0):
        self._eps    = (epsilon_ms or self.EPSILON_MS) / 1000
        self._derive = derive_ms / 1000   # Dérive simulée de l'horloge locale

    def maintenant(self) -> IntervalleTemps:
        t = time.time() + self._derive
        return IntervalleTemps(
            earliest = t - self._eps,
            latest   = t + self._eps,
        )

    def apres(self, t: float) -> bool:
        """True si le vrai temps actuel est CERTAINEMENT après t."""
        return self.maintenant().earliest > t

    def attendre_apres(self, t: float):
        """
        Commit wait : bloquer jusqu'à ce qu'on soit CERTAIN d'être après t.
        C'est le cœur de l'external consistency de Spanner.
        En pratique : sleep(max(0, t - now.earliest))
        """
        while not self.apres(t):
            time.sleep(0.001)


# ─── MVCC + PAXOS (SPANSERVER) ───────────────────────────────────────────────

@dataclass
class Version:
    """Une version d'une clef dans le MVCC de Spanner."""
    timestamp: float
    valeur:    Any
    tx_id:     str
    supprime:  bool = False


class StockageMVCC:
    """
    Stockage MVCC multi-versions avec timestamps TrueTime.
    Chaque écriture crée une nouvelle version horodatée.
    Les lectures se font à un timestamp T → retourne la version visible à T.
    """

    def __init__(self):
        self._donnees: dict[str, list[Version]] = defaultdict(list)
        self._lock = threading.Lock()

    def ecrire(self, cle: str, valeur: Any, timestamp: float, tx_id: str):
        with self._lock:
            self._donnees[cle].append(
                Version(timestamp=timestamp, valeur=valeur, tx_id=tx_id)
            )
            # Trier par timestamp pour les lectures efficaces
            self._donnees[cle].sort(key=lambda v: v.timestamp)

    def lire(self, cle: str, timestamp: float) -> Optional[Any]:
        """Lire la valeur visible à un timestamp donné (snapshot read)."""
        with self._lock:
            versions = self._donnees.get(cle, [])
        # Trouver la version committée la plus récente <= timestamp
        valeur = None
        for v in versions:
            if v.timestamp <= timestamp and not v.supprime:
                valeur = v.valeur
        return valeur

    def lire_courant(self, cle: str) -> Optional[Any]:
        """Lire la valeur courante (dernier timestamp)."""
        with self._lock:
            versions = self._donnees.get(cle, [])
        if not versions:
            return None
        return versions[-1].valeur if not versions[-1].supprime else None

    def historique(self, cle: str) -> list[Version]:
        with self._lock:
            return list(self._donnees.get(cle, []))

    def toutes_cles(self) -> list[str]:
        with self._lock:
            return list(self._donnees.keys())


class GroupePaxos:
    """
    Groupe Paxos simplifié — 1 leader + N répliques.
    En production : 5 répliques, quorum de 3, géo-distribuées.
    Ici : simulation du consensus pour le log de transactions.
    """

    def __init__(self, nom: str, nb_repliques: int = 5):
        self.nom          = nom
        self.nb_repliques = nb_repliques
        self.quorum       = nb_repliques // 2 + 1
        self._log:    list[dict] = []
        self._repliques_ok: int  = nb_repliques  # Nb répliques actives
        self._lock = threading.Lock()

    def proposer(self, entree: dict) -> bool:
        """
        Proposer une entrée au consensus Paxos.
        Retourne True si le quorum a accepté.
        """
        # Simuler le consensus : quorum accepte si assez de répliques vivantes
        if self._repliques_ok < self.quorum:
            return False
        with self._lock:
            self._log.append({**entree, "index": len(self._log)})
        return True

    def tuer_replique(self):
        self._repliques_ok = max(0, self._repliques_ok - 1)

    def ressusciter_replique(self):
        self._repliques_ok = min(self.nb_repliques, self._repliques_ok + 1)

    def disponible(self) -> bool:
        return self._repliques_ok >= self.quorum

    def log(self) -> list[dict]:
        with self._lock:
            return list(self._log)


# ─── TRANSACTION SPANNER ─────────────────────────────────────────────────────

class EtatTransaction(Enum):
    ACTIVE     = "active"
    COMMITTEE  = "committee"
    ANNULEE    = "annulee"


@dataclass
class Transaction:
    tx_id:      str
    timestamp_lecture: float    # Snapshot timestamp pour les lectures
    timestamp_commit:  Optional[float] = None
    etat:       EtatTransaction = EtatTransaction.ACTIVE
    lectures:   list = field(default_factory=list)
    ecritures:  dict = field(default_factory=dict)  # cle → valeur
    erreur:     Optional[str] = None


class SpannerDB:
    """
    Implémentation simplifiée de Google Spanner.
    
    Propriétés garanties :
    - External Consistency : si T1 committe avant T2, ts(T1) < ts(T2)
    - Snapshot Isolation   : lectures cohérentes à un timestamp
    - Scalabilité          : données shardées sur plusieurs groupes Paxos
    - Haute disponibilité  : quorum Paxos tolère N/2 pannes
    """

    def __init__(self, nb_shards: int = 3, epsilon_ms: float = 4.0):
        self._tt     = TrueTime(epsilon_ms=epsilon_ms)
        self._shards = {
            i: GroupePaxos(f"shard-{i}") for i in range(nb_shards)
        }
        self._stockage = StockageMVCC()
        self._lock     = threading.Lock()
        self._tx_count = 0
        self._stats    = defaultdict(int)

    def _shard_pour(self, cle: str) -> GroupePaxos:
        """Consistent hashing : distribuer les clefs sur les shards."""
        h = int(hashlib.md5(cle.encode()).hexdigest(), 16)
        return self._shards[h % len(self._shards)]

    def _nouveau_tx_id(self) -> str:
        with self._lock:
            self._tx_count += 1
            return f"tx-{self._tx_count:06d}"

    def commencer_transaction(self) -> Transaction:
        """
        Démarrer une transaction en lecture-écriture.
        Le timestamp de lecture est acquis via TrueTime.
        """
        ts_lecture = self._tt.maintenant().milieu
        return Transaction(
            tx_id              = self._nouveau_tx_id(),
            timestamp_lecture  = ts_lecture,
        )

    def lire(self, tx: Transaction, cle: str) -> Any:
        """Lecture snapshot à timestamp_lecture."""
        if tx.etat != EtatTransaction.ACTIVE:
            raise ValueError(f"Transaction {tx.tx_id} non active")
        # Vérifier si on a une écriture non-committée dans cette transaction
        if cle in tx.ecritures:
            return tx.ecritures[cle]
        val = self._stockage.lire(cle, tx.timestamp_lecture)
        tx.lectures.append(cle)
        return val

    def ecrire(self, tx: Transaction, cle: str, valeur: Any):
        """Buffer l'écriture, pas encore committée."""
        if tx.etat != EtatTransaction.ACTIVE:
            raise ValueError(f"Transaction {tx.tx_id} non active")
        tx.ecritures[cle] = valeur

    def committer(self, tx: Transaction) -> bool:
        """
        Committer une transaction avec external consistency.
        
        Algorithme :
        1. Acquérir timestamp_commit via TrueTime.now().latest
        2. Proposer au Paxos de chaque shard concerné
        3. COMMIT WAIT : attendre que TrueTime.now().earliest > timestamp_commit
        4. Appliquer les écritures au MVCC avec timestamp_commit
        
        Grâce au commit wait, tout futur observateur verra ce commit
        APRÈS son propre TrueTime.now().earliest → external consistency ✅
        """
        if tx.etat != EtatTransaction.ACTIVE:
            return False

        # 1. Timestamp de commit = latest (borne haute) pour garantir l'ordre
        tt_commit = self._tt.maintenant()
        ts_commit = tt_commit.latest

        # 2. Consensus Paxos sur les shards impliqués
        shards_impliques = set()
        for cle in tx.ecritures:
            shard = self._shard_pour(cle)
            shards_impliques.add(shard)

        for shard in shards_impliques:
            if not shard.disponible():
                tx.etat   = EtatTransaction.ANNULEE
                tx.erreur = f"Shard {shard.nom} indisponible (quorum perdu)"
                self._stats["tx_annulees"] += 1
                return False

            ok = shard.proposer({
                "tx_id":     tx.tx_id,
                "timestamp": ts_commit,
                "ecritures": list(tx.ecritures.keys()),
            })
            if not ok:
                tx.etat   = EtatTransaction.ANNULEE
                tx.erreur = "Consensus Paxos échoué"
                self._stats["tx_annulees"] += 1
                return False

        # 3. COMMIT WAIT — le cœur de l'external consistency
        self._tt.attendre_apres(ts_commit)

        # 4. Appliquer les écritures
        for cle, valeur in tx.ecritures.items():
            self._stockage.ecrire(cle, valeur, ts_commit, tx.tx_id)

        tx.timestamp_commit = ts_commit
        tx.etat             = EtatTransaction.COMMITTEE
        self._stats["tx_committees"] += 1
        return True

    def lire_snapshot(self, cle: str, timestamp: float) -> Any:
        """
        Lecture snapshot sans transaction (stale read).
        Utilisé pour les analytics et les rapports.
        Aucun verrou, aucun commit wait, ultra-rapide.
        """
        return self._stockage.lire(cle, timestamp)

    def lire_courant(self, cle: str) -> Any:
        """Lecture forte de la valeur courante."""
        return self._stockage.lire_courant(cle)

    def tuer_replique(self, shard_id: int):
        self._shards[shard_id].tuer_replique()

    def ressusciter_replique(self, shard_id: int):
        self._shards[shard_id].ressusciter_replique()

    def stats(self) -> dict:
        return {
            **dict(self._stats),
            "shards_disponibles": sum(
                1 for s in self._shards.values() if s.disponible()
            ),
            "total_shards": len(self._shards),
            "cles_stockees": len(self._stockage.toutes_cles()),
        }
