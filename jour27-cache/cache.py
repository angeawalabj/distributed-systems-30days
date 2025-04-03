"""
Jour 27 — "La Mémoire Collective" — Distributed Cache
=======================================================
Patterns : Cache-Aside, Write-Through, L1/L2, Anti-Stampede
"""

from __future__ import annotations
import time
import threading
import hashlib
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from collections import defaultdict


@dataclass
class EntreeCache:
    cle:       str
    valeur:    Any
    cree_a:    float
    expire_a:  float
    hits:      int = 0
    version:   int = 1

    @property
    def est_valide(self) -> bool:
        return time.time() < self.expire_a

    @property
    def ttl_restant(self) -> float:
        return max(0.0, self.expire_a - time.time())

    @property
    def age(self) -> float:
        return time.time() - self.cree_a


class CacheLocal:
    """
    Cache en mémoire locale (L1) — dans le processus courant.
    Plus rapide que Redis (<0.01ms) mais limité à un seul processus.
    """

    def __init__(self, capacite: int = 1000):
        self._store: dict[str, EntreeCache] = {}
        self._capacite = capacite
        self._lock     = threading.Lock()
        self._stats    = defaultdict(int)

    def get(self, cle: str) -> Optional[Any]:
        with self._lock:
            e = self._store.get(cle)
            if e is None:
                self._stats["miss"] += 1
                return None
            if not e.est_valide:
                del self._store[cle]
                self._stats["miss_expire"] += 1
                return None
            e.hits += 1
            self._stats["hit"] += 1
            return e.valeur

    def set(self, cle: str, valeur: Any, ttl_s: float = 300):
        with self._lock:
            if len(self._store) >= self._capacite:
                self._evict_lru()
            self._store[cle] = EntreeCache(
                cle      = cle,
                valeur   = valeur,
                cree_a   = time.time(),
                expire_a = time.time() + ttl_s,
            )
            self._stats["set"] += 1

    def delete(self, cle: str) -> bool:
        with self._lock:
            if cle in self._store:
                del self._store[cle]
                self._stats["delete"] += 1
                return True
            return False

    def _evict_lru(self):
        if not self._store:
            return
        victime = min(self._store.values(),
                      key=lambda e: e.hits * 1000 - e.age)
        del self._store[victime.cle]
        self._stats["evict"] += 1

    def stats(self) -> dict:
        total    = self._stats["hit"] + self._stats["miss"] + self._stats["miss_expire"]
        hit_rate = self._stats["hit"] / max(total, 1) * 100
        return {**dict(self._stats),
                "taille":   len(self._store),
                "hit_rate": round(hit_rate, 1)}


class CacheDistribue:
    """
    Simule un cluster Redis avec plusieurs shards (consistent hashing).
    Supporte pub/sub pour les invalidations cross-instance.
    """

    def __init__(self, nb_shards: int = 3):
        self._shards:  list[dict[str, EntreeCache]] = [{} for _ in range(nb_shards)]
        self._locks:   list[threading.Lock] = [threading.Lock() for _ in range(nb_shards)]
        self._nb_shards = nb_shards
        self._stats     = defaultdict(int)
        self._pubsub:   list[tuple[str, Callable]] = []

    def _shard(self, cle: str) -> int:
        h = int(hashlib.md5(cle.encode()).hexdigest(), 16)
        return h % self._nb_shards

    def get(self, cle: str) -> Optional[Any]:
        s = self._shard(cle)
        with self._locks[s]:
            e = self._shards[s].get(cle)
            if e is None:
                self._stats["miss"] += 1
                return None
            if not e.est_valide:
                del self._shards[s][cle]
                self._stats["miss_expire"] += 1
                return None
            e.hits += 1
            self._stats["hit"] += 1
            return e.valeur

    def set(self, cle: str, valeur: Any, ttl_s: float = 300,
            version: int = 1):
        s = self._shard(cle)
        with self._locks[s]:
            self._shards[s][cle] = EntreeCache(
                cle      = cle,
                valeur   = valeur,
                cree_a   = time.time(),
                expire_a = time.time() + ttl_s,
                version  = version,
            )
            self._stats["set"] += 1
        self._notifier(f"set:{cle}", valeur)

    def delete(self, cle: str) -> bool:
        s = self._shard(cle)
        with self._locks[s]:
            if cle in self._shards[s]:
                del self._shards[s][cle]
                self._stats["delete"] += 1
                self._notifier(f"del:{cle}", None)
                return True
        return False

    def delete_pattern(self, pattern: str) -> int:
        import re
        supprimees = 0
        regex = "^" + re.escape(pattern).replace(r"\*", ".*") + "$"
        for s, shard in enumerate(self._shards):
            with self._locks[s]:
                cles = [k for k in list(shard.keys()) if re.match(regex, k)]
                for cle in cles:
                    del shard[cle]
                    supprimees += 1
        self._stats["delete"] += supprimees
        return supprimees

    def subscribe(self, pattern: str, callback: Callable):
        self._pubsub.append((pattern, callback))

    def _notifier(self, event: str, valeur: Any):
        import re
        for pattern, cb in self._pubsub:
            regex = "^" + re.escape(pattern).replace(r"\*", ".*") + "$"
            if re.match(regex, event):
                cb(event, valeur)

    def taille_totale(self) -> int:
        return sum(len(s) for s in self._shards)

    def stats(self) -> dict:
        total = self._stats["hit"] + self._stats["miss"] + self._stats["miss_expire"]
        return {
            **dict(self._stats),
            "taille":       self.taille_totale(),
            "hit_rate":     round(self._stats["hit"] / max(total, 1) * 100, 1),
            "shards_sizes": [len(s) for s in self._shards],
        }


class CacheAside:
    """Pattern Cache-Aside (lazy loading)."""

    def __init__(self, cache: CacheDistribue,
                 source: Callable[[str], Any],
                 ttl_s: float = 300):
        self._cache  = cache
        self._source = source
        self._ttl    = ttl_s
        self._stats  = defaultdict(int)

    def get(self, cle: str) -> Any:
        val = self._cache.get(cle)
        if val is not None:
            self._stats["hit"] += 1
            return val
        self._stats["miss"] += 1
        val = self._source(cle)
        if val is not None:
            self._cache.set(cle, val, self._ttl)
        return val

    def invalidate(self, cle: str):
        self._cache.delete(cle)
        self._stats["invalidation"] += 1

    def stats(self) -> dict:
        return dict(self._stats)


class AntiStampede:
    """
    Protection thundering herd : mutex par clef.
    Un seul thread recalcule, les autres attendent le résultat.
    """

    def __init__(self, cache: CacheDistribue,
                 source: Callable[[str], Any],
                 ttl_s: float = 300):
        self._cache  = cache
        self._source = source
        self._ttl    = ttl_s
        self._locks: dict[str, threading.Event] = {}
        self._lock   = threading.Lock()
        self._stats  = defaultdict(int)

    def get(self, cle: str) -> Any:
        val = self._cache.get(cle)
        if val is not None:
            self._stats["hit"] += 1
            return val

        with self._lock:
            val = self._cache.get(cle)
            if val is not None:
                self._stats["hit_double_check"] += 1
                return val

            if cle in self._locks:
                event = self._locks[cle]
                self._stats["attentes"] += 1
            else:
                event = threading.Event()
                self._locks[cle] = event
                event = None   # Ce thread recalcule

        if event is not None:
            event.wait(timeout=5.0)
            return self._cache.get(cle)

        try:
            self._stats["recalculs"] += 1
            val = self._source(cle)
            if val is not None:
                self._cache.set(cle, val, self._ttl)
            return val
        finally:
            with self._lock:
                ev = self._locks.pop(cle, None)
                if ev:
                    ev.set()

    def stats(self) -> dict:
        return dict(self._stats)
