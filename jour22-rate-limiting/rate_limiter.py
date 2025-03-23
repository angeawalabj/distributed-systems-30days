"""
Jour 22 — Rate Limiting & Throttling
=====================================
4 algorithmes, 4 compromis :

  1. Fixed Window   : O(1), simple, mais burst en bordure de fenêtre
  2. Sliding Window : O(N), précis, pas de burst
  3. Token Bucket   : O(1), bursts contrôlés, le plus répandu (AWS, Stripe)
  4. Leaky Bucket   : O(1), débit constant garanti, lisse le trafic
"""

from __future__ import annotations
import time
import threading
from collections import deque
from dataclasses import dataclass


@dataclass
class Resultat:
    ok:         bool
    restantes:  int
    retry_s:    float
    algo:       str


# ─── 1. FIXED WINDOW ─────────────────────────────────────────────────────────

class FixedWindow:
    """Fenêtre fixe. Problème : 2×limite en 2ε secondes en bordure."""

    def __init__(self, limite: int, fenetre_s: float):
        self.limite    = limite
        self.fenetre_s = fenetre_s
        self._count    = 0
        self._debut    = time.time()
        self._lock     = threading.Lock()

    def autoriser(self) -> Resultat:
        with self._lock:
            now = time.time()
            if now - self._debut >= self.fenetre_s:
                self._count = 0
                self._debut = now
            if self._count < self.limite:
                self._count += 1
                return Resultat(True,  self.limite - self._count,
                                self.fenetre_s - (now - self._debut), "fixed_window")
            return Resultat(False, 0,
                            self.fenetre_s - (now - self._debut), "fixed_window")

    def etat(self):
        with self._lock:
            return {"count": self._count, "limite": self.limite,
                    "reset_s": max(0, self.fenetre_s - (time.time() - self._debut))}


# ─── 2. SLIDING WINDOW LOG ────────────────────────────────────────────────────

class SlidingWindowLog:
    """Journal glissant. Précis, O(N) mémoire."""

    def __init__(self, limite: int, fenetre_s: float):
        self.limite    = limite
        self.fenetre_s = fenetre_s
        self._log: deque[float] = deque()
        self._lock     = threading.Lock()

    def autoriser(self) -> Resultat:
        with self._lock:
            now   = time.time()
            seuil = now - self.fenetre_s
            while self._log and self._log[0] <= seuil:
                self._log.popleft()
            if len(self._log) < self.limite:
                self._log.append(now)
                reset = (self._log[0] + self.fenetre_s - now) if self._log else 0
                return Resultat(True, self.limite - len(self._log), reset, "sliding_log")
            reset = self._log[0] + self.fenetre_s - now
            return Resultat(False, 0, max(0, reset), "sliding_log")

    def etat(self):
        with self._lock:
            return {"dans_fenetre": len(self._log), "limite": self.limite}


# ─── 3. TOKEN BUCKET ─────────────────────────────────────────────────────────

class TokenBucket:
    """
    Seau à jetons. Capacité C, recharge R/s.
    Bursts jusqu'à C, débit moyen ≤ R.
    Utilisé par AWS API Gateway, Stripe, GitHub.
    """

    def __init__(self, capacite: int, recharge_par_s: float):
        self.capacite       = capacite
        self.recharge_par_s = recharge_par_s
        self._jetons        = float(capacite)
        self._derniere_maj  = time.time()
        self._lock          = threading.Lock()

    def autoriser(self, cout: int = 1) -> Resultat:
        with self._lock:
            self._recharger()
            if self._jetons >= cout:
                self._jetons -= cout
                return Resultat(True, int(self._jetons), 0, "token_bucket")
            retry = (cout - self._jetons) / self.recharge_par_s
            return Resultat(False, 0, retry, "token_bucket")

    def _recharger(self):
        now = time.time()
        self._jetons = min(self.capacite,
                           self._jetons + (now - self._derniere_maj) * self.recharge_par_s)
        self._derniere_maj = now

    def etat(self):
        with self._lock:
            self._recharger()
            return {"jetons": round(self._jetons, 2), "capacite": self.capacite,
                    "recharge_s": self.recharge_par_s}


# ─── 4. LEAKY BUCKET ─────────────────────────────────────────────────────────

class LeakyBucket:
    """
    Seau percé. File d'attente traitée à débit constant.
    Débit de sortie strictement lissé, quelle que soit l'entrée.
    Utilisé par Nginx (limit_req_zone).
    """

    def __init__(self, capacite: int, debit_par_s: float):
        self.capacite    = capacite
        self.debit_par_s = debit_par_s
        self._file       = 0.0
        self._dernier    = time.time()
        self._lock       = threading.Lock()

    def autoriser(self) -> Resultat:
        with self._lock:
            now   = time.time()
            delta = now - self._dernier
            self._file   = max(0.0, self._file - delta * self.debit_par_s)
            self._dernier = now
            if self._file < self.capacite:
                self._file += 1
                return Resultat(True, int(self.capacite - self._file),
                                self._file / self.debit_par_s, "leaky_bucket")
            return Resultat(False, 0, self._file / self.debit_par_s, "leaky_bucket")

    def etat(self):
        with self._lock:
            now = time.time()
            f   = max(0.0, self._file - (now - self._dernier) * self.debit_par_s)
            return {"dans_file": round(f, 2), "capacite": self.capacite,
                    "debit_s": self.debit_par_s}


# ─── RATE LIMITER PAR CLEF (MULTI-TENANT) ────────────────────────────────────

class RateLimiterMultiTenant:
    """
    Rate limiter par clef (IP, user_id, api_key…).
    Chaque clef a son propre Token Bucket.
    Utilisé pour les APIs publiques : chaque client a sa propre limite.
    """

    def __init__(self, capacite: int, recharge_par_s: float):
        self.capacite       = capacite
        self.recharge_par_s = recharge_par_s
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def autoriser(self, cle: str, cout: int = 1) -> Resultat:
        with self._lock:
            if cle not in self._buckets:
                self._buckets[cle] = TokenBucket(self.capacite, self.recharge_par_s)
        return self._buckets[cle].autoriser(cout)

    def etats(self) -> dict:
        with self._lock:
            return {cle: b.etat() for cle, b in self._buckets.items()}
