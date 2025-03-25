"""
Jour 23 — Backpressure & Queue Management
==========================================
Problème : les producteurs envoient plus vite que les consommateurs traitent.

  Producteur ──▶ [Queue ???] ──▶ Consommateur
     1000/s         ???              100/s

Sans contrôle : la queue grossit sans cesse → OOM → crash.
La différence avec le Rate Limiting (Jour 22) :
  Rate Limiting : le SERVEUR dit non au CLIENT ("429 Too Many Requests")
  Backpressure  : le CONSOMMATEUR signale sa saturation au PRODUCTEUR
                  pour qu'il ralentisse voluntairement

Quatre stratégies quand la queue est pleine :

  1. BLOCK (backpressure pure) :
     Le producteur est bloqué jusqu'à ce qu'il y ait de la place.
     ✅ Aucune perte de données
     ❌ Propagation de la lenteur vers l'amont (cascade)
     Usage : pipelines de traitement de données (Kafka consumers)

  2. DROP NEWEST (load shedding) :
     Rejeter les nouvelles requêtes quand la queue est pleine.
     ✅ Protège le système, latence des requêtes existantes inchangée
     ❌ Perte de données
     Usage : métriques, logs (perte acceptable)

  3. DROP OLDEST :
     Éjecter la plus ancienne requête pour accueillir la nouvelle.
     ✅ Données récentes prioritaires (capteurs, temps réel)
     ❌ Perte des requêtes les plus anciennes
     Usage : flux vidéo, IoT, trading (fraîcheur > complétude)

  4. PRIORITY QUEUE :
     File de priorité : haute priorité passe devant, basse priorité éjectée.
     ✅ SLA différenciés (premium vs free)
     ❌ Famine possible pour les basses priorités
     Usage : APIs avec tiers payants, retry avec priority

  + WORK STEALING :
     Plusieurs workers. Quand un worker est idle, il "vole"
     des tâches dans la queue d'un worker surchargé.
     ✅ Meilleure utilisation des ressources, auto-balancing
     Usage : thread pools, ForkJoinPool Java, Tokio Rust
"""

from __future__ import annotations
import time
import threading
import heapq
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from enum import Enum


@dataclass
class Tache:
    id:         str
    priorite:   int       # Plus petit = plus prioritaire (min-heap)
    payload:    Any
    cree_a:     float = field(default_factory=time.time)
    traitee_a:  Optional[float] = None

    @property
    def latence_ms(self) -> Optional[float]:
        if self.traitee_a:
            return (self.traitee_a - self.cree_a) * 1000
        return None

    def __lt__(self, other: "Tache"):
        return self.priorite < other.priorite


class StrategieQueue(Enum):
    BLOCK       = "block"
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"
    PRIORITY    = "priority"


# ─── QUEUE BORNÉE ────────────────────────────────────────────────────────────

class QueueBornee:
    """
    Queue à capacité fixe avec stratégie configurable.
    Mesure précisément les pertes, latences et pression.
    """

    def __init__(self, capacite: int, strategie: StrategieQueue):
        self.capacite  = capacite
        self.strategie = strategie
        self._lock     = threading.Lock()
        self._non_vide = threading.Condition(self._lock)

        if strategie == StrategieQueue.PRIORITY:
            self._heap: list[Tache] = []   # min-heap
        else:
            self._deque: deque[Tache] = deque()

        self.stats = {
            "entrees":   0,
            "traitees":  0,
            "perdues":   0,
            "max_taille": 0,
        }
        self._latences: list[float] = []

    def enqueue(self, tache: Tache, timeout_s: float = 5.0) -> bool:
        """
        Ajouter une tâche. Retourne True si acceptée, False si rejetée/perdue.
        En mode BLOCK, attend jusqu'à timeout_s.
        """
        with self._non_vide:
            self.stats["entrees"] += 1

            if self.strategie == StrategieQueue.BLOCK:
                deadline = time.time() + timeout_s
                while self._taille() >= self.capacite:
                    delai = deadline - time.time()
                    if delai <= 0:
                        self.stats["perdues"] += 1
                        return False
                    self._non_vide.wait(timeout=delai)
                self._push(tache)

            elif self.strategie == StrategieQueue.DROP_NEWEST:
                if self._taille() >= self.capacite:
                    self.stats["perdues"] += 1
                    return False
                self._push(tache)

            elif self.strategie == StrategieQueue.DROP_OLDEST:
                if self._taille() >= self.capacite:
                    self._pop_oldest()
                    self.stats["perdues"] += 1
                self._push(tache)

            elif self.strategie == StrategieQueue.PRIORITY:
                if self._taille() >= self.capacite:
                    # Éjecter la tâche de plus basse priorité si la nouvelle est meilleure
                    if tache.priorite < self._worst_priority():
                        self._eject_worst()
                        self.stats["perdues"] += 1
                    else:
                        self.stats["perdues"] += 1
                        return False
                self._push(tache)

            self.stats["max_taille"] = max(self.stats["max_taille"], self._taille())
            self._non_vide.notify_all()
            return True

    def dequeue(self, timeout_s: float = 1.0) -> Optional[Tache]:
        with self._non_vide:
            while self._taille() == 0:
                if not self._non_vide.wait(timeout=timeout_s):
                    return None
            tache = self._pop()
            tache.traitee_a = time.time()
            self.stats["traitees"] += 1
            self._latences.append(tache.latence_ms)
            self._non_vide.notify_all()
            return tache

    def taille(self) -> int:
        with self._lock:
            return self._taille()

    def pression(self) -> float:
        """Remplissage de la queue en % (indicateur de backpressure)."""
        with self._lock:
            return self._taille() / self.capacite * 100

    def latence_mediane_ms(self) -> float:
        with self._lock:
            if not self._latences:
                return 0.0
            s = sorted(self._latences)
            return s[len(s) // 2]

    def latence_p99_ms(self) -> float:
        with self._lock:
            if not self._latences:
                return 0.0
            s = sorted(self._latences)
            return s[int(len(s) * 0.99)]

    # ── Helpers internes ─────────────────────────────────────────────────────

    def _taille(self) -> int:
        if self.strategie == StrategieQueue.PRIORITY:
            return len(self._heap)
        return len(self._deque)

    def _push(self, tache: Tache):
        if self.strategie == StrategieQueue.PRIORITY:
            heapq.heappush(self._heap, tache)
        else:
            self._deque.append(tache)

    def _pop(self) -> Tache:
        if self.strategie == StrategieQueue.PRIORITY:
            return heapq.heappop(self._heap)
        return self._deque.popleft()

    def _pop_oldest(self) -> Tache:
        return self._deque.popleft()

    def _worst_priority(self) -> int:
        if not self._heap:
            return -1
        return max(t.priorite for t in self._heap)

    def _eject_worst(self):
        if not self._heap:
            return
        worst_idx = max(range(len(self._heap)),
                        key=lambda i: self._heap[i].priorite)
        self._heap[worst_idx] = self._heap[-1]
        self._heap.pop()
        heapq.heapify(self._heap)


# ─── WORKER POOL ─────────────────────────────────────────────────────────────

class Worker:
    """Worker qui consomme des tâches depuis une queue."""

    def __init__(self, worker_id: int, queue: QueueBornee,
                 fn_traitement: Callable[[Tache], None],
                 latence_traitement_ms: float = 10.0):
        self.id          = worker_id
        self.queue       = queue
        self.fn          = fn_traitement
        self.latence_ms  = latence_traitement_ms
        self._actif      = False
        self._thread:    Optional[threading.Thread] = None
        self.traitees    = 0

    def demarrer(self):
        self._actif = True
        self._thread = threading.Thread(target=self._boucle, daemon=True)
        self._thread.start()

    def arreter(self):
        self._actif = False

    def _boucle(self):
        while self._actif:
            tache = self.queue.dequeue(timeout_s=0.2)
            if tache:
                time.sleep(self.latence_ms / 1000)
                self.fn(tache)
                self.traitees += 1


# ─── WORK STEALING ───────────────────────────────────────────────────────────

class WorkStealingPool:
    """
    Pool de workers avec work stealing.
    Chaque worker a sa propre queue locale.
    Quand un worker est idle, il "vole" des tâches à un worker surchargé.

    Avantage : meilleure utilisation CPU, auto-équilibrage de charge.
    Utilisé par : ForkJoinPool (Java), Tokio (Rust), Go runtime.
    """

    def __init__(self, nb_workers: int, capacite_par_worker: int,
                 latence_traitement_ms: float = 10.0):
        self.nb_workers  = nb_workers
        self._queues = [
            deque() for _ in range(nb_workers)
        ]
        self._locks  = [threading.Lock() for _ in range(nb_workers)]
        self.latence = latence_traitement_ms / 1000
        self.stats   = {
            "traitees": 0, "vols": 0,
            "par_worker": [0] * nb_workers,
        }
        self._actif  = False

    def soumettre(self, tache: Tache) -> int:
        """Soumettre une tâche au worker le moins chargé."""
        idx = min(range(self.nb_workers),
                  key=lambda i: len(self._queues[i]))
        with self._locks[idx]:
            self._queues[idx].append(tache)
        return idx

    def demarrer(self):
        self._actif = True
        for i in range(self.nb_workers):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            t.start()

    def arreter(self):
        self._actif = False

    def _worker_loop(self, worker_id: int):
        while self._actif:
            tache = self._prendre_ou_voler(worker_id)
            if tache:
                time.sleep(self.latence)
                self.stats["traitees"] += 1
                self.stats["par_worker"][worker_id] += 1
            else:
                time.sleep(0.001)

    def _prendre_ou_voler(self, worker_id: int) -> Optional[Tache]:
        # Essayer sa propre queue
        with self._locks[worker_id]:
            if self._queues[worker_id]:
                return self._queues[worker_id].popleft()

        # Queue vide → chercher un worker avec du travail à voler
        for autre_id in range(self.nb_workers):
            if autre_id == worker_id:
                continue
            with self._locks[autre_id]:
                if len(self._queues[autre_id]) > 1:
                    # Vol : prendre la tâche la plus récente (fin de deque)
                    tache = self._queues[autre_id].pop()
                    self.stats["vols"] += 1
                    return tache
        return None

    def charge_par_worker(self) -> list[int]:
        return [len(q) for q in self._queues]
