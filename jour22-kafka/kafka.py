"""
Jour 22 — "Le Fleuve Infini" — Kafka
======================================
Apache Kafka : système de messagerie distribué basé sur un log append-only.

Problème : connecter N producteurs à M consommateurs sans couplage direct.
  Sans Kafka :
    Service A → appelle directement Service B → couplage fort
    Si B est lent → A attend ou perd des données
    Si B tombe → A perd des messages

  Avec Kafka :
    Producteur → écrit dans un Topic Kafka (log durable)
    Consommateur → lit depuis ce Topic à son rythme
    → Découplage total : A et B ne se connaissent pas

Concepts fondamentaux :

  TOPIC : canal de communication nommé (ex: "orders", "clicks")
    Un topic = une table dans une base de données, mais en append-only.

  PARTITION : subdivision d'un topic pour le parallélisme
    Topic "orders" avec 4 partitions → 4 flux indépendants.
    Chaque partition est un log ordonné et immuable.
    Clef de partitionnement : hash(clef) % nb_partitions
    → Tous les messages avec la même clef → même partition → ordre garanti

  OFFSET : position d'un message dans une partition
    Offset 0, 1, 2, 3... → monotone croissant par partition.
    Les consommateurs retiennent leur offset pour reprendre où ils en étaient.

  CONSUMER GROUP : groupe de consommateurs qui se partagent les partitions
    4 partitions + 2 consommateurs → 2 partitions par consommateur
    4 partitions + 4 consommateurs → 1 partition par consommateur
    4 partitions + 6 consommateurs → 2 consommateurs inactifs (on ne peut pas
                                     diviser une partition)
    → Scalabilité : ajouter des consommateurs = ajouter du parallélisme

  RETENTION : durée de conservation des messages
    Par défaut : 7 jours (configurable par topic)
    Un consommateur peut rejouer depuis n'importe quel offset
    → Rejouer depuis l'offset 0 = retraiter tout l'historique

  DELIVERY SEMANTICS :
    At-most-once   : messages perdus possibles, jamais de doublons
    At-least-once  : pas de pertes, doublons possibles (défaut Kafka)
    Exactly-once   : ni perte ni doublon (transactions Kafka + idempotence)

Broker : serveur Kafka qui stocke les partitions
ZooKeeper (ou KRaft depuis Kafka 3) : coordination du cluster
Leader : broker principal pour une partition
Follower : brokers qui répliquent le leader
ISR (In-Sync Replicas) : followers à jour → peuvent devenir leader
"""

from __future__ import annotations
import threading
import time
import random
import hashlib
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Any, Optional, Iterator
from enum import Enum


# ─── MESSAGE ─────────────────────────────────────────────────────────────────

@dataclass
class Message:
    topic:      str
    partition:  int
    offset:     int
    cle:        Optional[str]
    valeur:     Any
    timestamp:  float = field(default_factory=time.time)
    headers:    dict  = field(default_factory=dict)

    def __repr__(self):
        return (f"Msg(topic={self.topic}, part={self.partition}, "
                f"offset={self.offset}, cle={self.cle!r}, val={self.valeur!r})")


# ─── PARTITION ───────────────────────────────────────────────────────────────

class Partition:
    """
    Log append-only pour une partition d'un topic.
    Les messages sont immuables — on ne peut qu'ajouter à la fin.
    La retention supprime les anciens messages.
    """

    def __init__(self, topic: str, partition_id: int,
                 retention_s: float = float("inf")):
        self.topic        = topic
        self.partition_id = partition_id
        self.retention_s  = retention_s
        self._log: list[Message] = []
        self._lock        = threading.Lock()
        # High Watermark : offset du dernier message répliqué sur tous les ISR
        self.hw:          int = 0

    def produire(self, cle: Optional[str], valeur: Any,
                 headers: dict = None) -> Message:
        with self._lock:
            offset = len(self._log)
            msg    = Message(
                topic     = self.topic,
                partition = self.partition_id,
                offset    = offset,
                cle       = cle,
                valeur    = valeur,
                headers   = headers or {},
            )
            self._log.append(msg)
            self.hw = offset + 1
            return msg

    def lire(self, depuis_offset: int, max_messages: int = 100) -> list[Message]:
        """Lire des messages depuis un offset donné."""
        with self._lock:
            debut = max(0, depuis_offset)
            fin   = min(debut + max_messages, len(self._log))
            return list(self._log[debut:fin])

    def log_end_offset(self) -> int:
        with self._lock:
            return len(self._log)

    def purger_anciens(self):
        """Appliquer la politique de rétention."""
        if self.retention_s == float("inf"):
            return
        limite = time.time() - self.retention_s
        with self._lock:
            self._log = [m for m in self._log if m.timestamp >= limite]

    def taille(self) -> int:
        with self._lock:
            return len(self._log)


# ─── TOPIC ───────────────────────────────────────────────────────────────────

class Topic:
    """
    Un topic Kafka = N partitions.
    Chaque message est routé vers une partition selon sa clef.
    """

    def __init__(self, nom: str, nb_partitions: int,
                 retention_s: float = float("inf"),
                 facteur_replication: int = 3):
        self.nom                  = nom
        self.nb_partitions        = nb_partitions
        self.facteur_replication  = facteur_replication
        self.partitions           = [
            Partition(nom, i, retention_s)
            for i in range(nb_partitions)
        ]
        self._compteur_round_robin = 0
        self._lock = threading.Lock()

    def _choisir_partition(self, cle: Optional[str]) -> int:
        """
        Routing des messages vers les partitions :
          - Avec clef  : hash(clef) % nb_partitions → ordre garanti par clef
          - Sans clef  : round-robin → équilibrage de charge
        """
        if cle is not None:
            h = int(hashlib.md5(cle.encode()).hexdigest(), 16)
            return h % self.nb_partitions
        else:
            with self._lock:
                p = self._compteur_round_robin % self.nb_partitions
                self._compteur_round_robin += 1
                return p

    def produire(self, cle: Optional[str], valeur: Any,
                 headers: dict = None) -> Message:
        p = self._choisir_partition(cle)
        return self.partitions[p].produire(cle, valeur, headers)

    def nb_messages_total(self) -> int:
        return sum(p.taille() for p in self.partitions)

    def stats(self) -> dict:
        return {
            "topic":         self.nom,
            "partitions":    self.nb_partitions,
            "messages":      self.nb_messages_total(),
            "par_partition": [p.taille() for p in self.partitions],
        }


# ─── CONSUMER GROUP ──────────────────────────────────────────────────────────

class ConsumerGroup:
    """
    Groupe de consommateurs qui se partagent les partitions d'un topic.

    Rebalancing :
      Quand un consommateur rejoint ou quitte le groupe, les partitions
      sont redistribuées (rebalancing).
      Pendant le rebalancing → pause de la consommation.
    """

    def __init__(self, group_id: str, topic: Topic):
        self.group_id  = group_id
        self.topic     = topic
        # offsets committés : partition_id → offset
        self._offsets: dict[int, int] = {i: 0 for i in range(topic.nb_partitions)}
        # assignation : consumer_id → [partition_ids]
        self._assignation: dict[str, list[int]] = {}
        self._membres: set[str] = set()
        self._lock = threading.Lock()

    def rejoindre(self, consumer_id: str):
        """Un consommateur rejoint le groupe → rebalancing."""
        with self._lock:
            self._membres.add(consumer_id)
            self._rebalancer()

    def quitter(self, consumer_id: str):
        """Un consommateur quitte le groupe → rebalancing."""
        with self._lock:
            self._membres.discard(consumer_id)
            self._assignation.pop(consumer_id, None)
            if self._membres:
                self._rebalancer()

    def _rebalancer(self):
        """
        Range assignation : distribuer les partitions aux consommateurs.
        Stratégie simple : round-robin des partitions sur les membres triés.
        """
        membres = sorted(self._membres)
        if not membres:
            return
        self._assignation = {m: [] for m in membres}
        for i, partition_id in enumerate(range(self.topic.nb_partitions)):
            membre = membres[i % len(membres)]
            self._assignation[membre].append(partition_id)

    def partitions_de(self, consumer_id: str) -> list[int]:
        with self._lock:
            return list(self._assignation.get(consumer_id, []))

    def consommer(self, consumer_id: str,
                  max_messages: int = 10) -> list[Message]:
        """Consommer des messages depuis les partitions assignées."""
        partitions = self.partitions_de(consumer_id)
        messages   = []
        for pid in partitions:
            offset = self._offsets[pid]
            msgs   = self.topic.partitions[pid].lire(offset, max_messages)
            messages.extend(msgs)
        return messages

    def commiter_offsets(self, consumer_id: str, offsets: dict[int, int]):
        """Commiter les offsets traités (at-least-once)."""
        with self._lock:
            for pid, offset in offsets.items():
                self._offsets[pid] = max(self._offsets.get(pid, 0), offset)

    def lag(self) -> dict[int, int]:
        """
        Consumer lag = log_end_offset - committed_offset par partition.
        Métrique cruciale : lag > 0 → consommateur en retard.
        """
        result = {}
        for pid, partition in enumerate(self.topic.partitions):
            leo    = partition.log_end_offset()
            offset = self._offsets.get(pid, 0)
            result[pid] = leo - offset
        return result

    def lag_total(self) -> int:
        return sum(self.lag().values())


# ─── BROKER (KAFKA SIMPLIFIÉ) ─────────────────────────────────────────────────

class KafkaBroker:
    """
    Broker Kafka simplifié.
    Gère les topics, la production, et la consommation.
    """

    def __init__(self):
        self._topics: dict[str, Topic] = {}
        self._groups: dict[str, ConsumerGroup] = {}
        self._stats  = defaultdict(int)

    def creer_topic(self, nom: str, nb_partitions: int = 4,
                    retention_s: float = float("inf")) -> Topic:
        t = Topic(nom, nb_partitions, retention_s)
        self._topics[nom] = t
        print(f"  [Kafka] Topic '{nom}' créé "
              f"({nb_partitions} partitions, "
              f"rétention={'∞' if retention_s == float('inf') else f'{retention_s}s'})")
        return t

    def produire(self, topic: str, cle: Optional[str],
                 valeur: Any, headers: dict = None) -> Message:
        if topic not in self._topics:
            raise KeyError(f"Topic '{topic}' inexistant")
        msg = self._topics[topic].produire(cle, valeur, headers)
        self._stats["produits"] += 1
        return msg

    def creer_consumer_group(self, group_id: str, topic: str) -> ConsumerGroup:
        t = self._topics[topic]
        g = ConsumerGroup(group_id, t)
        self._groups[group_id] = g
        return g

    def stats_topic(self, topic: str) -> dict:
        return self._topics[topic].stats()
