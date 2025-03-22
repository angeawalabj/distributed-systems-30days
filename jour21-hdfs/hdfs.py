"""
Jour 21 — "Le Détective de Blocs" — HDFS
==========================================
Hadoop Distributed File System

Problème : stocker un fichier de 1 TB sur une seule machine = risque.
  → Disque tombe en panne → données perdues
  → Pas de parallélisme pour lire/écrire
  → Scalabilité limitée à une machine

HDFS résout ça avec 3 idées fondamentales :

  1. DÉCOUPAGE EN BLOCS (128 MB par défaut)
     Fichier 1 TB → 8192 blocs de 128 MB répartis sur le cluster.

  2. RÉPLICATION (factor=3 par défaut)
     Chaque bloc est copié sur 3 DataNodes différents.
     Tolère 2 pannes simultanées.

  3. RACK AWARENESS
     Réplique 1 : DataNode local
     Réplique 2 : DataNode dans un rack DIFFÉRENT
     Réplique 3 : DataNode dans le même rack que réplique 2
     → Survie à une panne de rack entier (switch HS)

Architecture :
  NameNode (1) : namespace + mapping fichier→blocs→DataNodes (métadonnées seulement)
  DataNode (N) : stocke les blocs, envoie heartbeats toutes les 3s
  Client       : interroge NameNode puis lit/écrit directement les DataNodes
"""

from __future__ import annotations
import random
import time
import math
from dataclasses import dataclass, field
from typing import Optional
from collections import defaultdict
from enum import Enum


class EtatNode(Enum):
    ALIVE = "ALIVE"
    DEAD  = "DEAD"


@dataclass
class DataNode:
    node_id:     str
    rack_id:     str
    capacite_gb: float
    utilise_gb:  float    = 0.0
    etat:        EtatNode = EtatNode.ALIVE
    dernier_hb:  float    = field(default_factory=time.time)
    blocs:       set      = field(default_factory=set)

    @property
    def libre_gb(self) -> float:
        return self.capacite_gb - self.utilise_gb

    def heartbeat(self):
        self.dernier_hb = time.time()
        self.etat = EtatNode.ALIVE

    def recevoir_bloc(self, bloc_id: str, taille_mb: float):
        self.blocs.add(bloc_id)
        self.utilise_gb += taille_mb / 1024

    def supprimer_bloc(self, bloc_id: str, taille_mb: float):
        self.blocs.discard(bloc_id)
        self.utilise_gb = max(0.0, self.utilise_gb - taille_mb / 1024)


@dataclass
class Bloc:
    bloc_id:      str
    fichier_path: str
    index:        int
    taille_mb:    float
    repliques:    list[str] = field(default_factory=list)  # node_ids

    @property
    def nb_repliques(self) -> int:
        return len(self.repliques)

    def est_sous_replique(self, facteur: int = 3) -> bool:
        return self.nb_repliques < facteur


class NameNode:
    """
    Cerveau de HDFS.
    Ne stocke que les métadonnées, jamais les données.
    En production tout tient en RAM (~200 octets par bloc).
    """

    TAILLE_BLOC_MB   = 128
    FACTEUR_REPLIQUE = 3
    HB_TIMEOUT_S     = 10

    def __init__(self):
        self._datanodes: dict[str, DataNode] = {}
        self._blocs:     dict[str, Bloc]     = {}
        self._namespace: dict[str, list[str]] = {}
        self._seq        = 0
        self._stats      = defaultdict(int)

    def enregistrer_datanode(self, dn: DataNode):
        self._datanodes[dn.node_id] = dn
        print(f"  [NameNode] DataNode {dn.node_id} enregistré "
              f"(rack={dn.rack_id}, {dn.capacite_gb}GB)")

    def recevoir_heartbeat(self, node_id: str):
        if node_id in self._datanodes:
            self._datanodes[node_id].heartbeat()

    def datanodes_vivants(self) -> list[DataNode]:
        return [dn for dn in self._datanodes.values()
                if dn.etat == EtatNode.ALIVE]

    # ── Placement rack-aware ──────────────────────────────────────────────

    def _choisir_datanodes(self, nb: int, exclure: list[str] = None) -> list[DataNode]:
        """
        Rack-aware placement :
          R1 → n'importe quel DN vivant (trié par espace libre)
          R2 → rack différent de R1
          R3 → même rack que R2 (économie bande passante inter-rack)
        """
        exclus  = set(exclure or [])
        vivants = [dn for dn in self.datanodes_vivants()
                   if dn.node_id not in exclus
                   and dn.libre_gb > self.TAILLE_BLOC_MB / 1024]
        if len(vivants) < nb:
            return vivants

        vivants.sort(key=lambda d: -d.libre_gb)
        choisis = [vivants[0]]

        # R2 : rack différent
        autres = [d for d in vivants[1:] if d.rack_id != choisis[0].rack_id]
        r2 = (autres[0] if autres else vivants[1]) if len(vivants) > 1 else choisis[0]
        choisis.append(r2)

        # R3+ : même rack que R2 ou autre
        while len(choisis) < nb:
            restants = [d for d in vivants if d not in choisis]
            if not restants:
                break
            meme_rack = [d for d in restants if d.rack_id == r2.rack_id]
            choisis.append(meme_rack[0] if meme_rack else restants[0])

        return choisis[:nb]

    # ── Écriture ─────────────────────────────────────────────────────────

    def creer_fichier(self, path: str, taille_mb: float) -> list[Bloc]:
        nb_blocs = max(1, math.ceil(taille_mb / self.TAILLE_BLOC_MB))
        blocs    = []
        for i in range(nb_blocs):
            taille_b = min(self.TAILLE_BLOC_MB, taille_mb - i * self.TAILLE_BLOC_MB)
            self._seq += 1
            bid = f"blk_{self._seq:06d}"
            dns = self._choisir_datanodes(self.FACTEUR_REPLIQUE)
            for dn in dns:
                dn.recevoir_bloc(bid, taille_b)
            b = Bloc(bloc_id=bid, fichier_path=path, index=i,
                     taille_mb=taille_b, repliques=[d.node_id for d in dns])
            self._blocs[bid]   = b
            blocs.append(b)
            self._stats["blocs"] += 1
        self._namespace[path] = [b.bloc_id for b in blocs]
        self._stats["fichiers"] += 1
        return blocs

    # ── Lecture ──────────────────────────────────────────────────────────

    def lire_fichier(self, path: str) -> list[tuple[str, list[str]]]:
        if path not in self._namespace:
            raise FileNotFoundError(f"{path} introuvable")
        return [(bid, self._blocs[bid].repliques)
                for bid in self._namespace[path]]

    # ── Détection et correction ───────────────────────────────────────────

    def blocs_sous_repliques(self) -> list[Bloc]:
        return [b for b in self._blocs.values()
                if b.est_sous_replique(self.FACTEUR_REPLIQUE)]

    def re_repliquer(self, bloc: Bloc) -> int:
        manquantes  = self.FACTEUR_REPLIQUE - bloc.nb_repliques
        if manquantes <= 0:
            return 0
        nouveaux    = self._choisir_datanodes(manquantes, exclure=bloc.repliques)
        for dn in nouveaux:
            dn.recevoir_bloc(bloc.bloc_id, bloc.taille_mb)
            bloc.repliques.append(dn.node_id)
            self._stats["replications"] += 1
        return len(nouveaux)

    def traiter_panne(self, node_id: str) -> dict:
        if node_id not in self._datanodes:
            return {}
        self._datanodes[node_id].etat = EtatNode.DEAD
        blocs_touches = [b for b in self._blocs.values()
                         if node_id in b.repliques]
        for b in blocs_touches:
            b.repliques.remove(node_id)
        re_rep = sum(self.re_repliquer(b) for b in blocs_touches)
        return {
            "blocs_affectes":        len(blocs_touches),
            "re_repliques":          re_rep,
            "encore_sous_repliques": len(self.blocs_sous_repliques()),
        }

    def rapport(self) -> dict:
        vivants    = len(self.datanodes_vivants())
        total_gb   = sum(dn.capacite_gb for dn in self._datanodes.values())
        utilise_gb = sum(dn.utilise_gb  for dn in self._datanodes.values())
        return {
            "datanodes_total":   len(self._datanodes),
            "datanodes_vivants": vivants,
            "fichiers":          self._stats["fichiers"],
            "blocs":             self._stats["blocs"],
            "replications_auto": self._stats["replications"],
            "capacite_gb":       round(total_gb, 1),
            "utilise_gb":        round(utilise_gb, 1),
            "remplissage_pct":   round(utilise_gb / max(total_gb, 1) * 100, 1),
        }
