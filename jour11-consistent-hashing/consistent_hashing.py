"""
Jour 11 — Consistent Hashing
==============================
Problème : répartir K clés sur N nœuds de façon à ce qu'ajouter
ou retirer un nœud perturbe le moins de clés possible.

Approche naïve : noeud = hash(clé) % N
  → Si N change de 10 à 11 : presque TOUTES les clés changent de nœud
  → Catastrophique : vide les caches, surcharge la DB, incident de prod

Consistent Hashing (Karger et al., 1997) :
  1. Placer les nœuds ET les clés sur un cercle [0, 2^32)
  2. Une clé appartient au premier nœud rencontré
     en tournant dans le sens horaire
  3. Ajouter un nœud : seule la portion entre son prédécesseur
     et lui est réaffectée → K/N clés en moyenne
  4. Retirer un nœud : ses clés passent à son successeur → K/N clés

  Propriété fondamentale :
    Modulo naïf    : ajouter 1 nœud réaffecte ~(N-1)/N des clés ≈ 90%
    Consistent     : ajouter 1 nœud réaffecte ~1/N des clés          ≈ 10%

  Nœuds virtuels (vnodes) :
    Problème sans vnodes : distribution inégale (certains nœuds
    reçoivent 5x plus de clés que d'autres par malchance).
    Solution : chaque nœud physique a V positions sur le cercle.
    → Distribution statistiquement uniforme avec V ≥ 100.
    → Cassandra utilise 256 vnodes par défaut.

Utilisé par : DynamoDB, Cassandra, Memcached (ketama),
              Nginx upstream, Varnish, Akamai CDN
"""

import hashlib
import bisect
import random
from dataclasses import dataclass, field
from typing import Optional


# ─── UTILITAIRE DE HASH ───────────────────────────────────────────────────────

def hash_md5(valeur: str) -> int:
    """Hash MD5 → entier sur [0, 2^128). On prend les 32 bits bas pour l'anneau."""
    return int(hashlib.md5(valeur.encode()).hexdigest(), 16) % (2**32)

def hash_sha1(valeur: str) -> int:
    return int(hashlib.sha1(valeur.encode()).hexdigest(), 16) % (2**32)

TAILLE_ANNEAU = 2**32   # [0, 4 294 967 295]


# ─── ANNEAU CONSISTENT HASHING (sans vnodes) ─────────────────────────────────

class AnneauSimple:
    """
    Consistent hashing basique sans nœuds virtuels.

    Illustre le problème de distribution inégale
    que les vnodes résolvent.
    """

    def __init__(self, fn_hash=hash_md5):
        self._fn_hash = fn_hash
        # Liste triée des positions sur l'anneau
        self._positions: list[int] = []
        # position → nom du nœud
        self._noeuds: dict[int, str] = {}

    def ajouter_noeud(self, nom: str) -> int:
        pos = self._fn_hash(nom)
        self._positions.append(pos)
        self._positions.sort()
        self._noeuds[pos] = nom
        return pos

    def retirer_noeud(self, nom: str):
        pos = self._fn_hash(nom)
        if pos in self._noeuds:
            del self._noeuds[pos]
            self._positions.remove(pos)

    def noeud_pour(self, cle: str) -> Optional[str]:
        """Trouve le nœud responsable d'une clé."""
        if not self._positions:
            return None
        pos = self._fn_hash(cle)
        # Trouver le premier nœud ≥ pos (rotation si on dépasse la fin)
        idx = bisect.bisect_left(self._positions, pos)
        if idx == len(self._positions):
            idx = 0   # Wrap-around : revenir au début de l'anneau
        return self._noeuds[self._positions[idx]]

    def distribution(self, cles: list[str]) -> dict[str, int]:
        """Compte combien de clés chaque nœud possède."""
        compteur: dict[str, int] = {n: 0 for n in self._noeuds.values()}
        for cle in cles:
            n = self.noeud_pour(cle)
            if n:
                compteur[n] = compteur.get(n, 0) + 1
        return compteur


# ─── ANNEAU AVEC NŒUDS VIRTUELS (vnodes) ─────────────────────────────────────

class AnneauVnodes:
    """
    Consistent hashing avec nœuds virtuels.

    Chaque nœud physique occupe V positions sur l'anneau.
    → Distribution uniforme, résilience aux inégalités.
    → Cassandra : 256 vnodes/nœud par défaut.
    """

    def __init__(self, vnodes: int = 150, fn_hash=hash_md5):
        self._vnodes   = vnodes
        self._fn_hash  = fn_hash
        self._positions: list[int] = []              # Triées
        self._anneau: dict[int, str] = {}            # position → nœud physique
        self._noeud_positions: dict[str, list[int]] = {}  # nœud → ses positions
        self._noeuds_physiques: set[str] = set()

    def ajouter_noeud(self, nom: str, poids: float = 1.0):
        """
        Ajoute un nœud avec un nombre de vnodes proportionnel au poids.
        poids=2.0 → deux fois plus de vnodes → deux fois plus de clés.
        Utile pour des nœuds avec des capacités différentes.
        """
        nb_vnodes = max(1, int(self._vnodes * poids))
        positions = []
        for i in range(nb_vnodes):
            # Chaque vnode a sa propre position
            vnode_key = f"{nom}#vnode{i}"
            pos = self._fn_hash(vnode_key)
            self._anneau[pos] = nom
            positions.append(pos)

        positions.sort()
        self._noeud_positions[nom] = positions
        self._noeuds_physiques.add(nom)

        # Mettre à jour la liste triée globale
        for pos in positions:
            bisect.insort(self._positions, pos)

    def retirer_noeud(self, nom: str):
        """Retire un nœud et toutes ses positions virtuelles."""
        if nom not in self._noeud_positions:
            return
        for pos in self._noeud_positions[nom]:
            del self._anneau[pos]
            self._positions.remove(pos)
        del self._noeud_positions[nom]
        self._noeuds_physiques.discard(nom)

    def noeud_pour(self, cle: str) -> Optional[str]:
        if not self._positions:
            return None
        pos = self._fn_hash(cle)
        idx = bisect.bisect_left(self._positions, pos)
        if idx == len(self._positions):
            idx = 0
        return self._anneau[self._positions[idx]]

    def N_noeuds_pour(self, cle: str, N: int) -> list[str]:
        """
        Retourne les N premiers nœuds distincts responsables d'une clé.
        Utilisé pour la réplication : N=3 → stocker sur 3 nœuds.
        """
        if not self._positions:
            return []
        pos    = self._fn_hash(cle)
        idx    = bisect.bisect_left(self._positions, pos)
        result = []
        seen   = set()
        for i in range(len(self._positions)):
            noeud = self._anneau[self._positions[(idx + i) % len(self._positions)]]
            if noeud not in seen:
                seen.add(noeud)
                result.append(noeud)
            if len(result) == N:
                break
        return result

    def distribution(self, cles: list[str]) -> dict[str, int]:
        compteur: dict[str, int] = {n: 0 for n in self._noeuds_physiques}
        for cle in cles:
            n = self.noeud_pour(cle)
            if n:
                compteur[n] = compteur.get(n, 0) + 1
        return compteur

    def nb_noeuds(self) -> int:
        return len(self._noeuds_physiques)

    def noeuds(self) -> set[str]:
        return set(self._noeuds_physiques)


# ─── COMPARATEUR MODULO vs CONSISTENT ────────────────────────────────────────

class ComparateurHashing:
    """
    Compare le hashing modulo naïf vs consistent hashing
    lors d'ajouts/retraits de nœuds.
    Mesure le pourcentage de clés réaffectées.
    """

    @staticmethod
    def noeud_modulo(cle: str, noeuds: list[str]) -> str:
        """Hashing modulo : nœud = hash(clé) % len(noeuds)."""
        return noeuds[hash_md5(cle) % len(noeuds)]

    @staticmethod
    def clés_reaffectees_modulo(
        cles: list[str],
        noeuds_avant: list[str],
        noeuds_apres: list[str],
    ) -> tuple[int, float]:
        """Compte combien de clés changent de nœud avec modulo."""
        changes = 0
        for cle in cles:
            avant = ComparateurHashing.noeud_modulo(cle, noeuds_avant)
            apres = ComparateurHashing.noeud_modulo(cle, noeuds_apres)
            if avant != apres:
                changes += 1
        return changes, changes / len(cles) * 100

    @staticmethod
    def clés_reaffectees_consistent(
        cles: list[str],
        anneau_avant: AnneauVnodes,
        anneau_apres: AnneauVnodes,
    ) -> tuple[int, float]:
        """Compte combien de clés changent de nœud avec consistent hashing."""
        changes = 0
        for cle in cles:
            avant = anneau_avant.noeud_pour(cle)
            apres = anneau_apres.noeud_pour(cle)
            if avant != apres:
                changes += 1
        return changes, changes / len(cles) * 100
