"""
Jour 27 — Index : B-Tree, Hash, BRIN
======================================
Problème : SELECT * FROM users WHERE age=30 sur une table de 10M lignes.
Sans index → seq scan : lire TOUTES les lignes = O(N) = lent.
Avec index → O(log N) ou O(1) selon le type.

Un index = structure de données auxiliaire qui mappe valeurs → RIDs (Row IDs).

Quatre types principaux :

1. B-Tree (Balanced Tree) :
   Arbre équilibré, ordre = 1 (feuilles triées).
   Supporte : =, <, >, <=, >=, BETWEEN, ORDER BY, LIKE 'foo%'
   Complexité : O(log N) insertion, recherche, suppression
   Utilisé : 95% des index en production
   Implémentation : PostgreSQL, MySQL, SQLite, Oracle

2. Hash Index :
   Table de hachage : clef → bucket → RIDs
   Supporte : = uniquement
   Complexité : O(1) amortie
   Limitation : pas de range scan, pas d'ORDER BY
   Utilisé : jointures par équivalence uniquement

3. BRIN (Block Range INdex) :
   Stocke juste min/max par bloc de N pages.
   Très petit (quelques KB pour une table de Go).
   Supporte : range queries sur données naturellement triées (timestamps, IDs)
   Condition : données doivent être corrélées à l'ordre physique
   Utilisé : tables de logs, séries temporelles

4. GiST (Generalized Search Tree) :
   Arbre générique pour types complexes.
   Supporte : géométries, full-text, intervalles, arrays
   Utilisé : PostGIS, recherche plein texte

Write Overhead :
  Chaque INSERT/UPDATE/DELETE doit aussi mettre à jour les index.
  → Sur-indexer = ralentit les écritures.
  Règle : créer seulement les index utilisés par des requêtes fréquentes.

Index composites :
  CREATE INDEX ON users(country, age)
  Utilisé pour : WHERE country='FR' AND age=30
  Pas utilisé pour : WHERE age=30 (sans country en préfixe)
  Ordre des colonnes dans l'index = crucial
"""

from __future__ import annotations
import math
import time
import random
from dataclasses import dataclass, field
from typing import Any, Optional, Iterator


# ─── B-TREE ───────────────────────────────────────────────────────────────────

@dataclass
class NoeudBTree:
    """Nœud d'un B-Tree d'ordre T (chaque nœud a [T-1, 2T-1] clefs)."""
    cles:    list = field(default_factory=list)      # clefs triées
    valeurs: list = field(default_factory=list)      # payload par clef
    enfants: list = field(default_factory=list)      # enfants (nœuds)
    est_feuille: bool = True

    def est_plein(self, t: int) -> bool:
        return len(self.cles) >= 2 * t - 1


class BTree:
    """
    B-Tree d'ordre T.
    Chaque nœud interne a entre T-1 et 2T-1 clefs.
    Hauteur = O(log_T N) → recherche en O(T × log_T N) comparaisons.
    En pratique T=100-1000 → hauteur 2-3 pour des millions de clefs.
    """

    def __init__(self, t: int = 3):
        self.t    = t          # Ordre (min degrés)
        self.racine = NoeudBTree()
        self.nb_cles = 0
        self.nb_noeuds = 1
        self.hauteur   = 1
        self.comparaisons = 0  # Pour mesurer

    # ── Recherche ─────────────────────────────────────────────────────────────

    def chercher(self, cle) -> Optional[Any]:
        self.comparaisons = 0
        return self._chercher(self.racine, cle)

    def _chercher(self, noeud: NoeudBTree, cle) -> Optional[Any]:
        i = 0
        while i < len(noeud.cles):
            self.comparaisons += 1
            if cle == noeud.cles[i]:
                return noeud.valeurs[i]
            if cle < noeud.cles[i]:
                break
            i += 1
        if noeud.est_feuille:
            return None
        return self._chercher(noeud.enfants[i], cle)

    def range_scan(self, cle_min, cle_max) -> list[tuple]:
        """Retourner toutes les paires (cle, valeur) dans [cle_min, cle_max]."""
        resultats = []
        self._range_scan(self.racine, cle_min, cle_max, resultats)
        return resultats

    def _range_scan(self, noeud: NoeudBTree, cle_min, cle_max, res: list):
        i = 0
        while i < len(noeud.cles):
            if not noeud.est_feuille and noeud.cles[i] > cle_min:
                self._range_scan(noeud.enfants[i], cle_min, cle_max, res)
            if cle_min <= noeud.cles[i] <= cle_max:
                res.append((noeud.cles[i], noeud.valeurs[i]))
            elif noeud.cles[i] > cle_max:
                return
            i += 1
        if not noeud.est_feuille:
            self._range_scan(noeud.enfants[i], cle_min, cle_max, res)

    # ── Insertion ─────────────────────────────────────────────────────────────

    def inserer(self, cle, valeur=None):
        self.nb_cles += 1
        racine = self.racine
        if racine.est_plein(self.t):
            nouvelle_racine = NoeudBTree(est_feuille=False)
            nouvelle_racine.enfants.append(racine)
            self._scinder_enfant(nouvelle_racine, 0)
            self.racine = nouvelle_racine
            self.hauteur += 1
            self.nb_noeuds += 2
        self._inserer_non_plein(self.racine, cle, valeur)

    def _inserer_non_plein(self, noeud: NoeudBTree, cle, valeur):
        i = len(noeud.cles) - 1
        if noeud.est_feuille:
            noeud.cles.append(None)
            noeud.valeurs.append(None)
            while i >= 0 and cle < noeud.cles[i]:
                noeud.cles[i + 1]   = noeud.cles[i]
                noeud.valeurs[i + 1] = noeud.valeurs[i]
                i -= 1
            noeud.cles[i + 1]   = cle
            noeud.valeurs[i + 1] = valeur
        else:
            while i >= 0 and cle < noeud.cles[i]:
                i -= 1
            i += 1
            if noeud.enfants[i].est_plein(self.t):
                self._scinder_enfant(noeud, i)
                self.nb_noeuds += 1
                if cle > noeud.cles[i]:
                    i += 1
            self._inserer_non_plein(noeud.enfants[i], cle, valeur)

    def _scinder_enfant(self, parent: NoeudBTree, i: int):
        t    = self.t
        fils = parent.enfants[i]
        nouveau = NoeudBTree(est_feuille=fils.est_feuille)
        # Monter la clef médiane
        parent.cles.insert(i, fils.cles[t - 1])
        parent.valeurs.insert(i, fils.valeurs[t - 1])
        parent.enfants.insert(i + 1, nouveau)
        # Répartir les clefs
        nouveau.cles   = fils.cles[t:]
        nouveau.valeurs = fils.valeurs[t:]
        fils.cles    = fils.cles[:t - 1]
        fils.valeurs = fils.valeurs[:t - 1]
        if not fils.est_feuille:
            nouveau.enfants = fils.enfants[t:]
            fils.enfants    = fils.enfants[:t]

    def stats(self) -> dict:
        return {
            "nb_cles":    self.nb_cles,
            "nb_noeuds":  self.nb_noeuds,
            "hauteur":    self.hauteur,
            "t":          self.t,
        }


# ─── HASH INDEX ───────────────────────────────────────────────────────────────

class HashIndex:
    """
    Index hash : clef → liste de RIDs.
    O(1) pour les lookups exacts.
    Pas de range scan possible.
    Utilise le chaining (liste liée par bucket) pour gérer les collisions.
    """

    def __init__(self, nb_buckets: int = 1024):
        self.nb_buckets = nb_buckets
        self._buckets: list[list[tuple]] = [[] for _ in range(nb_buckets)]
        self.nb_entrees = 0
        self.comparaisons = 0

    def inserer(self, cle, rid):
        bucket = hash(cle) % self.nb_buckets
        self._buckets[bucket].append((cle, rid))
        self.nb_entrees += 1

    def chercher(self, cle) -> list:
        self.comparaisons = 0
        bucket = hash(cle) % self.nb_buckets
        resultats = []
        for k, rid in self._buckets[bucket]:
            self.comparaisons += 1
            if k == cle:
                resultats.append(rid)
        return resultats

    def facteur_charge(self) -> float:
        return self.nb_entrees / self.nb_buckets

    def collisions_max(self) -> int:
        return max(len(b) for b in self._buckets)

    def stats(self) -> dict:
        buckets_utilises = sum(1 for b in self._buckets if b)
        return {
            "nb_entrees":     self.nb_entrees,
            "nb_buckets":     self.nb_buckets,
            "facteur_charge": round(self.facteur_charge(), 2),
            "collisions_max": self.collisions_max(),
            "buckets_vides":  self.nb_buckets - buckets_utilises,
        }


# ─── BRIN INDEX ───────────────────────────────────────────────────────────────

@dataclass
class BlocBRIN:
    """Min/max d'un bloc de N lignes."""
    bloc_id:   int
    min_val:   Any
    max_val:   Any
    nb_lignes: int


class BRINIndex:
    """
    Block Range INdex.
    Stocke uniquement min/max par bloc de `pages_par_bloc` lignes.
    Très compact : 16 octets par bloc vs des centaines pour B-Tree.
    Efficace seulement si les données sont naturellement corrélées à l'ordre
    physique (timestamps d'insertion, IDs auto-incrément, dates...).
    """

    def __init__(self, lignes_par_bloc: int = 128):
        self.lignes_par_bloc = lignes_par_bloc
        self._blocs: list[BlocBRIN] = []
        self.nb_lignes = 0

    def construire(self, valeurs: list):
        """Construire le BRIN depuis une liste de valeurs dans l'ordre physique."""
        self._blocs = []
        self.nb_lignes = len(valeurs)
        for debut in range(0, len(valeurs), self.lignes_par_bloc):
            bloc_vals = valeurs[debut:debut + self.lignes_par_bloc]
            self._blocs.append(BlocBRIN(
                bloc_id   = debut // self.lignes_par_bloc,
                min_val   = min(bloc_vals),
                max_val   = max(bloc_vals),
                nb_lignes = len(bloc_vals),
            ))

    def blocs_candidats(self, val_min, val_max) -> list[BlocBRIN]:
        """Retourner les blocs dont la plage intersecte [val_min, val_max]."""
        return [b for b in self._blocs
                if b.min_val <= val_max and b.max_val >= val_min]

    def taille_octets(self) -> int:
        # Chaque entrée BRIN : 2 valeurs int (8 octets) + bloc_id (4) + nb (4) = ~24 octets
        return len(self._blocs) * 24

    def stats(self) -> dict:
        return {
            "nb_blocs":        len(self._blocs),
            "lignes_par_bloc": self.lignes_par_bloc,
            "taille_octets":   self.taille_octets(),
            "nb_lignes":       self.nb_lignes,
        }
