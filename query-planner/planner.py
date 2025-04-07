"""
Query Planner & Execution Engine
==================================
Problème : pour exécuter SELECT * FROM users WHERE age=30 AND country='FR',
la base de données a plusieurs stratégies possibles.
Elle doit CHOISIR la moins coûteuse AVANT d'exécuter.

  Stratégie 1 : Seq Scan        — lire toutes les lignes (O(N))
  Stratégie 2 : Index Scan      — descendre dans le B-Tree (O(log N + K))
  Stratégie 3 : Bitmap Scan     — construire un bitmap de RIDs, puis fetch
  Stratégie 4 : Hash Join       — jointure par table de hachage
  Stratégie 5 : Nested Loop     — jointure par boucle imbriquée
  Stratégie 6 : Merge Join      — jointure sur données triées

Le Query Planner :
  1. Parse la requête → arbre AST
  2. Génère tous les plans possibles
  3. Estime le coût de chaque plan (statistiques colonnes)
  4. Choisit le plan de coût minimum
  5. L'Executor exécute le plan choisi

Statistiques utilisées :
  n_distinct    : nombre de valeurs distinctes (~cardinalité)
  n_rows        : nombre total de lignes dans la table
  correlation   : corrélation entre l'ordre physique et l'ordre de la colonne
  histogram     : distribution des valeurs (pour estimer la sélectivité)

Coûts (unités PostgreSQL) :
  seq_page_cost    = 1.0  (lire une page séquentiellement)
  random_page_cost = 4.0  (lire une page en random I/O)
  cpu_tuple_cost   = 0.01 (évaluer un prédicat sur un tuple)
  cpu_index_tuple  = 0.005
"""

from __future__ import annotations
import math
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from enum import Enum


# ─── STATISTIQUES DE COLONNE ──────────────────────────────────────────────────

@dataclass
class StatColonne:
    """
    Statistiques collectées par ANALYZE sur une colonne.
    Equivalnt de pg_stats dans PostgreSQL.
    """
    nom:          str
    n_distinct:   int        # Nombre de valeurs distinctes (-N si estimé)
    null_frac:    float      # Fraction de NULLs (0.0 → 1.0)
    avg_width:    int        # Taille moyenne en octets
    correlation:  float      # -1.0 → 1.0 (1.0 = parfaitement corrélé à l'ordre physique)
    histogram:    list[Any]  # Bornes des buckets de l'histogramme
    mcv:          list[tuple[Any, float]] = field(default_factory=list)
    # Most Common Values : [(valeur, fréquence)]

    def selectivite(self, op: str, valeur: Any) -> float:
        """
        Estimer la fraction de lignes qui satisfont col op valeur.
        C'est le cœur du planner — une mauvaise estimation → mauvais plan.
        """
        if op == "=":
            # Chercher dans les MCV d'abord
            for val, freq in self.mcv:
                if val == valeur:
                    return freq
            # Sinon : 1 / n_distinct
            if self.n_distinct > 0:
                mcv_total = sum(f for _, f in self.mcv)
                return (1.0 - mcv_total) / max(self.n_distinct - len(self.mcv), 1)
            return 0.01

        elif op in ("<", "<=", ">", ">="):
            # Utiliser l'histogramme pour estimer la fraction dans la plage
            if not self.histogram or len(self.histogram) < 2:
                return 0.33  # défaut conservateur
            mn, mx = self.histogram[0], self.histogram[-1]
            try:
                if op in ("<", "<="):
                    frac = (float(valeur) - float(mn)) / (float(mx) - float(mn) + 1e-9)
                else:
                    frac = (float(mx) - float(valeur)) / (float(mx) - float(mn) + 1e-9)
                return max(0.001, min(0.999, frac))
            except (TypeError, ValueError):
                return 0.33

        elif op == "BETWEEN":
            lo, hi = valeur
            sel_lo = self.selectivite(">=", lo)
            sel_hi = self.selectivite("<=", hi)
            return sel_lo * sel_hi

        return 0.1  # défaut


@dataclass
class StatTable:
    """Statistiques d'une table (équivalent pg_class + pg_stats)."""
    nom:        str
    n_rows:     int
    n_pages:    int          # Nombre de pages de 8KB
    colonnes:   dict[str, StatColonne] = field(default_factory=dict)

    def selectivite_predicat(self, predicat: "Predicat") -> float:
        col = self.colonnes.get(predicat.colonne)
        if col is None:
            return 0.1
        return col.selectivite(predicat.op, predicat.valeur)

    def lignes_estimees(self, predicats: list["Predicat"]) -> int:
        """Estimation du nombre de lignes après application des prédicats."""
        sel = 1.0
        for p in predicats:
            sel *= self.selectivite_predicat(p)
        return max(1, int(self.n_rows * sel))


# ─── PRÉDICATS ET REQUÊTE ─────────────────────────────────────────────────────

@dataclass
class Predicat:
    colonne: str
    op:      str   # =, <, >, <=, >=, BETWEEN
    valeur:  Any

    def __str__(self):
        return f"{self.colonne} {self.op} {self.valeur!r}"

    def evaluer(self, ligne: dict) -> bool:
        v = ligne.get(self.colonne)
        if v is None:
            return False
        if self.op == "=":    return v == self.valeur
        if self.op == "<":    return v < self.valeur
        if self.op == ">":    return v > self.valeur
        if self.op == "<=":   return v <= self.valeur
        if self.op == ">=":   return v >= self.valeur
        if self.op == "BETWEEN":
            lo, hi = self.valeur
            return lo <= v <= hi
        return False


@dataclass
class Index:
    nom:      str
    colonnes: list[str]    # Colonnes indexées (ordre = leftmost prefix rule)
    type_:    str          # btree, hash, brin
    n_pages:  int          # Taille de l'index en pages
    # Données simulées pour l'exécution
    _donnees: dict = field(default_factory=dict, repr=False)

    def peut_satisfaire(self, predicats: list[Predicat]) -> bool:
        """Vérifie si cet index peut accélérer ces prédicats (leftmost prefix)."""
        if not predicats:
            return False
        cols_predicat = {p.colonne for p in predicats}
        # Le premier niveau de l'index doit être dans les prédicats
        return self.colonnes[0] in cols_predicat

    def construire(self, table: list[dict]):
        """Remplir l'index depuis les données (simulation)."""
        self._donnees = {}
        for i, ligne in enumerate(table):
            # Clef composite (colonnes indexées)
            cle = tuple(ligne.get(c) for c in self.colonnes)
            self._donnees.setdefault(cle, []).append(i)

    def chercher(self, predicats: list[Predicat]) -> Optional[list[int]]:
        """Retourner les RIDs (indices) qui satisfont les prédicats."""
        if not self._donnees:
            return None
        rids = []
        for cle, idxs in self._donnees.items():
            # Vérifier que la clef satisfait les prédicats de l'index
            match = True
            for p in predicats:
                if p.colonne in self.colonnes:
                    pos = self.colonnes.index(p.colonne)
                    if pos < len(cle):
                        val = cle[pos]
                        if p.op == "=" and val != p.valeur:
                            match = False; break
                        elif p.op == "<" and not (val < p.valeur):
                            match = False; break
                        elif p.op == ">" and not (val > p.valeur):
                            match = False; break
                        elif p.op == "<=" and not (val <= p.valeur):
                            match = False; break
                        elif p.op == ">=" and not (val >= p.valeur):
                            match = False; break
                        elif p.op == "BETWEEN":
                            lo, hi = p.valeur
                            if not (lo <= val <= hi):
                                match = False; break
            if match:
                rids.extend(idxs)
        return rids


# ─── NŒUDS DE PLAN ────────────────────────────────────────────────────────────

class TypeNoeud(Enum):
    SEQ_SCAN     = "Seq Scan"
    INDEX_SCAN   = "Index Scan"
    BITMAP_SCAN  = "Bitmap Index Scan"
    HASH_JOIN    = "Hash Join"
    NESTED_LOOP  = "Nested Loop"
    MERGE_JOIN   = "Merge Join"
    SORT         = "Sort"
    LIMIT        = "Limit"


@dataclass
class NoeudPlan:
    type:         TypeNoeud
    table:        str
    predicats:    list[Predicat] = field(default_factory=list)
    index_nom:    Optional[str] = None
    cout_total:   float = 0.0
    cout_startup: float = 0.0
    lignes_est:   int   = 0
    enfants:      list["NoeudPlan"] = field(default_factory=list)

    def afficher(self, indent: int = 0):
        prefix = "  " * indent
        idx = f" on {self.index_nom}" if self.index_nom else ""
        preds = " WHERE " + " AND ".join(str(p) for p in self.predicats) if self.predicats else ""
        print(f"{prefix}→ {self.type.value}{idx} on {self.table}{preds}")
        print(f"{prefix}  coût={self.cout_startup:.1f}..{self.cout_total:.1f}  "
              f"lignes≈{self.lignes_est}")
        for e in self.enfants:
            e.afficher(indent + 1)


# ─── QUERY PLANNER ────────────────────────────────────────────────────────────

class QueryPlanner:
    """
    Planner simplifié basé sur les coûts.
    Génère plusieurs plans candidats et choisit le moins cher.

    Paramètres de coût (unités PostgreSQL) :
      seq_page_cost    = 1.0
      random_page_cost = 4.0
      cpu_tuple_cost   = 0.01
      cpu_index_tuple  = 0.005
    """

    SEQ_PAGE_COST    = 1.0
    RANDOM_PAGE_COST = 4.0
    CPU_TUPLE_COST   = 0.01
    CPU_INDEX_TUPLE  = 0.005

    def __init__(self):
        self._tables: dict[str, StatTable] = {}
        self._index:  dict[str, list[Index]] = {}  # table → index

    def enregistrer_table(self, stat: StatTable):
        self._tables[stat.nom] = stat

    def enregistrer_index(self, table: str, index: Index):
        self._index.setdefault(table, []).append(index)

    # ── Planification ─────────────────────────────────────────────────────────

    def planifier(self, table: str, predicats: list[Predicat],
                  limit: int = None) -> NoeudPlan:
        """
        Générer et comparer tous les plans candidats.
        Retourner le plan de coût minimum.
        """
        stat = self._tables.get(table)
        if not stat:
            raise ValueError(f"Table inconnue : {table}")

        candidats = []

        # Plan 1 : Seq Scan (toujours disponible)
        candidats.append(self._plan_seq_scan(table, stat, predicats))

        # Plans 2+ : Index Scans disponibles
        for idx in self._index.get(table, []):
            if idx.peut_satisfaire(predicats):
                if stat.lignes_estimees(predicats) < stat.n_rows * 0.1:
                    candidats.append(self._plan_index_scan(table, stat, predicats, idx))
                candidats.append(self._plan_bitmap_scan(table, stat, predicats, idx))

        # Choisir le moins cher
        meilleur = min(candidats, key=lambda p: p.cout_total)

        if limit:
            meilleur.lignes_est = min(meilleur.lignes_est, limit)

        return meilleur

    def _plan_seq_scan(self, table: str, stat: StatTable,
                       predicats: list[Predicat]) -> NoeudPlan:
        """
        Coût Seq Scan = seq_page_cost × n_pages + cpu_tuple_cost × n_rows
        Toutes les pages lues séquentiellement.
        """
        cout_io  = self.SEQ_PAGE_COST * stat.n_pages
        cout_cpu = self.CPU_TUPLE_COST * stat.n_rows
        lignes   = stat.lignes_estimees(predicats)
        return NoeudPlan(
            type         = TypeNoeud.SEQ_SCAN,
            table        = table,
            predicats    = predicats,
            cout_startup = 0.0,
            cout_total   = cout_io + cout_cpu,
            lignes_est   = lignes,
        )

    def _plan_index_scan(self, table: str, stat: StatTable,
                          predicats: list[Predicat], idx: Index) -> NoeudPlan:
        """
        Coût Index Scan :
          startup = log2(n_rows) × cpu_index_tuple  (descente dans l'arbre)
          total   = startup + lignes_matchant × random_page_cost
        Random I/O car les tuples sont épars dans la heap.
        Intéressant uniquement pour une petite sélectivité (<10-15%).
        """
        lignes      = stat.lignes_estimees(predicats)
        cout_index  = math.log2(max(stat.n_rows, 2)) * self.CPU_INDEX_TUPLE
        cout_heap   = lignes * self.RANDOM_PAGE_COST * (idx.n_pages / max(stat.n_pages, 1))
        cout_total  = cout_index + cout_heap
        return NoeudPlan(
            type         = TypeNoeud.INDEX_SCAN,
            table        = table,
            predicats    = predicats,
            index_nom    = idx.nom,
            cout_startup = cout_index,
            cout_total   = cout_total,
            lignes_est   = lignes,
        )

    def _plan_bitmap_scan(self, table: str, stat: StatTable,
                           predicats: list[Predicat], idx: Index) -> NoeudPlan:
        """
        Coût Bitmap Scan :
          Phase 1 : parcourir l'index → bitmap de RIDs (séquentiel)
          Phase 2 : récupérer les pages heap selon le bitmap (moins de random I/O)
        Bon pour la sélectivité moyenne (5-30%).
        """
        lignes       = stat.lignes_estimees(predicats)
        sel          = lignes / max(stat.n_rows, 1)
        pages_heap   = max(1, int(stat.n_pages * sel))
        cout_index   = idx.n_pages * self.SEQ_PAGE_COST  # Parcours séquentiel de l'index
        cout_heap    = pages_heap * self.RANDOM_PAGE_COST * 0.5  # Bitmap réduit le random
        return NoeudPlan(
            type         = TypeNoeud.BITMAP_SCAN,
            table        = table,
            predicats    = predicats,
            index_nom    = idx.nom,
            cout_startup = cout_index,
            cout_total   = cout_index + cout_heap,
            lignes_est   = lignes,
        )


# ─── EXECUTOR ─────────────────────────────────────────────────────────────────

class Executor:
    """
    Exécute le plan choisi par le planner sur les vraies données.
    Pipeline d'itérateurs (Volcano model) :
      chaque nœud implémente next() → produit un tuple à la fois.
    """

    def __init__(self, tables: dict[str, list[dict]],
                 indexes: dict[str, list[Index]]):
        self._tables  = tables
        self._indexes = indexes

    def executer(self, plan: NoeudPlan) -> tuple[list[dict], dict]:
        """Retourne (résultats, stats_execution)."""
        stats = {"tuples_lus": 0, "tuples_retournes": 0,
                 "pages_lues": 0, "index_lookups": 0}
        t0 = time.perf_counter()

        if plan.type == TypeNoeud.SEQ_SCAN:
            resultats = self._seq_scan(plan, stats)
        elif plan.type == TypeNoeud.INDEX_SCAN:
            resultats = self._index_scan(plan, stats)
        elif plan.type == TypeNoeud.BITMAP_SCAN:
            resultats = self._bitmap_scan(plan, stats)
        else:
            resultats = self._seq_scan(plan, stats)

        stats["temps_ms"] = (time.perf_counter() - t0) * 1000
        stats["tuples_retournes"] = len(resultats)
        return resultats, stats

    def _seq_scan(self, plan: NoeudPlan, stats: dict) -> list[dict]:
        table = self._tables.get(plan.table, [])
        stats["tuples_lus"] = len(table)
        return [l for l in table
                if all(p.evaluer(l) for p in plan.predicats)]

    def _index_scan(self, plan: NoeudPlan, stats: dict) -> list[dict]:
        idx = self._trouver_index(plan.table, plan.index_nom)
        if idx is None:
            return self._seq_scan(plan, stats)
        rids = idx.chercher(plan.predicats)
        stats["index_lookups"] = 1
        table = self._tables.get(plan.table, [])
        resultats = []
        for rid in (rids or []):
            if rid < len(table):
                stats["tuples_lus"] += 1
                l = table[rid]
                if all(p.evaluer(l) for p in plan.predicats):
                    resultats.append(l)
        return resultats

    def _bitmap_scan(self, plan: NoeudPlan, stats: dict) -> list[dict]:
        # Bitmap : construire d'abord l'ensemble des RIDs, puis fetch groupé
        idx = self._trouver_index(plan.table, plan.index_nom)
        if idx is None:
            return self._seq_scan(plan, stats)
        rids = set(idx.chercher(plan.predicats) or [])
        stats["index_lookups"] = 1
        table = self._tables.get(plan.table, [])
        resultats = []
        for rid in sorted(rids):   # Trié = accès plus séquentiel
            if rid < len(table):
                stats["tuples_lus"] += 1
                l = table[rid]
                if all(p.evaluer(l) for p in plan.predicats):
                    resultats.append(l)
        return resultats

    def _trouver_index(self, table: str, nom: str) -> Optional[Index]:
        for idx in self._indexes.get(table, []):
            if idx.nom == nom:
                return idx
        return None
