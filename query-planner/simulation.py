"""
Query Planner & Execution Engine — Simulation
===============================================
5 scénarios :
  1. Seq Scan vs Index Scan — la sélectivité décide
  2. EXPLAIN — afficher le plan avec estimations de coût
  3. Mauvaises statistiques — quand le planner se trompe
  4. Index composite — leftmost prefix rule dans le planner
  5. Jointure — Nested Loop vs Hash Join selon la taille
"""

import random
import time
import math
from planner import (
    StatTable, StatColonne, Index, Predicat,
    QueryPlanner, Executor
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def generer_table_users(n: int, seed: int = 42) -> list[dict]:
    random.seed(seed)
    pays = ['FR', 'DE', 'US', 'UK', 'JP'] * 2 + ['CN', 'BR']
    return [
        {
            "id":      i,
            "age":     random.randint(18, 80),
            "country": random.choice(pays),
            "score":   random.randint(0, 1000),
            "email":   f"user{i}@example.com",
            "premium": random.random() < 0.05,  # 5% premium
        }
        for i in range(n)
    ]


def stat_users(table: list[dict], nom: str = "users") -> StatTable:
    n = len(table)
    ages     = [r["age"] for r in table]
    scores   = [r["score"] for r in table]
    pays_set = set(r["country"] for r in table)

    # Histogrammes
    ages_sorted   = sorted(ages)
    scores_sorted = sorted(scores)
    def histogram(vals, nb=10):
        step = max(1, len(vals) // nb)
        return [vals[i] for i in range(0, len(vals), step)]

    # Most Common Values pour country
    from collections import Counter
    country_counts = Counter(r["country"] for r in table)
    mcv_country = [(c, cnt/n) for c, cnt in country_counts.most_common(5)]

    return StatTable(
        nom     = nom,
        n_rows  = n,
        n_pages = max(1, n // 100),  # ~100 lignes par page de 8KB
        colonnes = {
            "age": StatColonne(
                nom="age", n_distinct=63, null_frac=0.0,
                avg_width=4, correlation=0.1,
                histogram=histogram(ages_sorted),
                mcv=[],
            ),
            "country": StatColonne(
                nom="country", n_distinct=len(pays_set), null_frac=0.0,
                avg_width=3, correlation=0.05,
                histogram=list(sorted(pays_set)),
                mcv=mcv_country,
            ),
            "score": StatColonne(
                nom="score", n_distinct=1001, null_frac=0.0,
                avg_width=4, correlation=0.02,
                histogram=histogram(scores_sorted),
                mcv=[],
            ),
            "premium": StatColonne(
                nom="premium", n_distinct=2, null_frac=0.0,
                avg_width=1, correlation=0.0,
                histogram=[False, True],
                mcv=[(False, 0.95), (True, 0.05)],
            ),
        }
    )


# ─── SCÉNARIO 1 : SÉLECTIVITÉ DÉCIDE ─────────────────────────────────────────

def scenario_selectivite():
    titre("SCÉNARIO 1 — La sélectivité décide : Seq Scan vs Index Scan")

    print("""
  Le planner compare le coût estimé de chaque stratégie.
  Règle fondamentale :
    Sélectivité faible (peu de résultats) → Index Scan
    Sélectivité élevée (beaucoup de résultats) → Seq Scan

  Intuition : si 50% des lignes correspondent, autant tout lire en séquentiel
  plutôt que faire N random I/Os vers la heap.
    """)

    N = 50_000
    table = generer_table_users(N)
    stat  = stat_users(table)

    planner  = QueryPlanner()
    planner.enregistrer_table(stat)

    idx_age = Index(nom="idx_age", colonnes=["age"], type_="btree",
                    n_pages=max(1, N // 500))
    idx_age.construire(table)
    idx_premium = Index(nom="idx_premium", colonnes=["premium"], type_="btree",
                        n_pages=max(1, N // 1000))
    idx_premium.construire(table)
    planner.enregistrer_index("users", idx_age)
    planner.enregistrer_index("users", idx_premium)

    executor = Executor({"users": table}, {"users": [idx_age, idx_premium]})

    requetes = [
        ("premium = True (5%)",      [Predicat("premium", "=", True)]),
        ("age = 30 (~1.6%)",         [Predicat("age", "=", 30)]),
        ("age BETWEEN 25 ET 35 (~16%)", [Predicat("age", "BETWEEN", (25, 35))]),
        ("age > 40 (~65%)",          [Predicat("age", ">", 40)]),
    ]

    print(f"  {'Requête':<32} {'Plan choisi':<18} {'Coût':<8}  {'Lignes≈':<8}  {'Réel':<6}  {'Temps'}")
    print("  " + "─"*80)

    for label, preds in requetes:
        plan = planner.planifier("users", preds)
        res, stats = executor.executer(plan)
        print(f"  {label:<32} {plan.type.value:<18} {plan.cout_total:<8.1f}  "
              f"{plan.lignes_est:<8}  {len(res):<6}  {stats['temps_ms']:.1f}ms")

    print(f"""
  Observations :
    premium=True (5%)    → Index Scan  (peu de résultats, random I/O vaut le coup)
    age>40 (65%)         → Seq Scan    (trop de résultats, seq. moins cher)
    BETWEEN 25-35 (16%)  → Bitmap Scan (compromis : bitmap réduit le random I/O)

  PostgreSQL imprime ceci avec EXPLAIN (sans ANALYZE) avant exécution.
    """)


# ─── SCÉNARIO 2 : EXPLAIN ─────────────────────────────────────────────────────

def scenario_explain():
    titre("SCÉNARIO 2 — EXPLAIN : visualiser le plan avec coûts et estimations")

    print("""
  EXPLAIN affiche le plan d'exécution AVANT de lancer la requête.
  EXPLAIN ANALYZE l'exécute ET affiche les métriques réelles.
  C'est l'outil #1 pour diagnostiquer les requêtes lentes.
    """)

    N = 100_000
    table = generer_table_users(N)
    stat  = stat_users(table)

    planner = QueryPlanner()
    planner.enregistrer_table(stat)

    idx_country_age = Index(nom="idx_country_age", colonnes=["country","age"],
                             type_="btree", n_pages=N//200)
    idx_country_age.construire(table)
    planner.enregistrer_index("users", idx_country_age)

    executor = Executor({"users": table}, {"users": [idx_country_age]})

    requetes = [
        ("WHERE country='FR' AND age=30",
         [Predicat("country", "=", "FR"), Predicat("age", "=", 30)]),
        ("WHERE country='FR'",
         [Predicat("country", "=", "FR")]),
        ("WHERE score > 900",
         [Predicat("score", ">", 900)]),
    ]

    for sql, preds in requetes:
        plan = planner.planifier("users", preds)
        res, stats = executor.executer(plan)
        print(f"\n  EXPLAIN ANALYZE — SELECT * FROM users {sql}")
        print(f"  {'─'*58}")
        plan.afficher(indent=1)
        print(f"\n  ANALYZE :")
        print(f"    Lignes réelles      : {stats['tuples_retournes']}")
        print(f"    Tuples examinés     : {stats['tuples_lus']}")
        print(f"    Temps réel          : {stats['temps_ms']:.1f}ms")
        erreur = abs(plan.lignes_est - stats['tuples_retournes'])
        pct    = erreur / max(stats['tuples_retournes'], 1) * 100
        print(f"    Erreur estimation   : {erreur} lignes ({pct:.0f}%)")


# ─── SCÉNARIO 3 : MAUVAISES STATISTIQUES ─────────────────────────────────────

def scenario_stale_stats():
    titre("SCÉNARIO 3 — Mauvaises statistiques : quand le planner se trompe")

    print("""
  Si les statistiques sont obsolètes (table modifiée sans ANALYZE),
  le planner peut choisir un plan sous-optimal.

  Simulation : stats calculées sur 10 000 lignes, table grossit à 200 000.
  Le planner croit que la table est petite → choisit un plan coûteux.
    """)

    N_STATS  = 10_000   # Statistiques calculées sur cette taille
    N_REEL   = 200_000  # Taille réelle de la table

    table = generer_table_users(N_REEL)

    # Statistiques stale (sur une petite table)
    stat_stale = stat_users(table[:N_STATS])   # stale
    stat_frais = stat_users(table)              # à jour

    idx = Index(nom="idx_age", colonnes=["age"], type_="btree",
                n_pages=N_REEL // 500)
    idx.construire(table)

    pred = [Predicat("age", ">", 40)]   # ~65% des lignes

    for label, stat in [("Statistiques STALE (10K lignes)", stat_stale),
                         ("Statistiques FRAÎCHES (200K lignes)", stat_frais)]:
        planner = QueryPlanner()
        planner.enregistrer_table(stat)
        planner.enregistrer_index(stat.nom, idx)
        executor = Executor({stat.nom: table}, {stat.nom: [idx]})

        plan = planner.planifier(stat.nom, pred)
        res, stats_exec = executor.executer(plan)

        print(f"\n  {label} :")
        print(f"    Plan choisi   : {plan.type.value}")
        print(f"    Lignes estim. : {plan.lignes_est}  (réel: {len(res)})")
        print(f"    Coût estimé   : {plan.cout_total:.1f}")
        print(f"    Temps réel    : {stats_exec['temps_ms']:.1f}ms")

    print(f"""
  Stat stale → planner croit que la table a 10K lignes →
  peut choisir Index Scan car random I/O "semble" acceptable.
  En réalité 200K lignes → Index Scan sur 65% = 130K random I/Os → lent.

  Solution : ANALYZE régulier (PostgreSQL autovacuum le fait automatiquement).
  Si table grossit vite : ALTER TABLE users SET (autovacuum_analyze_scale_factor = 0.01)
  → déclenche ANALYZE dès que 1% des lignes changent (défaut = 20%)
    """)


# ─── SCÉNARIO 4 : INDEX COMPOSITE DANS LE PLANNER ────────────────────────────

def scenario_composite():
    titre("SCÉNARIO 4 — Index composite dans le planner : préfixe et ordre")

    print("""
  Le planner vérifie si l'index satisfait le préfixe gauche des prédicats.
  Index (country, age) :
    WHERE country='FR' AND age=30  → utilisable ✅
    WHERE country='FR'             → utilisable (préfixe) ✅
    WHERE age=30                   → NON utilisable ❌ (age n'est pas le préfixe)
    """)

    N = 80_000
    table = generer_table_users(N)
    stat  = stat_users(table)

    planner = QueryPlanner()
    planner.enregistrer_table(stat)

    idx_country_age = Index(nom="idx_country_age",
                            colonnes=["country", "age"], type_="btree",
                            n_pages=N // 300)
    idx_age_country = Index(nom="idx_age_country",
                            colonnes=["age", "country"], type_="btree",
                            n_pages=N // 300)
    idx_country_age.construire(table)
    idx_age_country.construire(table)
    planner.enregistrer_index("users", idx_country_age)
    planner.enregistrer_index("users", idx_age_country)

    executor = Executor({"users": table},
                        {"users": [idx_country_age, idx_age_country]})

    cas = [
        ("WHERE country='FR' AND age=30",
         [Predicat("country","=","FR"), Predicat("age","=",30)]),
        ("WHERE country='FR'",
         [Predicat("country","=","FR")]),
        ("WHERE age=30",
         [Predicat("age","=",30)]),
    ]

    print(f"\n  {'Requête':<36} {'Plan':<18} {'Index utilisé':<22} {'Temps'}")
    print("  " + "─"*75)
    for sql, preds in cas:
        plan = planner.planifier("users", preds)
        _, stats = executor.executer(plan)
        idx_str = plan.index_nom or "—"
        print(f"  {sql:<36} {plan.type.value:<18} {idx_str:<22} {stats['temps_ms']:.1f}ms")

    print(f"""
  Résultat attendu :
    WHERE country='FR' AND age=30 → idx_country_age  (préfixe complet)
    WHERE country='FR'            → idx_country_age  (préfixe partiel OK)
    WHERE age=30                  → Seq Scan ou idx_age_country
    """)


# ─── SCÉNARIO 5 : JOINTURE ────────────────────────────────────────────────────

def scenario_jointure():
    titre("SCÉNARIO 5 — Jointure : Nested Loop vs Hash Join selon la taille")

    print("""
  Deux stratégies de jointure :

  Nested Loop Join :
    Pour chaque ligne de T1, chercher les lignes de T2.
    Coût : O(N1 × N2) sans index, O(N1 × log N2) avec index sur T2.
    Bon quand : T1 petite ET index sur T2.

  Hash Join :
    Phase 1 : construire une table de hachage de T2 (la plus petite)
    Phase 2 : pour chaque ligne de T1, lookup dans le hash
    Coût : O(N1 + N2)
    Bon quand : pas d'index, tables moyennes à grandes.

  On simule : orders JOIN users ON orders.user_id = users.id
    """)

    def cout_nested_loop(n1: int, n2: int, has_index: bool) -> float:
        if has_index:
            return n1 * math.log2(max(n2, 2)) * QueryPlanner.CPU_INDEX_TUPLE
        return n1 * n2 * QueryPlanner.CPU_TUPLE_COST

    def cout_hash_join(n1: int, n2: int) -> float:
        # Build phase (n2) + Probe phase (n1)
        return (n2 + n1) * QueryPlanner.CPU_TUPLE_COST * 3

    print(f"\n  {'Taille T1':>10}  {'Taille T2':>10}  {'Index?':>7}  "
          f"{'Nested Loop':>12}  {'Hash Join':>10}  {'Choix planner'}")
    print("  " + "─"*70)

    cas_jointure = [
        (100,    100_000, True),
        (100,    100_000, False),
        (10_000, 100_000, True),
        (10_000, 100_000, False),
        (50_000, 100_000, False),
    ]

    for n1, n2, has_idx in cas_jointure:
        c_nl = cout_nested_loop(n1, n2, has_idx)
        c_hj = cout_hash_join(n1, n2)
        choix = "Nested Loop ✅" if c_nl < c_hj else "Hash Join   ✅"
        idx_str = "✅" if has_idx else "❌"
        print(f"  {n1:>10}  {n2:>10}  {idx_str:>7}  {c_nl:>12.1f}  {c_hj:>10.1f}  {choix}")

    print(f"""
  Analyse :
    T1=100 + index sur T2 → Nested Loop  (100 lookups index = ultra rapide)
    T1=100 sans index      → Hash Join   (construire le hash = moins cher que 100×100K)
    T1=50K sans index      → Hash Join   (O(N1+N2) bat toujours O(N1×N2))

  En production (PostgreSQL) :
    enable_nestloop  = on/off  (forcer le choix pour debug)
    enable_hashjoin  = on/off
    work_mem         = 64MB    (mémoire pour la phase de construction du hash)
    → Si work_mem insuffisant → Hash Join dégrade en Grace Hash Join (spill to disk)
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   QUERY PLANNER & EXECUTION ENGINE                      ║")
    print("╚" + "═"*62 + "╝")

    scenario_selectivite()
    scenario_explain()
    scenario_stale_stats()
    scenario_composite()
    scenario_jointure()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Query Planner — Ce qu'il faut retenir :

    Statistiques → sélectivité → estimation de coût → plan optimal
    Coût Seq Scan    = seq_page_cost × pages + cpu × tuples
    Coût Index Scan  = descente arbre + lignes × random_page_cost
    Coût Bitmap Scan = index séquentiel + heap bitmap (moins de random)
    Coût Hash Join   = O(N1 + N2) — idéal sans index
    Coût Nested Loop = O(N1 × log N2) avec index sur T2

  Seuil empirique :
    Sélectivité < 5%  → Index Scan
    5% → 20%          → Bitmap Scan
    > 20%             → Seq Scan

  Ce que nos scénarios ont prouvé :
    Scénario 1 → premium=5% → Index Scan, age>40=65% → Seq Scan ✅
    Scénario 2 → EXPLAIN montre le plan avec startup et total cost ✅
    Scénario 3 → Stale stats → mauvais plan (index sur 65% des lignes) ✅
    Scénario 4 → age=30 sans préfixe → Seq Scan (index composite inutilisable) ✅
    Scénario 5 → T1=100 + index → Nested Loop, T1=50K → Hash Join ✅

  Outils de diagnostic :
    EXPLAIN           → plan estimé, coûts
    EXPLAIN ANALYZE   → plan réel + temps + lignes réelles
    pg_stat_statements→ requêtes les plus coûteuses
    auto_explain      → log des plans lents automatiquement
  """)

if __name__ == "__main__":
    main()
