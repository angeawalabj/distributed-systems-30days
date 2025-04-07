# Query Planner — "Le Stratège des Requêtes"

## Le problème

```sql
SELECT * FROM users WHERE premium = true;
```

Deux façons d'exécuter cette requête :
```
Seq Scan   : lire toutes les 100 000 lignes → filtrer → garder 5 000
Index Scan : descendre le B-Tree → accéder directement aux 5 000 lignes

Avec un mauvais choix : 10× à 100× plus lent
```

**Le query planner choisit automatiquement le plan le moins coûteux** en utilisant les statistiques de la table.

## Fonctionnement

```
Requête SQL
    │
    ▼
Planner : consulte pg_statistic (n_rows, histogrammes, MCV...)
    │
    ├── Estimer la sélectivité : combien de lignes va-t-on lire ?
    │
    ├── Calculer le coût de chaque plan :
    │     Coût Seq Scan   = seq_page_cost × pages + cpu × tuples
    │     Coût Index Scan = descente B-Tree + lignes × random_page_cost
    │     Coût Hash Join  = O(N1 + N2)
    │     Coût Nested Loop= O(N1 × log N2) avec index sur T2
    │
    └── Choisir le plan de coût minimal
```

## Règles de décision (seuils PostgreSQL)

```
Sélectivité < 5%   → Index Scan  (peu de lignes → random I/O vaut le coup)
Sélectivité 5-20%  → Zone grise  (dépend des coûts disque)
Sélectivité > 20%  → Seq Scan    (trop de lignes → seq. toujours moins cher)
```

## Résultats mesurés

### Scénario 1 — Sélectivité décide

```
Prédicat         Sélectivité   Plan choisi      Justification
───────────────────────────────────────────────────────────────────────
premium=True          5%       Index Scan  ✅   peu de résultats
age > 40             65%       Seq Scan    ✅   trop de résultats
score BETWEEN...     10%       Index Scan  ✅   sélectif
country = 'FR'       20%       Seq Scan    ✅   trop commun
```

### Scénario 2 — EXPLAIN (plan avec coûts)

```
EXPLAIN SELECT * FROM users WHERE premium = true;

→ Index Scan on idx_premium
    startup cost = 0.29
    total cost   = 45.12
    rows estimés = 5 000
    filter       : premium = true

Comparer avec Seq Scan :
    total cost = 433.0  (9.6× plus cher)
```

### Scénario 3 — Mauvaises statistiques

```
Table réelle : 200 000 lignes, age > 40 → 65% → Seq Scan correct

Avec stats obsolètes (ANALYZE non exécuté depuis une migration) :
  Stats pensent : 10 000 lignes, sélectivité 5%
  Plan choisi   : Index Scan  ← MAUVAIS CHOIX
  Erreur estimation : 99%

→ Index Scan sur 130 000 lignes = 130 000 random I/Os = catastrophe

Solution : ANALYZE users; (ou autovacuum bien configuré)
```

### Scénario 4 — Index composite et leftmost prefix rule

```
Index : idx_age_country ON users(age, country)

WHERE age=30 AND country='FR'  → ✅ utilise l'index complet
WHERE age=30                   → ✅ utilise le préfixe gauche (age)
WHERE country='FR'             → ❌ Seq Scan  (country seul, pas de préfixe)
WHERE age BETWEEN 20 AND 30    → ✅ range scan sur age
WHERE age=30 ORDER BY country  → ✅ index couvre le tri

Règle : l'index (a, b, c) est utilisable seulement si la requête
        commence par 'a' dans ses prédicats.
```

### Scénario 5 — Jointures : Nested Loop vs Hash Join

```
T1 lignes   Index sur T2   Plan choisi          Coût
─────────────────────────────────────────────────────────
100         oui            Nested Loop  ✅      100 × log(100K) ≈ 1 700
100         non            Hash Join    ✅      100 + 100K = 100 100
50 000      oui            Hash Join    ✅      50K + 100K = 150K (Nested Loop = 850K)
50 000      non            Hash Join    ✅      O(N1+N2)

→ Petite outer table + index → Nested Loop
→ Grandes tables sans index  → Hash Join (O(N1+N2) bat O(N1×log N2))
```

## Quand le planner se trompe

| Cause | Symptôme | Solution |
|-------|----------|----------|
| Stats obsolètes | Index Scan sur 65% des lignes | `ANALYZE table` |
| Correlation incorrecte | Mauvais ordre de filtre | Recalculer les stats |
| Seuil random_page_cost trop haut | Préfère Seq Scan même sur 2% | `SET random_page_cost = 1.1` (SSD) |
| `work_mem` insuffisant | Hash Join → Grace Hash (spill) | Augmenter `work_mem` |

## Forcer un plan (dernier recours)

```sql
-- Désactiver temporairement un type de scan pour déboguer
SET enable_seqscan = off;
SET enable_nestloop = off;

-- Ou utiliser des hints (pg_hint_plan extension)
/*+ IndexScan(users idx_premium) */ SELECT ...
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `planner.py` | `QueryPlanner`, `Executor`, `StatTable`, `StatColonne`, `Index`, `Predicat` |
| `simulation.py` | 5 scénarios : sélectivité, EXPLAIN, stats obsolètes, index composite, jointures |

## Lancer

```bash
python3 simulation.py
```

## Contexte dans le curriculum

Ce module fait partie des "bonus base de données" (jours 24-28 bonus) :  
WAL → LSM Tree → MVCC → **Index (B-Tree/Hash/BRIN)** → **Query Planner**  
→ Jour 30 (Synthèse) : TrueTime/Spanner — MVCC distribué avec horloges atomiques.
