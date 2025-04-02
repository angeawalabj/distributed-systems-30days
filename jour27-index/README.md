# Jour 27 — Index : B-Tree, Hash, BRIN

## Le problème

```sql
SELECT * FROM users WHERE age = 30;
-- Sans index : lire TOUTES les lignes = O(N) = seq scan
-- Avec index : O(log N) ou O(1) selon le type
```

Un index = structure auxiliaire qui mappe `valeur → RID (Row ID)`.

## Les 3 types implémentés

### B-Tree (universel)
```
Hauteur 3 pour 100 000 clefs avec T=50 (log_50(100000) ≈ 2.9)
Supporte : =  <  >  BETWEEN  ORDER BY  LIKE 'foo%'
Ne supporte pas : LIKE '%foo'  (pas de préfixe)
```

### Hash Index (equality uniquement)
```
O(1) amortie — parfait pour WHERE email='alice@example.com'
Inutilisable pour WHERE age > 30  (pas de range)
```

### BRIN (séries temporelles)
```
Stocke uniquement min/max par bloc de 128 lignes.
85× plus petit qu'un B-Tree pour les mêmes données.
Efficace seulement si données corrélées à l'ordre physique.
```

## Résultats mesurés

### Scénario 1 — B-Tree (100 000 clefs, T=50)

```
Insertion      : 130 115 inserts/s
Hauteur        : 3  (log_50(100000) ≈ 2.9)  ← ultra-plat
Nœuds          : 1468

Range scan (~5% des données) :
  B-Tree   :  5001 résultats en  2.17ms  ← démarre au bon endroit
  Seq scan :  5001 résultats en  7.29ms  ← parcourt tout
  B-Tree 3× plus rapide ✅
```

*(Note : le dict Python (C) bat notre B-Tree Python sur les lookups ponctuels — le B-Tree gagne sur les range scans, son vrai avantage.)*

### Scénario 2 — Hash Index (50 000 entrées)

```
Buckets   Charge    Max collisions   1000 lookups
  1024    48.8×          72            3.8ms  (dégradé)
  4096    12.2×          25            1.6ms
 25000     2.0×           9            1.0ms  ← optimal

Comparaison directe B-Tree vs Hash (50K entrées, 1000 lookups) :
  B-Tree : 6.90ms
  Hash   : 1.68ms
  Hash 4.1× plus rapide pour l'equality ✅
```

### Scénario 3 — BRIN (1M lignes)

```
                      Taille BRIN   Taille B-Tree   Ratio
Timestamps croissants     183 KB      15 625 KB      85×

Pour "dernière heure" (3600 lignes / 1M) :
  Blocs candidats : 1 / 7813
  Lignes à scanner : 64 / 1 000 000  (0.006%)  ✅

Table de 100M logs en production :
  B-Tree : ~3 GB d'index
  BRIN   : ~150 KB d'index  (20 000× plus petit !)
```

### Scénario 4 — Index composite : leftmost prefix rule

```sql
-- Table : 100 000 lignes, 10 pays, âges 18-80

-- WHERE country='FR' AND age=30
Index (country, age) :  89 résultats en  0.12ms  ✅
Index (age, country) :  91 résultats en  0.12ms  ✅
Seq scan             : 147 résultats en  2.55ms

-- WHERE age=30  (sans country)
Index (country, age) : 100000 résultats en 67.05ms  ❌ balayage complet
Index (age, country) :   1647 résultats en  0.91ms  ✅
Seq scan             :   1647 résultats en  1.98ms
```

**67ms vs 0.9ms** — preuve directe que l'ordre des colonnes dans un index composite est critique.

```
Leftmost Prefix Rule — Index (A, B, C) :
  WHERE A=x              ✅  préfixe (A)
  WHERE A=x AND B=y      ✅  préfixe (A, B)
  WHERE B=y              ❌  pas de préfixe
  WHERE B=y AND C=z      ❌  pas de préfixe
```

### Scénario 5 — Write overhead

```
Configuration              Insert/s    Durée   Overhead
0 index (heap only)       8 289 850      2ms   —
1 index (age)               210 587     95ms   39×
3 index (age,gender,zip)     89 065    225ms   93×
5 index (tous)               39 288    509ms   211×
```

Chaque index ajoute une mise à jour structurée à chaque INSERT/UPDATE/DELETE. 5 index → 211× plus lent que heap-only.

## Quand utiliser quel index

| Index | Supporte | Ne supporte pas | Taille | Usage |
|-------|---------|----------------|--------|-------|
| **B-Tree** | `= < > BETWEEN ORDER BY LIKE 'x%'` | `LIKE '%x'` | Moyen | 95% des cas |
| **Hash** | `=` uniquement | Range, ORDER BY | Petit | Jointures par égalité |
| **BRIN** | Range sur données corrélées | Données désordonnées | Minuscule | Logs, séries temporelles |
| **GiST** | Geometries, full-text, intervalles | — | Grand | PostGIS, tsvector |

## Lancer

```bash
python3 simulation.py
```

## Jour 28 → Query Planner & Execution Engine

Comment la base choisit-elle quel index utiliser — ou de ne pas utiliser d'index du tout ? Le query planner estime les coûts en combinant les statistiques de colonnes (cardinalité, histogramme) avec les coûts matériels (seq_page_cost vs random_page_cost). `EXPLAIN ANALYZE` expose ce raisonnement. On implémente un mini planner avec statistiques et estimations de sélectivité.
