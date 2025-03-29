# Jour 25 — LSM Tree & SSTables (RocksDB / Cassandra / LevelDB)

## Le problème

Comment stocker des centaines de millions de clés avec des écritures rapides ?

```
B-Tree (PostgreSQL, MySQL) :
  ✅ Lectures rapides O(log N)
  ❌ Écritures lentes → random I/O (mise à jour in-place)

LSM Tree :
  ✅ Écritures ultra-rapides → toujours séquentiel
  ✅ 10× plus de throughput d'écriture que B-Tree
  ❌ Lectures plus complexes (chercher plusieurs niveaux)
  ❌ Compaction en arrière-plan (CPU + I/O)
```

## Architecture

```
Écriture :
  Application → MemTable (mémoire, trié)
                    │ [pleine]
                    ▼
               SSTable L0 (disque, immutable, trié)
                    │ [4 SSTables L0]
                    ▼ compaction
               SSTable L1 (10× plus grand)
                    │ compaction
               SSTable L2 (10× L1)
                    ...

Lecture :
  Application → MemTable → L0 → L1 → L2...
               [Bloom filter avant chaque niveau]
```

## Résultats mesurés

### Scénario 1 — Bloom Filter

```
fp_rate    Bits/élém   Hash fns   Taille (1M éléments)
0.1000          4.8          3            585 KB
0.0100          9.6          6           1170 KB
0.0010         14.4          9           1755 KB
0.0001         19.2         13           2340 KB

Test réel (10 000 éléments, cible 1%) :
  Faux positifs : 80/10000 = 0.80%  ✅ (sous 1%)
  Faux négatifs : 0/10000  = 0.00%  ✅ (toujours 0)
  Taille bloom  : 11.7 KB  vs  488 KB (set Python)

Impact sur les lectures :
  Sans Bloom : 1000 lectures × 5 SSTables = 5000 I/Os disque
  Avec Bloom : ~50 I/Os (99% évitées) ✅
```

### Scénario 2 — Cycle MemTable → SSTable

```
50 écritures en mémoire : 0.12ms  (418 000/s)
Flush → SSTable         : 0.89ms

SSTable créé :
  Clef min  : user:0000  →  max : user:0049
  Trié lexicographiquement : ✅
  Bloom     : 479 bits, 6 fonctions de hash
  Taille    : 4 609 octets

Recherche binaire 20 clefs : 0.16ms (128 000/s)
1000 clefs absentes → Bloom bloque 990/1000 (99%) ✅
```

### Scénario 3 — Compaction

```
Avant (3 SSTables L0) :
  SST1: alice=1000(seq1), bob=500(seq2), charlie=300(seq3)
  SST2: alice=800(seq5), diana=700(seq6), eve=∅(seq7 tombstone)
  SST3: bob=450(seq9), eve=999(seq4), frank=200(seq10)

Après compaction (1 SSTable L1) :
  alice   = 800   seq=5   ← version la plus récente
  bob     = 450   seq=9   ← version la plus récente
  charlie = 300   seq=3
  diana   = 700   seq=6
  frank   = 200   seq=10
  [eve supprimé — tombstone éliminé ✅]

9 entrées brutes → 5 entrées nettes
```

### Scénario 4 — Read path

```
Clef en MemTable  : key:0050 = 'UPDATED:50'   [3µs]
Clef dans SSTable : key:0099 = 'value:99'     [21µs]
Clef absente      : key:9999 = None           [8µs]
Clef tombstone    : key:0001 = None           [2µs]

Bloom filter évite 99% des I/Os sur les clefs absentes
```

### Scénario 5 — Write Amplification

```
300 écritures + 60 mises à jour :
  Octets application  :  7 838 B
  Octets disque total : 19 679 B
  WAF mesuré          : 2.5×  (faible car 1 seul niveau)

En production (plusieurs niveaux de compaction) :
  RocksDB leveled  : WAF 10-30×
  RocksDB universal: WAF 5-10×
  Cassandra        : WAF 5-20×
```

## Bloom Filter : propriété fondamentale

```
"absent" → CERTAIN absent  → ne pas lire le SSTable
"présent" → PROBABLE présent → lire le SSTable (peut être faux positif)

Faux négatifs : impossibles par construction
Faux positifs : taux configurable (1% = standard)

Formule :
  m = -n × ln(fp) / ln(2)²   (bits nécessaires)
  k = m/n × ln(2)             (nombre de hash functions)
```

## Compaction : Leveled vs Tiered

| | Leveled | Tiered |
|---|---|---|
| Structure | 1 SSTable par clef par niveau | Plusieurs SSTables par niveau |
| Lectures | Rapides (1 SSTable max par niveau) | Plus lentes |
| WAF | Élevé (10-30×) | Plus faible (5-10×) |
| Utilisé par | RocksDB leveled, LevelDB | Cassandra, RocksDB universal |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `lsm.py` | `BloomFilter`, `MemTable`, `SSTable`, `MoteurLSM`, `compacter` |
| `simulation.py` | 5 scénarios : bloom, flush, compaction, read path, WAF |

## Lancer

```bash
python3 simulation.py
```

## Jour 26 → MVCC (Multi-Version Concurrency Control)

Le WAL garantit la **durabilité**, le LSM Tree optimise les **écritures**. Comment gérer les **lectures et écritures concurrentes** sans verrous ? PostgreSQL et MySQL InnoDB utilisent MVCC : chaque transaction voit un snapshot immutable. Les lecteurs ne bloquent pas les écrivains, les écrivains ne bloquent pas les lecteurs. On implémente un moteur MVCC avec snapshots, versions, et garbage collection des anciennes versions.
