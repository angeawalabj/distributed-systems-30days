# Jour 12 — Sharding (Partitionnement)

## Le problème

Une seule base de données atteint ses limites physiques :
- 10 milliards de lignes → les index ne tiennent plus en RAM
- 100 000 écritures/seconde → le disque sature
- 10 To → impossible sur un seul serveur

**Solution : diviser les données en shards** répartis sur plusieurs serveurs. Chaque shard contient un sous-ensemble. Les requêtes sont routées vers le bon shard.

## Les 3 stratégies

### 1. Hash Sharding
```
shard = hash(clé) % N
```
✅ Distribution uniforme (CV ~3%)
❌ Range queries impossibles — scan de tous les shards
❌ Resharding catastrophique (75% des clés migrent si N+1)

### 2. Range Sharding
```
Shard 0 : user:A – user:F
Shard 1 : user:F – user:L
Shard 2 : user:L – user:R
Shard 3 : user:R – ∞
```
✅ Range queries ciblées (2 shards au lieu de 4)
❌ Hotspot si clés séquentielles (IDs auto-increment)

### 3. Directory Sharding
```
directory["user:alice"] = shard_id=2
directory["user:bob"]   = shard_id=0
```
✅ Migration chirurgicale sans rehashing ni downtime
❌ Le directory est un SPOF → doit être répliqué (Raft, Jour 7)

## Résultats mesurés

### Scénario 1 — Comparaison (3 000 enregistrements, 4 shards)
```
Stratégie    CV%   Équilibre    Range Query
Hash          3%   ✅ uniforme   ❌ scan all
Range         3%   ✅ uniforme   ✅ efficace
Directory     0%   ✅ parfait    ❌ scan all
```

### Scénario 2 — Hotspot Range Sharding (IDs séquentiels)
```
1000 nouvelles inscriptions (IDs 9001–10000) :

Shard-0 :    0 (  0.0%)  ← inactif
Shard-1 :    0 (  0.0%)  ← inactif
Shard-2 :    0 (  0.0%)  ← inactif
Shard-3 : 1000 (100.0%)  ← HOTSPOT, CV=173% ❌

→ En prod : disque de Shard-3 sature, les 3 autres sont vides.
→ Solution : UUIDs, ou hash sur l'ID avant le range sharding.
```

### Scénario 3 — Resharding (5 000 clés, 3→4 shards)
```
Hash naïf (modulo)  : 3766/5000 migrées  (75.3%)  ~75% théorique
Consistent Hashing  : 1163/5000 migrées  (23.3%)  ~25% théorique

→ Consistent Hashing migre 3.2x moins de clés.
Impact sur 100 Go : 75 Go vs 25 Go à transférer.
```

### Scénario 4 — Range Query (user:G* à user:M*)
```
Hash Sharding  : 4 shards scannés  → toujours O(N)
Range Sharding : 2 shards scannés  → O(shards couverts)

Sur 100 shards et 10 To : 100 I/O vs 2 I/O.
```

### Scénario 5 — Migration Directory (Shard-0 plein)
```
Avant :  Shard-0=600  Shard-1=133  Shard-2=133

Migration de 300 clés Shard-0 → Shard-3 (785ms, sans downtime)

Après :  Shard-0=434  Shard-1=333  Shard-2=333  Shard-3=300 ✅
Lectures post-migration : transparentes ✅
```

## Règle de décision

```
┌────────────────────────────┬──────────────────────────────┐
│ Cas d'usage                │ Stratégie recommandée        │
├────────────────────────────┼──────────────────────────────┤
│ Accès par clé exacte       │ Hash ou Consistent Hash      │
│ ORDER BY, BETWEEN, range   │ Range Sharding               │
│ IDs séquentiels            │ Hash (évite le hotspot)      │
│ Migration fréquente        │ Directory Sharding           │
│ Cluster évolutif           │ Consistent Hash (Jour 11)    │
└────────────────────────────┴──────────────────────────────┘
```

## Utilisé en production

| Système | Stratégie | Détails |
|---------|-----------|---------|
| **MongoDB** | Hash + Range | Hash par défaut, Range optionnel |
| **Cassandra** | Consistent Hash | 256 vnodes, partition key |
| **HBase/Bigtable** | Range | Row key → tablet server |
| **CockroachDB** | Range | Ranges de 64 Mo, split automatique |
| **DynamoDB** | Consistent Hash | Partition key → partition |
| **Elasticsearch** | Hash | `_id` → shard number |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `sharding.py` | `HashSharding`, `RangeSharding`, `DirectorySharding`, `ConsistentHashSharding`, `Shard` |
| `simulation.py` | 5 scénarios : comparaison, hotspot, resharding, range query, migration |

## Lancer

```bash
python3 simulation.py
```

## Jour 13 → Réplication Multi-Maître

Le sharding répartit les données. La réplication les duplique pour la tolérance aux pannes. Avec un seul maître, c'est simple. Avec **plusieurs maîtres** (chaque datacenter peut écrire), deux utilisateurs peuvent modifier la même donnée simultanément sur deux serveurs différents. Comment détecter et résoudre ce conflit ? Last-Write-Wins, merge automatique, ou intervention applicative ?
