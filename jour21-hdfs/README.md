# Jour 21 — HDFS : Hadoop Distributed File System

## Le problème

Stocker 1 TB sur une seule machine :

```
Disque unique → panne = perte totale
Lecture séquentielle → pas de parallélisme
Scalabilité plafonnée à la machine la plus grosse
```

## La solution : 3 idées fondamentales

### 1. Découpage en blocs (128 MB par défaut)

```
Fichier 1 TB → 8 192 blocs de 128 MB
Chaque bloc stocké indépendamment sur un DataNode
→ Lecture parallèle depuis N DataNodes simultanément
```

### 2. Réplication (facteur 3 par défaut)

```
Bloc A → copié sur DN1, DN3, DN7
→ Tolère 2 pannes simultanées sans perte de données
```

### 3. Rack Awareness

```
Réplique 1 → DataNode local (performance)
Réplique 2 → DataNode dans un rack DIFFÉRENT (tolérance rack)
Réplique 3 → DataNode dans le même rack que réplique 2

Pourquoi ? Un switch de rack peut tomber → tout un rack inaccessible.
Avec 2 répliques dans des racks différents → on survit à 1 panne de rack.
```

## Architecture

```
NameNode (1 seul)                DataNodes (N machines)
┌──────────────────────┐         ┌────────────────┐
│  Namespace           │◄────────│  Heartbeat 3s  │
│  /data/file.csv      │         │  Block Report  │
│  → [blk_1, blk_2...] │         │  Stocke blocs  │
│                      │         └────────────────┘
│  blk_1 → DN1,DN3,DN7 │
│  blk_2 → DN2,DN5,DN8 │
│  (MÉTADONNÉES SEUL)  │
└──────────────────────┘

Client :
  1. Demande au NameNode : "où est /data/file.csv ?"
  2. Reçoit : bloc1→[DN1,DN3], bloc2→[DN2,DN5]...
  3. Lit chaque bloc depuis le DataNode le plus proche
  4. Le NameNode ne voit jamais les données → pas de bottleneck
```

## Résultats mesurés

### Scénario 1 — Découpage et réplication

```
📁 /data/ml/training.parquet  (1280 MB → 10 blocs × 3 répliques)
Bloc            Taille  Répliques → Racks
───────────────────────────────────────────────────────
blk_000008       128MB  ['dn-r2-2', 'dn-r1-0', 'dn-r1-1']  ['rack-2', 'rack-1', 'rack-1']
blk_000009       128MB  ['dn-r1-2', 'dn-r2-0', 'dn-r2-1']  ['rack-1', 'rack-2', 'rack-2']
...
```

### Scénario 2 — Rack awareness

```
Distribution de 30 blocs sur 4 racks × 4 DataNodes :

  Racks distincts   Blocs      %  Tolérance
  ─────────────────────────────────────────────────────
  2 rack(s)           30   100%  ⚠️  survit à 1 panne de rack
```

### Scénario 3 — Panne DataNode

```
💥 Panne : ['dn-r0-0', 'dn-r0-1'] (rack-0)

[NameNode] dn-r0-0 → DEAD
  Blocs affectés        : 4
  Re-répliqués          : 4
  Encore sous-répliqués : 0

[NameNode] dn-r0-1 → DEAD
  Blocs affectés        : 5
  Re-répliqués          : 5
  Encore sous-répliqués : 0

→ Re-réplications auto : 9 — 0 bloc sous-répliqué ✅
```

### Scénario 4 — Panne rack entier

```
💥 Panne rack entier : rack-1 (4 DataNodes)

  Blocs affectés          : 18
  Blocs encore lisibles   : 15/15 (100%) ✅
  Re-réplications lancées : 18
```

### Scénario 5 — Lecture parallèle

```
6 blocs lus EN PARALLÈLE : 36ms
Séquentiel estimé        : ~150ms
Gain parallélisme        : 4.2× ✅

Tous les blocs lus depuis rack-0 (local) → data locality
```

## Tolérance aux pannes

| Événement | Détection | Récupération |
|-----------|-----------|--------------|
| 1 DataNode mort | 30s (heartbeat timeout) | Re-réplication automatique |
| 1 rack entier | Idem | Copies sur 2 autres racks |
| NameNode mort | Immédiat | ❌ Cluster inaccessible (SPOF) |

**Solution NameNode HA** : Standby + ZooKeeper (Hadoop 2+), basculement en < 30s.

## Data Locality

```
Spark connaît la localisation des blocs via le NameNode.
Il schedule les tâches SUR la machine qui a les données.
→ 0 transfert réseau → 10-100× plus rapide qu'un accès distant.
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `hdfs.py` | `NameNode`, `DataNode`, `Bloc`, `EtatNode` |
| `simulation.py` | 5 scénarios : découpage, rack awareness, panne DN, panne rack, lecture |

## Lancer

```bash
python3 simulation.py
```

## Jour 22 → Kafka

HDFS stocke des fichiers **statiques** (batch).  
Kafka stocke des **flux d'événements** en temps réel (streaming).  
Topics, partitions, offsets, consumer groups, replay, at-least-once vs exactly-once.
