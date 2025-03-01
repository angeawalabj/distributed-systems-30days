# Systèmes Distribués — 30 Jours, 35 Modules

Curriculum complet de systèmes distribués implémentés en Python.
Chaque jour : un concept, une implémentation, une simulation avec résultats mesurés.

```
30 jours × ~200 lignes de code + simulation = 6 000+ lignes
35 modules, 175+ scénarios mesurés
```

---

## Structure

```
.
├── jour1-grpc/                   # Jour 1  — gRPC & Protobuf
├── jour2-serialisation/          # Jour 2  — Sérialisation (JSON/Protobuf/Avro)
├── jour3-lamport/                # Jour 3  — Horloges de Lamport
├── jour4-heartbeat/              # Jour 4  — Heartbeat & Détection de pannes
├── jour5-idempotence/            # Jour 5  — Idempotence & Exactly-once
├── jour6-bully/                  # Jour 6  — Élection de leader (Bully)
├── jour7-raft/                   # Jour 7  — Consensus Raft
├── jour8-cap/                    # Jour 8  — Théorème CAP
├── jour9-quorum/                 # Jour 9  — Réplication par Quorum
├── jour10-gossip/                # Jour 10 — Protocole Gossip
├── jour11-consistent-hashing/    # Jour 11 — Consistent Hashing
├── jour12-sharding/              # Jour 12 — Sharding
├── jour13-multi-master/          # Jour 13 — Multi-Master Replication
├── jour14-vector-clocks/         # Jour 14 — Vector Clocks
├── jour15-redlock/               # Jour 15 — Redlock (verrou distribué)
├── jour16-load-balancing/        # Jour 16 — Load Balancing L4/L7
├── jour17-service-discovery/     # Jour 17 — Service Discovery (Consul)
├── jour18-distributed-tracing/   # Jour 18 — Distributed Tracing (Jaeger)
├── jour19-event-sourcing/        # Jour 19 — Event Sourcing & CQRS
├── jour20-saga/                  # Jour 20 — Saga Pattern
├── jour21-hdfs/                  # Jour 21 — HDFS (stockage distribué)
├── jour21-circuit-breaker/       # Bonus  — Circuit Breaker
├── jour22-kafka/                 # Jour 22 — Apache Kafka
├── jour22-rate-limiting/         # Bonus  — Rate Limiting (4 algorithmes)
├── jour23-flink/                 # Jour 23 — Apache Flink (stream processing)
├── jour23-backpressure/          # Bonus  — Backpressure & Queue Management
├── jour24-zerotrust/             # Jour 24 — Zero-Trust Networking
├── jour24-wal/                   # Bonus  — Write-Ahead Log (WAL)
├── jour25-spiffe/                # Jour 25 — SPIFFE/SPIRE (identité workload)
├── jour25-lsm/                   # Bonus  — LSM Tree (stockage clé-valeur)
├── jour26-gateway/               # Jour 26 — API Gateway + OPA
├── jour26-mvcc/                  # Bonus  — MVCC (Multi-Version Concurrency)
├── jour27-cache/                 # Jour 27 — Distributed Cache
├── jour27-index/                 # Bonus  — Index (B-Tree, Hash, BRIN)
├── jour28-chaos/                 # Jour 28 — Chaos Engineering
├── jour29-observabilite/         # Jour 29 — Observabilité (métriques, logs, traces)
├── jour30-spanner/               # Jour 30 — TrueTime & Spanner (synthèse finale)
└── query-planner/                # Bonus  — Query Planner (optimiseur SQL)
```

---

## Curriculum principal — Jours 1 à 30

### Fondations (Jours 1–5)

| Jour | Titre | Concept clé | Résultat mesuré |
|------|-------|-------------|-----------------|
| 1 | **gRPC** | Protobuf + HTTP/2, streaming bidirectionnel | 3.2× plus rapide que JSON, 68% plus petit |
| 2 | **Sérialisation** | JSON vs Protobuf vs Avro, schéma evolution | Protobuf 5× plus compact, Avro le plus flexible |
| 3 | **Lamport Clocks** | Causalité, happens-before, ordre partiel | Détection causale correcte, concurrence détectée |
| 4 | **Heartbeat** | Détection de pannes, timeout adaptatif, SUSPECT/DEAD | Faux positifs éliminés avec timeout × 1.5 |
| 5 | **Idempotence** | Exactly-once, clef d'idempotence, dedup store | 0 doublon malgré rejeu, latence +2ms max |

### Consensus & Coordination (Jours 6–9)

| Jour | Titre | Concept clé | Résultat mesuré |
|------|-------|-------------|-----------------|
| 6 | **Bully Election** | Élection par ID, re-élection sur crash | Leader stable en 3 tours, re-élection en 2 tours |
| 7 | **Raft** | Log consensus, terme, commit majoritaire | 5 nœuds, quorum 3, 0 perte sur crash follower |
| 8 | **CAP** | CP vs AP, partition tolerance, stale reads | Mongo CP : 0 lecture stale. Cassandra AP : lat -40% |
| 9 | **Quorum** | W+R>N, read repair, anti-entropy | W=2 R=2 N=3 : 100% cohérence + tolérance 1 panne |

### Scalabilité & Topologie (Jours 10–15)

| Jour | Titre | Concept clé | Résultat mesuré |
|------|-------|-------------|-----------------|
| 10 | **Gossip** | Propagation épidémique, O(log N), fanout | 1000 nœuds convergent en 10 rounds (fanout=3) |
| 11 | **Consistent Hashing** | Anneau virtuel, vnodes, minimal redistribution | +1 nœud : 12% redistribution (vs 50% naïf) |
| 12 | **Sharding** | Hash/range/directory, hotspot, resharding | Range sharding : 1ms range scan vs 80ms scatter |
| 13 | **Multi-Master** | LWW, merge CRDT, convergence, clock skew | LWW : 3 updates perdues silencieusement révélées |
| 14 | **Vector Clocks** | Causalité multi-nœuds, siblings, merge | Concurrent détecté là où Lamport voyait un ordre |
| 15 | **Redlock** | Quorum de locks, fencing token, GC pause | Token rejeté correctement sous GC pause simulée |

### Opérations (Jours 16–20)

| Jour | Titre | Concept clé | Résultat mesuré |
|------|-------|-------------|-----------------|
| 16 | **Load Balancing** | L4 vs L7, Power of Two, canary, sticky | Power-of-2 : -30% variance vs round-robin |
| 17 | **Service Discovery** | Consul/etcd, TTL health check, Watch | Failover détecté en 1 TTL, cache hit 94% |
| 18 | **Distributed Tracing** | Jaeger, span propagation, sampling | Goulot identifié : db-svc 78% de la latence totale |
| 19 | **Event Sourcing** | Append-only, projections, time-travel | Snapshot réduit replay de 10 000 → 500 events |
| 20 | **Saga** | Chorégraphie vs orchestration, compensation | 3 compensations sur panne partielle, état cohérent |

### Data & Streaming (Jours 21–23)

| Jour | Titre | Concept clé | Résultat mesuré |
|------|-------|-------------|-----------------|
| 21 | **HDFS** | Blocs 128MB, réplication 3×, rack awareness | 4.2× gain lecture parallèle, 0 perte sur rack down |
| 22 | **Kafka** | Topics, partitions, offsets, consumer groups | Replay 60 msgs historiques, 0 doublon avec idempotence |
| 23 | **Flink** | Tumbling/Sliding/Session windows, watermarks | Fraude détectée en <1s (bob : 1436€/5min) |

### Sécurité & Identité (Jours 24–26)

| Jour | Titre | Concept clé | Résultat mesuré |
|------|-------|-------------|-----------------|
| 24 | **Zero-Trust** | mTLS, SPIFFE, OPA, moindre privilège | Mouvement latéral bloqué, 3 refus → alerte SIEM |
| 25 | **SPIFFE/SPIRE** | Attestation workload, SVID, rotation, fédération | 3 pods sans secret → SVIDs émis, rotation 0-downtime |
| 26 | **API Gateway + OPA** | Pipeline auth→RL→policy→route, policy-as-code | Blocage urgence en <1s, hacker : 6 refus → détecté |

### Résilience (Jours 27–28)

| Jour | Titre | Concept clé | Résultat mesuré |
|------|-------|-------------|-----------------|
| 27 | **Distributed Cache** | Cache-Aside, stampede, L1/L2, warming | 50 threads → avec mutex : 1 appel base (sans : 50) |
| 28 | **Chaos Engineering** | Steady state, injection, abort condition | 2/5 hypothèses réfutées → faiblesses corrigées |

---

## Modules bonus — Base de données engine

Piste parallèle aux jours 21-28, implémentant un moteur de base de données
de zéro : WAL → LSM → MVCC → Index → Query Planner.

| Module | Titre | Concept clé | Résultat mesuré |
|--------|-------|-------------|-----------------|
| Circuit Breaker | `jour21-circuit-breaker` | CLOSED/OPEN/HALF_OPEN, fallback | 21 court-circuits → payments protégé |
| Rate Limiting | `jour22-rate-limiting` | Fixed Window, Token Bucket, Leaky Bucket | Token Bucket : burst 10 puis 429 exact |
| Backpressure | `jour23-backpressure` | DROP/BLOCK/PRIORITY, work stealing | Priority : 0 message critique perdu |
| WAL | `jour24-wal` | Write-Ahead Log, ACID, crash recovery | Crash recovery en 3 étapes, 0 perte |
| LSM Tree | `jour25-lsm` | MemTable, SSTable, Bloom filter, compaction | Write 10× plus rapide que B-Tree |
| MVCC | `jour26-mvcc` | Snapshot isolation, versions, GC | 0 dirty read, lecteurs non bloquants |
| Index | `jour27-index` | B-Tree, Hash Index, BRIN | B-Tree range : 2ms, Hash exact : 0.1ms |
| Query Planner | `query-planner` | Sélectivité, EXPLAIN, join strategies | Stale stats → index scan sur 65% détecté |

---

## Fil conducteur : comment tout s'assemble

```
Données stockées  →  HDFS (blocs) + LSM (clé-valeur) + WAL (durabilité)
Données indexées  →  B-Tree / Hash / BRIN  →  Query Planner
Données répliquées→  Raft (consensus) + Quorum + MVCC (isolation)
Données distribuées→ Sharding + Consistent Hashing + Vector Clocks
Données en flux   →  Kafka (transport) + Flink (traitement fenêtré)

Services découverts→ Service Discovery (Consul)
Services équilibrés→ Load Balancer L7 (Power of Two)
Services tracés   →  Distributed Tracing (Jaeger)

Identité prouvée  →  SPIFFE/SPIRE (attestation)
Réseau sécurisé   →  mTLS + Zero-Trust
Accès contrôlé    →  API Gateway + OPA (policy-as-code)

Performance       →  Distributed Cache (L1/L2 Redis)
Résilience        →  Circuit Breaker + Rate Limiting + Backpressure
Pannes validées   →  Chaos Engineering (hypothèse → expérience → correctif)

Transactions distrib→ Saga (compensation) + Event Sourcing (replay)
Coordination      →  Lamport / Vector Clocks + Bully + Redlock
```

### Synthèse finale : Google Spanner / TrueTime

Tous ces concepts convergent dans Spanner :
- **Raft** pour le consensus par groupe de Paxos
- **Consistent Hashing** pour distribuer les tablets
- **MVCC** pour les transactions globales non-bloquantes
- **WAL** pour la durabilité sur chaque réplique
- **TrueTime** (horloges GPS + atome) pour ordonner les transactions sans coordinateur central

Résultat : une base de données distribuée sur 3+ continents, avec des transactions ACID, à 99.999% de disponibilité.


### Jour 29 — Observabilité
`jour29-observabilite/`

Les 3 piliers : métriques (Counter/Gauge/Histogramme → méthode RED), logs structurés (recherche multi-critères, corrélation via trace_id), traces distribuées (OpenTelemetry, flamegraph, spans lents). SLO, Error Budget, Burn Rate, alertes Alertmanager.

**Résultat mesuré :** Corrélation métrique→log→trace : MTTD 45min → 3min, MTTR 2h → 8min. Semaine 3 burn rate 80× → alerte critique. p99=144ms invisible sur la moyenne ; histogramme révèle tout. Stale Read : 0.01ms vs Read-Write 15.06ms.

---

### Jour 30 — TrueTime & Spanner *(synthèse)*
`jour30-spanner/`

TrueTime : horloges atomiques GPS + césium, incertitude bornée ε ≤ 7ms. Commit Wait Protocol : attendre 2ε pour garantir la causalité mondiale. MVCC distribué multi-tablets. Read-Only sans verrou, Stale Read sur réplique locale. Synthèse des 30 jours.

**Résultat mesuré :** Commit Wait moyen 14.65ms (≈ 2ε). 3 RW transactions : historique MVCC complet, snapshot à tout timestamp passé. Sans Commit Wait (NTP ±150ms) : causalité non garantie, Bob ne voit pas le paiement d'Alice. Read-Only sans verrou : 750× plus rapide que RW.

---

---

## Comment utiliser ce curriculum

### Lancer tous les scénarios

```bash
# Chaque module est autonome
cd jour7-raft && python3 simulation.py

# Vérifier les résultats d'un module
cat jour7-raft/README.md
```

### Ordre recommandé

```
Débutant   : 1 → 3 → 4 → 7 → 8 → 11 → 16
Intermédiaire: + 9 → 10 → 12 → 13 → 14 → 19 → 20
Avancé     : tout dans l'ordre + bonus DB engine
Production : 21-28 (HDFS/Kafka/Flink/ZT/SPIFFE/Gateway/Cache/Chaos)
```

### Prérequis

```bash
python3 --version  # 3.10+
# Aucune dépendance externe — stdlib Python uniquement
```

---

## Résultats remarquables

| Résultat | Module | Valeur |
|----------|--------|--------|
| Gain sérialisation Protobuf | Jour 2 | 5× plus compact que JSON |
| Redistribution consistent hashing | Jour 11 | 12% (vs 50% naïf) |
| Latence Circuit Breaker fail-fast | Bonus CB | <1ms (vs timeout 30s) |
| Gain lecture parallèle HDFS | Jour 21 | 4.2× sur 6 blocs |
| Fraud detection Flink | Jour 23 | <1s entre transaction et alerte |
| Stampede protection cache | Jour 27 | 50 threads → 1 appel base |
| Chaos : hypothèses réfutées | Jour 28 | 2/5 → faiblesses découvertes |

---

## Références

Chaque module cite ses inspirations :

- **Raft** : Ongaro & Ousterhout, 2014 — "In Search of an Understandable Consensus Algorithm"
- **CAP** : Gilbert & Lynch, 2002 — "Brewer's Conjecture and the Feasibility of Consistent Available Services"
- **Consistent Hashing** : Karger et al., 1997 — "Consistent Hashing and Random Trees"
- **CRDT** : Shapiro et al., 2011 — "Conflict-Free Replicated Data Types"
- **Kafka** : Kreps, Narkhede & Rao, 2011 — "Kafka: a Distributed Messaging System for Log Processing"
- **Flink** : Carbone et al., 2015 — "Apache Flink: Stream and Batch Processing in a Single Engine"
- **SPIFFE** : CNCF — spiffe.io
- **Spanner** : Corbett et al., 2012 — "Spanner: Google's Globally Distributed Database"
- **Chaos Engineering** : Basiri et al., 2016 — "Chaos Engineering" (IEEE Software)
