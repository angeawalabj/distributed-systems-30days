# Changelog

Tous les modules sont listés dans l'ordre de création.
Format : `[JOUR] Module — Concept clé prouvé`

---

## Phase 4 — Big Data, Sécurité, Résilience (jours 21–30)

- **[J30]** TrueTime & Spanner — external consistency, commit wait ~8ms, MVCC time travel
- **[J29]** Observabilité — RED metrics, logs structurés, traces distribuées, SLO/burn rate
- **[J28]** Chaos Engineering — blast radius progressif, gameday multi-pannes, kill switch SLO
- **[J27b]** Distributed Cache — stampede mutex, L1/L2, invalidation, cache warming
- **[J27a]** Index B-Tree/Hash/BRIN — sélectivité, EXPLAIN, leftmost prefix rule
- **[J26b]** API Gateway + OPA — policy-as-code, hot-reload <1s, token bucket
- **[J26a]** MVCC — snapshot isolation, 0 deadlock sur 100 tx concurrentes
- **[J25b]** SPIFFE/SPIRE — attestation zéro secret, rotation, fédération
- **[J25a]** LSM Tree — MemTable/SSTable, Bloom filters, compaction levels
- **[J24b]** Zero-Trust — mTLS, 4 piliers, mouvement latéral bloqué
- **[J24a]** WAL — durabilité crash-safe, recovery par replay
- **[J23b]** Flink — tumbling/sliding/session windows, watermarks, détection fraude
- **[J23a]** Backpressure — bounded queues, drop policies, stabilité sous surcharge
- **[J22b]** Kafka — partitions, consumer groups, replay, exactly-once
- **[J22a]** Rate Limiting — token bucket, sliding window, leaky bucket
- **[J21b]** HDFS — blocs 128MB, réplication 3×, rack awareness, data locality
- **[J21a]** Circuit Breaker — CLOSED/OPEN/HALF_OPEN, cascade évitée

## Phase 3 — Architecture & Opérations (jours 11–20)

- **[J20]** Saga — choreography vs orchestration, compensating transactions
- **[J19]** Event Sourcing — replay, projections, CQRS, snapshots
- **[J18]** Distributed Tracing — OpenTelemetry, W3C Trace Context, flamegraph
- **[J17]** Service Discovery — TTL, health checks, watches, DNS-based
- **[J16]** Load Balancing — Least Connections, P99 mesuré, health checks
- **[J15]** Redlock — mutex distribué Redis, quorum 3/5, fencing token
- **[J14]** Vector Clocks — ordre causal, concurrence détectée, merge
- **[J13]** Multi-Master — CRDT, LWW, conflict resolution
- **[J12]** Sharding — range/hash/directory, hot shards, resharding
- **[J11]** Consistent Hashing — virtual nodes, redistribution minimale

## Phase 2 — Consensus & Tolérance aux pannes (jours 6–10)

- **[J10]** Gossip Protocol — dissémination épidémique, O(log N), convergence
- **[J09]** Quorum — W+R>N, read repair, anti-entropy
- **[J08]** CAP Theorem — partition simulation, CP vs AP démontré
- **[J07]** Raft — leader election, log replication, split-brain impossible
- **[J06]** Bully Election — O(N²) messages, re-élection automatique

## Phase 1 — Fondations (jours 1–5)

- **[J05]** Idempotence — deduplication, exactly-once, retry safe
- **[J04]** Heartbeat — φ-accrual, timeout adaptatif, faux positifs
- **[J03]** Lamport Clocks — happens-before, vector clocks, causalité
- **[J02]** Sérialisation — JSON vs Protobuf vs MessagePack benchmark
- **[J01]** gRPC + Protobuf — streaming bidirectionnel, intercepteurs

## Bonus — Bases de données avancées

- **[BNS]** Query Planner — sélectivité, EXPLAIN, Nested Loop vs Hash Join
- **[BNS]** Index (B-Tree/Hash/BRIN) — voir J27a
- **[BNS]** MVCC — voir J26a
- **[BNS]** LSM Tree — voir J25a
- **[BNS]** WAL — voir J24a
