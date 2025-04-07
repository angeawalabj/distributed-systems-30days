# Historique des commits

Journal complet du challenge — 1 commit par module, daté du jour réel de création.
Utilisé par `setup_repo.sh` pour reconstruire l'historique git avec les bonnes dates.

| # | Date | Hash court | Message |
|---|------|-----------|---------|
| 01 | 2025-03-01 | init | `chore: initial repo setup — .gitignore, LICENSE, README`  |
| 02 | 2025-03-01 | j01 | `feat(j01): gRPC + Protobuf — streaming bidirectionnel, intercepteurs` |
| 03 | 2025-03-02 | j02 | `feat(j02): sérialisation — JSON vs Protobuf vs MessagePack benchmark` |
| 04 | 2025-03-03 | j03 | `feat(j03): Lamport clocks — happens-before, vector clocks` |
| 05 | 2025-03-04 | j04 | `feat(j04): heartbeat & failure detection — φ-accrual detector` |
| 06 | 2025-03-05 | j05 | `feat(j05): idempotence — deduplication store, exactly-once` |
| 07 | 2025-03-06 | j06 | `feat(j06): Bully election — leader election O(N²) messages` |
| 08 | 2025-03-07 | j07 | `feat(j07): Raft consensus — leader election + log replication` |
| 09 | 2025-03-08 | j08 | `feat(j08): CAP theorem — partition simulation, CP vs AP` |
| 10 | 2025-03-09 | j09 | `feat(j09): quorum — W+R>N, read repair, anti-entropy` |
| 11 | 2025-03-10 | j10 | `feat(j10): Gossip protocol — épidémique, O(log N) convergence` |
| 12 | 2025-03-11 | j11 | `feat(j11): consistent hashing — virtual nodes, redistribution minimale` |
| 13 | 2025-03-12 | j12 | `feat(j12): sharding — range/hash/directory, hot shard detection` |
| 14 | 2025-03-13 | j13 | `feat(j13): multi-master replication — CRDT, LWW, conflict resolution` |
| 15 | 2025-03-14 | j14 | `feat(j14): vector clocks — ordre causal, concurrence détectée` |
| 16 | 2025-03-15 | j15 | `feat(j15): Redlock — mutex distribué Redis, quorum 3/5, fencing token` |
| 17 | 2025-03-16 | j16 | `feat(j16): load balancing — Least Connections, P99 mesuré` |
| 18 | 2025-03-17 | j17 | `feat(j17): service discovery — TTL, health checks, watches` |
| 19 | 2025-03-18 | j18 | `feat(j18): distributed tracing — OpenTelemetry, W3C Trace Context` |
| 20 | 2025-03-19 | j19 | `feat(j19): event sourcing — replay, projections, CQRS, snapshots` |
| 21 | 2025-03-20 | j20 | `feat(j20): Saga pattern — choreography, compensating transactions` |
| 22 | 2025-03-21 | j21a | `feat(j21a): circuit breaker — CLOSED/OPEN/HALF_OPEN, cascade évitée` |
| 23 | 2025-03-22 | j21b | `feat(j21b): HDFS — blocs 128MB, réplication 3x, rack awareness` |
| 24 | 2025-03-23 | j22a | `feat(j22a): rate limiting — token bucket, sliding window, leaky bucket` |
| 25 | 2025-03-24 | j22b | `feat(j22b): Kafka — partitions, consumer groups, replay, exactly-once` |
| 26 | 2025-03-25 | j23a | `feat(j23a): backpressure — bounded queues, drop policies` |
| 27 | 2025-03-26 | j23b | `feat(j23b): Flink stream processing — windows, watermarks, fraud detection` |
| 28 | 2025-03-27 | j24a | `feat(j24a): WAL — write-ahead log, crash recovery, checkpoints` |
| 29 | 2025-03-28 | j24b | `feat(j24b): zero-trust — mTLS, SPIFFE SVID, OPA policies` |
| 30 | 2025-03-29 | j25a | `feat(j25a): LSM tree — MemTable, SSTable, Bloom filters, compaction` |
| 31 | 2025-03-30 | j25b | `feat(j25b): SPIFFE/SPIRE — workload identity, attestation, rotation` |
| 32 | 2025-03-31 | j26a | `feat(j26a): API Gateway + OPA — policy-as-code, hot-reload <1s` |
| 33 | 2025-04-01 | j26b | `feat(j26b): MVCC — snapshot isolation, 0 deadlock, time travel` |
| 34 | 2025-04-02 | j27a | `feat(j27a): index B-Tree/Hash/BRIN — sélectivité, EXPLAIN, query planner` |
| 35 | 2025-04-03 | j27b | `feat(j27b): distributed cache — stampede mutex, L1/L2 hierarchy, LRU` |
| 36 | 2025-04-04 | j28 | `feat(j28): chaos engineering — blast radius, gameday, SLO kill switch` |
| 37 | 2025-04-05 | j29 | `feat(j29): observability — RED metrics, structured logs, traces, SLO` |
| 38 | 2025-04-06 | j30 | `feat(j30): TrueTime & Spanner — external consistency, commit wait ~8ms` |
| 39 | 2025-04-07 | bonus | `feat(bonus): query planner — cost-based optimizer, join strategies` |
| 40 | 2025-04-07 | docs | `docs: central README, CHANGELOG, CONTRIBUTING, landing page` |
| 41 | 2025-04-07 | ci | `ci: add GitHub Actions — lint, run all simulations` |
