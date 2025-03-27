# Jour 24 — Write-Ahead Log (WAL) & Storage Engine

## Le problème

Une base de données écrit en mémoire (rapide) puis sur disque (lent). Si le système crashe entre les deux → état incohérent.

```
SANS WAL :
  1. Modifier pages en mémoire  ✅ rapide
  2. Écrire pages sur disque    ← CRASH ICI
  → État incohérent, données perdues

AVEC WAL :
  1. Écrire opération dans WAL  (séquentiel + fsync) ← jamais perdu
  2. Retourner "commit OK"
  3. Plus tard : écrire les pages (checkpoint)
  → Crash = rejouer le WAL → état cohérent garanti
```

## Architecture

```
Client
  │
  ▼
BEGIN tx1
  │──▶ WAL: [LSN=1, BEGIN, tx1]
WRITE key=A val=100
  │──▶ WAL: [LSN=2, WRITE, tx1, A, 100]  (buffered en mémoire)
COMMIT
  │──▶ WAL: [LSN=3, COMMIT, tx1]  ← fsync() ICI → durable
  │──▶ appliquer A=100 en mémoire
  ▼
"OK"
```

## Résultats mesurés

### Scénario 1 — Transactions ACID

```
WAL complet (13 entrées, 2038 octets) :
 LSN  Type         TX   Détails
   1  BEGIN        tx1
   2  WRITE        tx1  user:alice={nom: Alice, solde: 1000}
   3  WRITE        tx1  user:bob={nom: Bob, solde: 500}
   4  WRITE        tx1  config:max=100
   5  COMMIT       tx1  ← durable
   6  BEGIN        tx2
   7  WRITE        tx2  user:alice={solde: 0}
   8  WRITE        tx2  user:carol=...
   9  ABORT        tx2  ← annulé, invisible à la lecture
  10  BEGIN        tx3
  11  WRITE        tx3  user:alice={solde: 800}   (virement -200)
  12  WRITE        tx3  user:bob={solde: 700}     (virement +200)
  13  COMMIT       tx3

user:alice = {solde: 800}   ← TX1 puis TX3 appliquées
user:carol = None           ← TX2 abortée, invisible ✅
```

### Scénario 2 — Crash recovery

```
Avant crash (mémoire) :    compte:A=1000, compte:B=500, compte:C=300
TX2 : WRITE A=800, WRITE B=700  ← pas de COMMIT (crash)

WAL (10 entrées) :
  LSN 1-4  : TX1 (commit)  → compte:A=1000, compte:B=500
  LSN 5-7  : TX2 (pas de commit !)
  LSN 8-10 : TX3 (commit)  → compte:C=300

Après recovery :
  compte:A = 1000 ✅  (TX1 commitée, TX2 ignorée)
  compte:B =  500 ✅  (TX1 commitée, TX2 ignorée)
  compte:C =  300 ✅  (TX3 commitée)
  TX2 annulée silencieusement ✅
```

### Scénario 3 — Checkpoint

```
WAL avant checkpoint : 30 entrées
Checkpoint LSN=30 : snapshot de 10 clefs sauvegardé sur disque
5 transactions supplémentaires → 16 entrées après checkpoint

Crash → recovery :
  Charger snapshot (10 clefs) + rejouer 16 entrées (pas 46)
  Recovery en 1.1ms  ✅
  15/15 clefs récupérées ✅
```

### Scénario 4 — Corruption CRC

```
WAL légitime : 798 octets, 6 entrées
Corruption injectée à l'offset 399 (flip de 2 bytes)

❌ Corruption détectée à l'offset 396 : CRC 979558510 ≠ 3543987239

Entrées valides : 3   Corrompues : 1
→ Recovery s'arrête avant les données fausses ✅
→ Sans CRC : données silencieusement corrompues ❌
```

### Scénario 5 — Performance WAL vs Group Commit

```
200 écritures avec fsync individuel :
  WAL séquentiel  : 303ms  (660/s)
  Random I/O      : 434ms  (461/s)
  WAL 1.4× plus rapide (sur SSD — 100-1000× sur HDD)

Group Commit (1 seul fsync pour 200 écritures) :
  200 écritures  : 3.9ms
  Vs 200 × fsync : 303ms
  Accélération   : 78× ✅
```

## Recovery ARIES-style (simplifié)

```
1. Charger le dernier checkpoint
2. Rejouer le WAL depuis le LSN checkpoint+1
3. REDO  : appliquer les WRITE/DELETE des TX avec COMMIT
4. UNDO  : ignorer les TX sans COMMIT (abortées implicitement)
```

## Comparaison des moteurs de stockage

| Moteur | WAL | Checkpoint | Particularité |
|--------|-----|-----------|---------------|
| PostgreSQL | `pg_wal/` | `checkpoint_timeout=5min` | `archive_mode` pour réplication |
| MySQL InnoDB | `ib_logfile*` | `innodb_log_file_size` | `innodb_flush_log_at_trx_commit=1` |
| SQLite | WAL mode | Automatique | Lecture sans verrou en WAL mode |
| RocksDB | WAL + MemTable | Compaction SST | LSM Tree (Jour 25) |
| Kafka | Segment files | Retention policy | WAL distribué multi-broker |

## Lancer

```bash
python3 simulation.py
```

## Jour 25 → LSM Tree & SSTables (RocksDB / Cassandra)

Le WAL garantit la **durabilité**. Mais comment stocker et lire efficacement des millions de clés sur disque ? Le LSM Tree (Log-Structured Merge Tree) écrit d'abord en mémoire (**MemTable**), puis compacte sur disque en fichiers **SSTable immutables**. Les lectures utilisent des **Bloom filters** (test d'appartenance probabiliste O(1)) pour éviter de lire les SSTables qui ne contiennent pas la clef.
