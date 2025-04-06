# Jour 30 — TrueTime & Spanner : "La Synthèse Finale"

## Le défi

```
CAP theorem (Brewer, 2000) :
  "Vous ne pouvez pas avoir C + A + P simultanément."

Google Spanner (2012) :
  "Tenez mon café."

Comment ? En résolvant le vrai problème sous-jacent : le TEMPS.
```

## Le problème fondamental du temps distribué

Dans un système distribué, **"maintenant" n'existe pas.** Chaque machine a sa propre horloge, légèrement décalée. Si deux transactions s'exécutent "en même temps" sur deux continents, laquelle vient en premier ?

```
NTP (Network Time Protocol) :
  Précision : ±100ms sur WAN, ±10ms sur LAN
  Insuffisant : deux transactions à 50ms d'intervalle sont indiscernables

Solution Google : TrueTime
  GPS + horloges atomiques (rubidium) dans chaque datacenter
  API : TrueTime.now() → [earliest, latest]  (intervalle, pas un instant)
  Epsilon garanti : ±4ms

  "Je ne sais pas exactement quelle heure il est,
   mais je garantis que c'est entre earliest et latest."

  Une certitude imparfaite mais garantie bat une précision illusoire.
```

## External Consistency

```
Si T1 committe AVANT que T2 démarre → ts(T1) < ts(T2)
                                       GARANTI, sur toute la planète.

C'est plus fort que la sérialisabilité classique :
  Sérialisabilité : ordre ÉQUIVALENT à un ordre séquentiel
  External consistency : ordre = l'ordre RÉEL du temps causal
```

## Algorithme : Commit Wait

```
1. Acquérir ts_commit = TrueTime.now().latest  (borne haute)
2. Proposer au Paxos de chaque shard concerné
3. COMMIT WAIT : bloquer jusqu'à TrueTime.now().earliest > ts_commit
4. Appliquer les écritures avec ts_commit

Pourquoi ça marche :
  Toute transaction future T2 acquiert son ts_lecture APRÈS l'attente.
  Or TrueTime garantit : ts_lecture_T2 >= vrai_temps >= ts_commit
  Donc ts(T1) < ts(T2) — irréfutablement.

Coût : commit wait ≈ 2×ε ≈ 8ms
```

## Architecture Spanner

```
Shard (jour 12)     → scalabilité horizontale
MVCC  (jour 26)     → lectures snapshot sans verrou
Paxos (≈ Raft j7)   → consensus au sein de chaque shard
TrueTime            → ordre global cohérent entre shards

Chaque shard = groupe Paxos de 5 répliques
  3 zones de disponibilité (us-east, us-central, us-west)
  2 répliques read-only dans 2 autres régions (stale reads rapides)
  Quorum = 3/5 → tolère 2 pannes simultanées par shard
```

## Résultats mesurés

### Scénario 1 — TrueTime

```
Impact de l'epsilon :
  NTP WAN          ±100ms  → 2 tx à 50ms indiscernables
  NTP LAN          ±10ms   → encore insuffisant
  GPS seul         ±5ms    → mieux
  TrueTime         ±4ms    → Spanner production
  TrueTime optimisé±2ms    → objectif futur

Ordre garanti (écart 20ms > 2×ε) : True ✅
Ordre ambigu  (écart 3ms < 2×ε)  : chevauchement, sérialisation nécessaire

Commit wait mesuré : 8.57ms  (cible ~8ms = 2×ε) ✅
```

### Scénario 2 — MVCC Time Travel

```
5 versions de compte:alice sur 75ms :
  v1: alice=1000  v2: alice=850  v3: alice=920  v4: alice=780  v5: alice=950

Time travel → lire_snapshot(t₂) = 850  ✅
Invariant   → alice + bob = 5000 à chaque version  ✅

Snapshot isolation : 2 reports lisent alice=920 même pendant une écriture concurrente ✅
Stale read analytics : latence 10-40ms → 1-5ms (aucun round-trip leader Paxos)
```

### Scénario 3 — Résilience Paxos

```
Panne 1 réplique/shard (4/5) : quorum maintenu → écriture OK ✅
Panne 3 répliques shard-0    : quorum perdu (2/5) → écriture KO, comportement CP ❌
Récupération répliques       : 3/3 shards → OK ✅

Topologie production :
  Panne 1 zone (2 répliques) → quorum 3/5 maintenu → 0 downtime
  Panne 2 zones (4 répliques)→ writes bloquées (CP mode)
```

### Scénario 4 — External Consistency

```
TrueTime Europe (+2ms dérive)  ×  TrueTime USA (-1.5ms dérive)

T1 (Europe) commité à ts₁, durée 8.7ms
T2 (USA)    commité à ts₂, durée 8.8ms

ts(T1) < ts(T2) malgré la dérive d'horloge : OUI ✅

Time travel entre T1 et T2 :
  alice=900  bob=1100  carol=None
  → T1 visible, T2 pas encore commité
```

### Scénario 5 — 4 virements concurrents

```
alice=10000  bob=5000  carol=3000  dave=8000

T1: alice→bob 500€   T2: bob→carol 200€
T3: dave→alice 300€  T4: carol→dave 100€

4/4 transactions committées avec timestamps ordonnés ✅
→ Ordre de commit déterministe et auditable
```

## Spanner vs les autres

| Système | Consistency | Dispo | Latence commit |
|---------|-------------|-------|----------------|
| Cassandra | Eventual | Haute | ~1ms |
| MongoDB | Eventual/Strong | Haute | ~2ms |
| CockroachDB | Linearizable | Haute | ~10ms |
| **Spanner** | **External** | **99.999%** | **~8ms** |
| Single DB | ACID | SPOF | ~1ms |

Spanner n'a pas violé CAP. Il a rendu l'incertitude temporelle **bornée et garantie**, ce qui suffit à sérialiser les transactions de façon globalement cohérente.

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `spanner.py` | `TrueTime`, `IntervalleTemps`, `StockageMVCC`, `GroupePaxos`, `Transaction`, `SpannerDB` |
| `simulation.py` | 5 scénarios : TrueTime, MVCC time travel, Paxos résilience, external consistency, synthèse |

## Lancer

```bash
python3 simulation.py
```

---

## La leçon fondamentale des 30 jours

> *"Un système distribué est un système dans lequel la panne d'un ordinateur dont tu n'avais même pas conscience de l'existence rend ton propre ordinateur inutilisable."*
> — Leslie Lamport

Il n'y a pas de solution parfaite. Il y a des **trade-offs** :

| Trade-off | Jours | Exemple |
|-----------|-------|---------|
| Cohérence vs Disponibilité | 7, 8, 9 | Raft (CP) vs Cassandra (AP) |
| Latence vs Consistance | 30 | Commit wait 8ms pour external consistency |
| Throughput vs Durabilité | 24 | WAL fsync : sync vs async |
| Flexibilité vs Sécurité | 24, 25, 26 | Zero-Trust : tout vérifier |
| Observabilité vs Complexité | 29 | 3 piliers + instrumentation |

Ces trade-offs, maintenant, tu les connais. Tu peux les **nommer**, les **mesurer**, et **choisir** en connaissance de cause. C'est ce que 30 jours de systèmes distribués t'ont donné.
