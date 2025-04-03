# Jour 27 — Distributed Cache : "La Mémoire Collective"

## Le problème

Une requête typique sans cache :

```
SELECT user FROM users WHERE id=123      → 15ms
SELECT perms FROM permissions WHERE ...  → 12ms
SELECT config FROM settings              → 8ms
Total                                    → 35ms × toutes les requêtes

Avec 10 000 req/s → 350 000ms de travail base/seconde
→ Base saturée → timeouts → panne en cascade
```

## La solution

```
Première requête : miss → lire la base (35ms) → stocker en cache
Requêtes suivantes : hit → lire le cache (< 1ms) → résultat immédiat

99% des requêtes évitent la base.
La base ne voit que les 1% restants.
```

## Les 4 patterns

### Cache-Aside (lazy loading)

```
get(clef) :
  val = cache.get(clef)
  if val: return val              ← hit
  val = base.read(clef)           ← miss
  cache.set(clef, val, ttl=300)
  return val

Écriture : base.write() → cache.delete(clef)  ← invalider, pas mettre à jour
```

### Write-Through

```
set(clef, val) :
  base.write(clef, val)    ← toujours en premier
  cache.set(clef, val)     ← puis le cache

→ Cache toujours cohérent avec la base
→ Écriture 2× plus lente
```

### Write-Behind (write-back)

```
set(clef, val) :
  cache.set(clef, val)     ← écriture ultra-rapide
  queue.push(clef, val)    ← flush asynchrone vers la base

→ Latence d'écriture minimale
→ Risque de perte si crash avant le flush
```

## Résultats mesurés

### Scénario 1 — Cache-Aside, distribution Zipf

```
200 lectures (20% des users = 80% des requêtes) :

Métrique                        Sans cache      Avec cache
────────────────────────────────────────────────────────
Temps total                         4119ms        1709ms
Appels base de données               200             82
Lectures cache (hit)                   —            118
Hit rate                               —          59.0%
Gain de vitesse                       1×           2.4×

→ 2.4× en régime de warm-up.
  En régime permanent (cache chaud) : > 10× car misses → quasi 0.
```

### Scénario 2 — Invalidation

```
user:42 initial : {nom: 'User_42', ...}

Écriture base → nom: 'Alice_Nouveau'

SANS invalidation :
  Cache retourne : {nom: 'User_42'}    ← ❌ STALE DATA

Après invalidation explicite :
  Cache retourne : {nom: 'Alice_Nouveau'}  ✅

Invalidation pattern '*:42' :
  Clefs supprimées : 2 (user:42 ET perm:42 ensemble)

Règle d'or : 1) écrire base  2) invalider cache  (jamais l'inverse)
```

### Scénario 3 — Stampede (thundering herd)

```
50 threads simultanés sur 'hot:key' expirée :

Métrique                      Sans protection   Avec mutex
──────────────────────────────────────────────────────────
Appels base de données                   50            1
Résultats distincts retournés            47            1   ← cohérence ✅

Stats anti-stampede : {recalculs: 1, attentes: 49}

→ 49 threads attendent le résultat du 1er → 1 seul appel base
→ Sans mutex : 50 appels base + race condition (47 valeurs différentes !)
```

### Scénario 4 — Cache L1/L2

```
300 requêtes (hot keys = config:global, user:1, user:2) :

Source     Requêtes     %   Latence
────────────────────────────────────────────
L1            237      79%  < 0.01ms  (mémoire locale)
L2              0       0%  ~ 0.5ms   (Redis)
DB             43      14%  ~ 20ms    (base)

Temps total : 893ms (vs 300 × 20ms = 6000ms sans cache)

Invalidation pub/sub :
  Écriture user:1 → Redis delete → pub/sub → L1 invalidé ['user:1'] ✅
```

### Scénario 5 — Cache warming + éviction LRU

```
Capacité cache : 20 entrées

SANS warming (50 requêtes au démarrage) :
  Misses : 50/50 — Temps : 540ms — Appels base : 50

AVEC warming (top 10 clefs pré-chargées) :
  Warming : 10 clefs en 108ms
  Misses  : 41/50 — Temps : 444ms

Éviction LRU (capacité=5) :
  10 insertions → taille=5, évictions=5
```

## Problèmes classiques et solutions

| Problème | Symptôme | Solution |
|----------|----------|----------|
| Stampede | 1 clef expire → surcharge base | Mutex par clef (`SET NX`) |
| Stale data | Lecture après écriture → ancienne valeur | Invalider sur écriture |
| Hot key | 1 shard Redis saturé | Cache local L1 devant L2 |
| Cold start | 100% misses au démarrage | Cache warming (top-N clefs) |
| OOM | Redis mange toute la RAM | `maxmemory-policy allkeys-lru` |

## Configuration Redis production

```bash
# Limiter la mémoire + éviction automatique
config set maxmemory 4gb
config set maxmemory-policy allkeys-lru

# Monitoring lag
redis-cli info stats | grep evicted_keys
redis-cli info keyspace
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `cache.py` | `CacheLocal`, `CacheDistribue`, `CacheAside`, `AntiStampede` |
| `simulation.py` | 5 scénarios : cache-aside, invalidation, stampede, L1/L2, warming |

## Lancer

```bash
python3 simulation.py
```

## Jour 28 → Chaos Engineering

L'infrastructure est construite : HDFS, Kafka, Flink, Zero-Trust, SPIFFE, Gateway, Cache.  
Comment savoir qu'elle résiste vraiment aux pannes ?  
Chaos Engineering = injecter des pannes contrôlées → mesurer la résilience → corriger les faiblesses avant que la production ne le fasse.
