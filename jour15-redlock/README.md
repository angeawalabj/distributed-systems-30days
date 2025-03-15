# Jour 15 — Distributed Locking : Redlock

## Le problème

Un verrou local (`threading.Lock`) ne fonctionne que dans un seul processus. Un verrou sur **une seule instance Redis** est un SPOF : si cette instance tombe ou si le réseau se partitionne, le verrou est soit perdu, soit bloqué indéfiniment.

## La solution : Redlock (Antirez, 2016)

Acquisition sur **N instances Redis indépendantes** (N impair, recommandé N=5).

### Les 6 étapes

```
1. t1 = now()
2. SET lock_key token NX PX ttl   ← sur TOUTES les instances en parallèle
3. t2 = now()
4. elapsed = t2 - t1
5. validity_time = ttl - elapsed - drift
6. Acquis si : instances_ok ≥ N/2+1  ET  validity_time > 0
   Sinon    : libérer sur TOUTES les instances (même celles KO)
```

### Libération (script Lua atomique)

```lua
if redis.call('get', key) == token then
    return redis.call('del', key)
end
```

Le `token` UUID unique empêche de libérer le verrou d'un autre client.

## Résultats mesurés

### Scénario 1 — Acquisition nominale
```
5 instances, quorum=3, TTL=5000ms

✅ acquis=True  validity=4936ms  durée=11.7ms  fencing=1
   OK : ['redis-1', 'redis-2', 'redis-3', 'redis-4', 'redis-5']

TTL restant après 100ms de section critique : ~4895ms sur chaque instance
Après libération : toutes les instances → (vide) ✅
```

### Scénario 2 — Exclusion mutuelle
```
client-A et client-B tentent simultanément (2ms d'écart) :

+   0ms  client-A → ✅ ACQUIS  (fencing=2)
+   0ms  client-A → travaille 200ms...
+ ~10ms  client-B → ❌ REFUSÉ  (fencing=0)
+ 200ms  client-A → verrou libéré

Clients ayant acquis : 1/2  ✅ exclusion mutuelle respectée
```

### Scénario 3 — Tolérance aux pannes
```
Config        Instances down    Acquis   Validity
0 panne       (aucune)          ✅ oui   4941ms
1 panne       redis-2           ✅ oui   4942ms
2 pannes      redis-2, redis-4  ✅ oui   4941ms  ← limite
3 pannes      redis-2,3,4       ❌ non      0ms

Redlock tolère ⌊(N-1)/2⌋ = 2 pannes simultanées (même règle que Raft).
```

### Scénario 4 — GC Pause + Fencing Token
```
+   5ms  client-A acquiert verrou  TTL=300ms  fencing=6
+   5ms  client-A entre en GC PAUSE (400ms)
+  362ms client-B acquiert verrou  (TTL de A expiré)  fencing=7
+  405ms client-A sort de GC pause, écrit avec fencing=6
+  412ms client-B écrit avec fencing=7

Résultat final : données_B_valides (fencing 7 > 6)
Si A avait essayé d'écrire APRÈS B → rejeté (token obsolète) ✅
```

Le fencing token garantit que la **dernière écriture valide gagne** quelle que soit la temporisation réseau ou les pauses GC.

### Scénario 5 — Verrou simple vs Redlock sous partition
```
Instances down    Verrou simple (1)    Redlock (5)
0/5               ✅ acquis            ✅ acquis
1/5               ❌ échec             ✅ acquis   ← Redlock résiste
2/5               ❌ échec             ✅ acquis   ← Redlock résiste
3/5               ❌ échec             ❌ échec
```

## Le débat Kleppmann vs Antirez

**Kleppmann (2016)** : *"Redlock est unsafe. Une GC pause peut faire expirer le verrou pendant que le client croit l'avoir encore — deux clients peuvent opérer simultanément."*

**Antirez** : *"C'est vrai sans fencing token. Avec fencing token, la ressource protégée rejette les requêtes avec un token périmé."*

**Consensus** : Redlock + fencing tokens est sûr pour la grande majorité des cas. Le seul risque résiduel est que la ressource cible n'implémente pas les fencing tokens (elle doit le faire elle-même).

## Quand utiliser quoi

```
┌─────────────────────────┬──────────────────────────────────┐
│ Cas                     │ Solution recommandée             │
├─────────────────────────┼──────────────────────────────────┤
│ Service mono-instance   │ threading.Lock (trivial)         │
│ Non-critique, 1 DC      │ SET NX PX sur 1 Redis            │
│ Critique, multi-DC      │ Redlock (5 instances)            │
│ Très critique, rigueur  │ etcd ou ZooKeeper (Raft/ZAB)     │
│ Déjà sur PostgreSQL     │ SELECT FOR UPDATE (simple)       │
└─────────────────────────┴──────────────────────────────────┘
```

## Coût de Redlock

| | Verrou simple | Redlock (N=5) |
|---|---|---|
| Infrastructure | 1 Redis | 5 Redis indépendants |
| Latence acquisition | ~2ms | ~10-15ms |
| Tolérance pannes | 0 | 2 sur 5 |
| Protection GC pause | ❌ | ✅ (avec fencing token) |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `redlock.py` | `InstanceRedis`, `ClientRedlock`, `RessourceProtegee`, `ResultatVerrou` |
| `simulation.py` | 5 scénarios : nominal, exclusion, pannes, GC pause, comparaison |

## Lancer

```bash
python3 simulation.py
```

## Semaine 4 → Jour 16 : Load Balancing L4 vs L7

On sait maintenant stocker, répliquer et coordonner les accès aux données. La Semaine 4 s'attaque à la **scalabilité** : comment répartir le trafic entre plusieurs instances d'un service ? Le load balancing de **couche 4** (TCP brut) ne voit que les adresses IP et ports. Le load balancing de **couche 7** (HTTP) peut inspecter les URLs, headers, cookies — et prendre des décisions bien plus intelligentes.
