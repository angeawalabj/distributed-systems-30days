# Jour 16 — Load Balancing : Couche 4 vs Couche 7

## Le problème

Un seul serveur ne peut pas absorber 1M requêtes/seconde. Il faut répartir le trafic sur N backends. Mais comment choisir lequel ? Et avec quelle intelligence ?

## L4 vs L7 — La différence fondamentale

```
Requête HTTP :
  TCP: src=10.0.1.5:54321  dst=10.0.2.1:443     ← L4 voit ça
  TLS: [chiffré]
  HTTP: GET /api/v2/users HTTP/1.1               ← L7 voit ça aussi
        Host: api.example.com
        Authorization: Bearer eyJ...
        X-User-Id: 42
```

**L4** (HAProxy TCP, AWS NLB, LVS) : ne voit que les métadonnées réseau. Ultra-rapide, pas de parsing.

**L7** (Nginx, Envoy, AWS ALB) : inspecte tout le contenu HTTP. Plus lent mais infiniment plus puissant.

## Algorithmes de répartition

### Résultats mesurés (300 requêtes, 3 backends : 5ms / 15ms / 35ms)

```
Algorithme            CV%    Lat moy   Distribution
Round Robin            0%    19.6ms   fast:100  medium:100  slow:100
Random                 1%    18.8ms   fast:101  medium:99   slow:100
Least Connections     12%    17.7ms   fast:116  medium:97   slow:87
Power of Two           4%    18.6ms   fast:105  medium:98   slow:97
```

**Observations** :
- Round Robin distribue parfaitement mais ignore que `slow` prend 7x plus de temps que `fast`
- Least Connections s'adapte : plus de requêtes vers `fast` (CV 12% acceptable)
- Power of Two : quasi-optimal en choisissant le moins chargé parmi 2 aléatoires — O(1) comparaisons

### Quand utiliser quoi

| Algo | Utiliser si |
|------|-------------|
| Round Robin | Backends identiques (même latence, même capacité) |
| Least Connections | Backends hétérogènes ou requêtes longues |
| Power of Two | Backends hétérogènes + très haute charge |
| IP Hash | Sessions applicatives sans Redis |
| Weighted RR | Migration : ancien serveur à 30%, nouveau à 70% |

## Scénarios mesurés

### Scénario 2 — Path-based routing L7
```
/api/v1/*  → java-{1,2,3}    lat=29.9ms
/api/v2/*  → go-{1,2,3}      lat=10.6ms   ← 3x plus rapide
/static/*  → cdn-{1,2}       lat= 3.6ms
default    → fallback         lat=52.7ms

L4 aurait tout envoyé au même pool sans distinction.
```

### Scénario 3 — Canary Deployment
```
500 requêtes avec hash(user-id) % 10 == 0 → v2 :

v2 (canary) : 57/500   (11.4%)   lat=15.9ms  ✅ plus rapide
v1 (prod)   : 443/500  (88.6%)   lat=20.4ms

Rollout progressif : 0% → 1% → 10% → 50% → 100%
```

### Scénario 4 — Health checks et failover
```
Phase 1 (tous sains)    : L4 lat=10.6ms   L7 lat=10.2ms
backend-2 tombe, backend-3 dégradé (5x plus lent)
Phase 2 (après pannes)  : L4 lat=31.3ms   L7 lat=10.5ms

L4 : envoie encore vers backend-3 (port ouvert) → latence x3
L7 : Least Connections dé-priorise backend-3 → 100% sur backend-1
```

### Scénario 5 — Rate limiting + Sticky sessions
```
Rate limit (5 req/s par IP) — 20 requêtes en rafale :
  200 OK      :  5
  429 Limited : 15  ✅

Sticky sessions (IP Hash) :
  10.0.0.1 → app-1 → app-1  ✅
  10.0.0.2 → app-3 → app-3  ✅
  10.0.0.3 → app-3 → app-3  ✅
  (toujours le même backend pour la même IP)
```

## Tableau comparatif final

| Fonctionnalité | L4 | L7 |
|---|---|---|
| Vitesse | ✅ Ultra-rapide | ⚠️ Légèrement plus lent |
| TLS termination | ❌ | ✅ |
| Routage par URL | ❌ | ✅ |
| Canary deployment | ❌ | ✅ |
| Rate limiting | ❌ | ✅ |
| Health check applicatif | ❌ (TCP only) | ✅ (/health → 200) |
| Sticky par cookie | ❌ | ✅ |
| Circuit breaking | ❌ | ✅ |

## Utilisé en production

| Produit | Couche | Particularité |
|---------|--------|---------------|
| **AWS NLB** | L4 | Millions de req/s, latence <1ms |
| **AWS ALB** | L7 | Routage par path, header, query |
| **Nginx** | L7 | Reverse proxy + LB le plus répandu |
| **HAProxy** | L4 + L7 | Référence open-source |
| **Envoy** | L7 | Service mesh (Istio, Consul Connect) |
| **Traefik** | L7 | Kubernetes-native, auto-découverte |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `load_balancer.py` | `BackendServer`, `LoadBalancerL4`, `LoadBalancerL7`, `RegleRoutage` |
| `simulation.py` | 5 scénarios : algos, path routing, canary, failover, rate limit |

## Lancer

```bash
python3 simulation.py
```

## Jour 17 → Service Discovery (Consul/Etcd)

Le load balancer sait router vers des backends — mais comment connaît-il leurs adresses IP ? Dans un cluster Kubernetes, chaque Pod redémarré obtient une nouvelle IP. Le **Service Discovery** permet aux services de s'enregistrer dynamiquement et de se retrouver sans configuration statique. Consul et etcd implémentent un registre distribué avec health checks intégrés.
