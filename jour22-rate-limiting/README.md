# Jour 22 — Rate Limiting & Throttling

## Le problème

Un client envoie 10 000 req/s. Votre base sature. Les autres clients souffrent. La facture cloud explose.

## Les 4 algorithmes

### 1. Fixed Window — Simple, mais exploitable

```
[──── fenêtre 1s ────][──── fenêtre 1s ────]
 req 1..10 autorisées  req 1..10 autorisées
       ↑                       ↑
   t=0.99s                 t=1.01s
   ←──────── 20 req en 20ms ────────→ = 2× la limite !
```

**Boundary burst attack** : envoyer N requêtes juste avant la fin de fenêtre, puis N juste après le reset.

### 2. Sliding Window Log — Précis, coûteux

Garde le timestamp de chaque requête. Compte celles dans `[now-T, now]`. Aucun burst possible. O(N) mémoire.

### 3. Token Bucket — **Le standard** (AWS, Stripe, GitHub)

```
Capacité=20, recharge=5/s

t=0s  : seau plein → 20 req autorisées d'un coup (burst légal)
t=1s  : 5 jetons rechargés → 5 req autorisées
t=3s  : 10 jetons rechargés → 10 req autorisées

"économiser" des jetons pour un burst ponctuel = comportement VOULU
```

### 4. Leaky Bucket — Débit constant (Nginx)

Les requêtes entrent dans une file, sortent à débit constant R/s. Lisse les bursts en entrée. Augmente la latence.

## Résultats mesurés

### Scénario 1 — 25 requêtes en burst (limite=10)

```
Algorithme       ✅ OK   ❌ Refus  Séquence
Fixed Window       10       15   ✅✅✅✅✅✅✅✅✅✅❌❌❌❌❌...
Sliding Log        10       15   ✅✅✅✅✅✅✅✅✅✅❌❌❌❌❌...
Token Bucket       10       15   ✅✅✅✅✅✅✅✅✅✅❌❌❌❌❌...
Leaky Bucket       11       14   ✅✅✅✅✅✅✅✅✅✅✅❌❌❌❌...
```

### Scénario 2 — Boundary burst attack

```
                     Moment      Fixed Window    Sliding Log
Lot 1 (fin fen.)    t=0.99s    10 autorisées   10 autorisées
Lot 2 (début fen.)  t=1.01s    10 autorisées    0 autorisées
TOTAL               (~10ms)    20 autorisées   10 autorisées

Fixed Window : 20 req en ~10ms = 2× la limite ❌
Sliding Log  : 10 req en ~10ms = exactement la limite ✅
```

### Scénario 3 — Token Bucket (capacité=20, recharge=5/s)

```
Burst initial    20 req  → 20 ✅  0 ❌   (seau plein)
--- pause 1s ---                          5.0 jetons rechargés
Après 1s          8 req  →  5 ✅  3 ❌
--- pause 2s ---                         10.0 jetons rechargés
Après 2s         15 req  → 10 ✅  5 ❌
```

### Scénario 4 — Multi-tenant

```
Client              Demandes   ✅ OK   ❌ Refus   Taux OK
alice (free)            25       10       15       40%
bob (pro)              150      100       50       67%
corp (enterprise)      500      500        0      100%

alice abusive → bloquée. bob et corp : isolation totale.
```

### Scénario 5 — Headers 429

```
Req   Status   Remaining   Retry-After
  1      200           4           —    ✅
  2      200           3           —    ✅
  3      200           2           —    ✅
  4      200           1           —    ✅
  5      200           0           —    ✅
  6      429           0         0.5s   ❌ attendre 0.5s
  7      429           0         0.5s   ❌
  8      429           0         0.5s   ❌
```

## Comparaison des algorithmes

| | Fixed Window | Sliding Log | Token Bucket | Leaky Bucket |
|---|---|---|---|---|
| Mémoire | O(1) | O(N) | O(1) | O(1) |
| Burst | ❌ 2× en bordure | ✅ Non | ✅ Contrôlé | ✅ Lissé |
| Précision | Moyen | Excellente | Bonne | Bonne |
| Complexité | Simple | Moyen | Simple | Simple |
| Usage | Cache, compteurs | APIs sensibles | **APIs publiques** | Nginx, backend |

## En production

| Outil | Algorithme | Notes |
|-------|-----------|-------|
| **Redis + Lua** | Token Bucket | Script atomique, distribué |
| **Nginx** | Leaky Bucket | `limit_req_zone $binary_remote_addr` |
| **AWS API Gateway** | Token Bucket | Par clef d'API, configurable |
| **Cloudflare** | Sliding Window Counter | Approximation O(1) de Sliding Log |
| **Kong / Envoy** | Configurable | Plugins par route |

## Lancer

```bash
python3 simulation.py
```

## Jour 23 → Backpressure & Queue Management

Le Rate Limiter répond au problème **côté serveur** en rejetant le surplus. Le **Backpressure** le résout différemment : au lieu de rejeter, on **signale la pression** en amont pour que le producteur ralentisse. Bounded queues, work stealing, priority queues, load shedding avec rejection sampling.
