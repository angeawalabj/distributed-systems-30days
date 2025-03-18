# Jour 18 — Distributed Tracing (Jaeger/Zipkin)

## Le problème

Une requête `/checkout` prend 2.3 secondes. Elle touche 5 microservices. Sans tracing : logs dans 5 endroits différents, aucune corrélation, impossible de savoir lequel est lent.

## La solution : propager un trace_id

```
Client ──── POST /checkout ────────────────────────────────────────────
              traceparent: 00-abc123-span1-01
                 │
                 ▼
            api-gateway  [span: POST /checkout, 632ms]
               │  traceparent: 00-abc123-span2-01
               ├──────────────────────────────────────
               ▼                                      ▼
          cart-svc [span, 181ms]            payment-svc [span, 431ms]
               │  traceparent: 00-abc123-span3-01
               ▼
         inventory-svc [span, 120ms]
```

Chaque service crée un **span** avec `trace_id` + `parent_id`. Le collecteur (Jaeger/Zipkin) reconstitue l'arbre complet.

## Standard W3C Trace Context

```
traceparent: 00-{trace_id}-{parent_span_id}-{flags}
             00-2ecd20c3f8e04890a32b547c0971ffe8-dcf94c192b7f493b-01
              ↑  ←────── 32 hex chars ──────→  ←── 16 hex ──→  ↑
            version                                           sampled
```

## Résultats mesurés

### Scénario 1 — Cascade /checkout (cascade style Jaeger UI)

```
Trace 2ecd20c3...  duree_totale=632ms

✅ api-gateway/POST /checkout          632ms  ████████████████████████████████████████████████████████████
  ✅ cart-svc/GET /cart/{user}         181ms   █████████████████
    ✅ inventory-svc/CHECK_STOCK       120ms         ███████████
  ✅ payment-svc/CHARGE_CARD           431ms                     ████████████████████████████████████████
    ✅ notif-svc/SEND_EMAIL             80ms                                                      ███████

Goulot : payment-svc (431ms sur 632ms = 68% du temps total)
```

### Scénario 2 — Spans parallèles

```
Séquentiel (521ms) :
  ✅ payment-svc/PROCESS_ORDER          521ms  ████████████████████████████████████████████████████████████
    ✅ inventory-svc/CHECK_STOCK        121ms  █████████████
    ✅ fraud-svc/FRAUD_ANALYSIS         400ms               ██████████████████████████████████████████████

Parallèle (403ms) :
  ✅ payment-svc/PROCESS_ORDER_PARALLEL 402ms  ████████████████████████████████████████████████████████████
    ✅ inventory-svc/CHECK_STOCK        120ms  █████████████████
    ✅ fraud-svc/FRAUD_ANALYSIS         401ms  ███████████████████████████████████████████████████████████

Gain : 119ms (23% plus rapide) ✅
```

### Scénario 3 — Détection d'erreur

```
Panne dans inventory-svc :
  ✅ api-gateway/handle_request          15ms  ██████████
    ✅ cart-svc/handle_request           40ms             █████████████████████████████
      ❌ inventory-svc/handle_request    27ms                                          ███████████████████

Diagnostic : ❌ inventory-svc : "DB connection timeout in inventory-svc"
→ Rootcause identifiée en 1 coup d'œil, pas de log-grep dans 5 fichiers
```

### Scénario 4 — P50/P95/P99 sur 20 requêtes

```
Service                P50     P95     P99     Max   Erreurs
payment-svc           354ms  1012ms  1012ms  1012ms     0   ← GOULOT
inventory-svc         111ms   387ms   387ms   387ms     0
notif-svc              74ms   239ms   239ms   239ms     1
cart-svc               42ms   130ms   130ms   130ms     1
api-gateway            16ms    41ms    41ms    41ms     0
```

### Scénario 5 — Sampling adaptatif

```
200 requêtes simulées :
  Sans sampling  : 200 traces stockées (100%)
  Avec sampling  :  45 traces stockées  (22%)
    Erreurs (always-keep) : 6
    Lents > 100ms         : 22
    Aléatoires (10%)      : ~20

À 10 000 req/s : 2 250 spans/s stockés au lieu de 50 000 (réduction 78%)
Règle d'or : 100% des erreurs toujours gardées.
```

## Head sampling vs Tail sampling

```
Head sampling :
  Décision à la naissance (avant d'avoir les données).
  ✅ Simple, pas de buffer
  ❌ Peut manquer une trace qui devient intéressante (erreur en fin de chaîne)

Tail sampling :
  Décision après avoir vu la trace complète.
  ✅ Garde toujours les traces avec erreurs ou lentes
  ❌ Nécessite un buffer (garder N secondes de traces en attente)
  Utilisé par : OpenTelemetry Collector, Jaeger Agent
```

## Comparaison des outils

| Outil | Origine | Points forts |
|-------|---------|-------------|
| **Jaeger** | Uber / CNCF | UI riche, Adaptive Sampling, multi-backend |
| **Zipkin** | Twitter | Simple, excellent support Spring Boot/Brave |
| **Grafana Tempo** | Grafana Labs | Intégré Loki+Prometheus, stockage objet |
| **Datadog APM** | Datadog | Auto-instrumentation, corrélation logs/traces |
| **OpenTelemetry** | CNCF | Standard universel, vendor-neutral |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `tracing.py` | `Span`, `ContexteTrace`, `Tracer`, `Collecteur`, `MicroService` |
| `simulation.py` | 5 scénarios : cascade, parallèle, erreur, latence, sampling |

## Lancer

```bash
python3 simulation.py
```

## Jour 19 → Event Sourcing

On sait maintenant **observer** ce qui se passe dans les services. Jour 19 change la façon dont l'état est **stocké**. Au lieu de `UPDATE accounts SET balance=900`, on enregistre `{event: "DEBIT", amount: 100, ts: ...}`. L'état courant est reconstruit en rejouant tous les événements depuis le début — comme Git reconstruit HEAD depuis les commits.
