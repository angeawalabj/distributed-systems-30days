# Jour 29 — Observabilité : "L'Observateur Omniscient"

## Le problème

```
Un système distribué tombe à 14h32.
Les logs sont sur 47 machines différentes.
La requête qui a échoué a traversé 8 services.
Personne ne sait où ça a pêché ni pourquoi.

→ MTTD (Mean Time To Detect)   : 15 minutes
→ MTTR (Mean Time To Recover)  : 2 heures
→ Coût : SLO violé, clients impactés
```

## La solution : les 3 piliers de l'observabilité

```
MÉTRIQUES → "Est-ce que le système est en bonne santé ?"
            Valeurs numériques agrégées dans le temps.
            Prometheus, StatsD, OpenMetrics

LOGS      → "Qu'est-ce qui s'est passé exactement ?"
            Événements textuels structurés (JSON).
            ELK Stack (Elasticsearch + Logstash + Kibana), Loki

TRACES    → "Comment une requête a traversé le système ?"
            Graphe de causalité d'une requête end-to-end.
            OpenTelemetry, Jaeger, Zipkin
```

Ces 3 piliers sont **complémentaires**, pas substituables :

```
Métrique taux erreur 5% → QUAND ? → maintenant
Log "pool DB épuisé"    → QUOI ?  → pool_actif=100/100
Trace 4f8a → api→db     → OÙ ?    → database, 800ms, timeout
```

## Les types de métriques

```
Counter   : monotone croissant — jamais ne diminue
            Exemple : http_requests_total{service="payments", status="500"}
            Usage   : rate(http_requests_total[5m]) → débit par seconde

Gauge     : valeur instantanée — monte et descend
            Exemple : active_connections{service="database"}
            Usage   : alerte si > seuil absolu

Histogram : distribution des valeurs en buckets
            Exemple : http_latency_ms_bucket{le="100"} = nb requêtes ≤ 100ms
            Usage   : histogram_quantile(0.99, ...) → P99 latence

Summary   : quantiles pré-calculés côté client (moins flexible)
```

## Logs structurés vs texte libre

```
❌ Texte libre :
   "2024-01-15 14:32:01 ERROR payment failed for user alice"
   → impossible à interroger, impossible à corréler

✅ Log structuré (JSON) :
   {
     "ts":       "2024-01-15T14:32:01.234Z",
     "level":    "ERROR",
     "msg":      "payment failed",
     "service":  "payments",
     "trace_id": "4f8a2b7c",
     "span_id":  "e3d1f209",
     "user_id":  "alice",
     "amount":   150.0,
     "erreur":   "timeout"
   }
   → interrogeable : "tous les logs ERROR de alice avec amount > 100"
```

## Traces distribuées (OpenTelemetry)

```
W3C Trace Context : trace_id + parent_id propagés dans chaque requête HTTP

Requête checkout :
  ┌─────────────────────────────────────────────────┐
  │ Trace 4f8a2b7c    durée totale : 312ms           │
  ├─────────────────────────────────────────────────┤
  │ cart.GET /cart              [0ms ──────── 22ms]  │
  │   inventory.CHECK /stock      [5ms ── 18ms]      │
  │     payments.POST /pay          [8ms ─── 280ms]  │ ← LENT
  │       notifier.SEND /email        [285ms─ 310ms] │
  └─────────────────────────────────────────────────┘
  → payments identifié comme goulot d'étranglement en 1 query Jaeger
```

## Résultats mesurés

### Scénario 1 — Métriques

```
80 requêtes payments (taux_erreur=5%) + 120 requêtes catalog :

  payments_requests_total   : 80
  payments_errors_total     : 3
  catalog_requests_total    : 120
  catalog_errors_total      : 2

  Alertes déclenchées :
    🔴 [CRITICAL] HighErrorRatePayments
       valeur=3.75%  seuil=3%
       Taux d'erreur payments > 3%

  Taux d'erreur effectif : 3/80 = 3.8% ✅ détecté automatiquement
```

### Scénario 2 — Logs structurés

```
400 logs produits (80 payments + 120 catalog, entrée + sortie) :

  Par niveau :
    ERROR      5  █████
    INFO     395  ██████████████████████████████

  Recherches :
    Q1 level >= ERROR      → 5 logs  (toutes les erreurs du run)
    Q2 service = payments  → 160 logs
    Q3 trace_id = 96436c3a → 2 logs  (corrélation log↔trace ✅)
    Q4 contient 'timeout'  → 0 logs  (les erreurs ne mentionnent pas timeout)

  Corrélation log↔trace sur 96436c3a :
    [INFO]  payments — checkout reçue
    [ERROR] payments — checkout échouée
```

### Scénario 3 — Traces distribuées

```
10 requêtes checkout → 10 traces, 4 spans chacune :
  cart → inventory → payments2 → notifier

  Total spans     : 240 (scénarios 1+3 combinés)
  Spans en erreur : 5
  Durée P50       : 16.3ms
  Durée P99       : 77.3ms

  Exemple trace c17a6569 :
    Durée totale : 22.4ms  |  4 spans
    cart → inventory → payments2 → notifier

  Traces > 100ms : 0  (trafic nominal, pas d'incident injecté ici)
```

### Scénario 4 — Corrélation des 3 piliers (enquête incident)

```
Incident injecté : database saturée (60% erreurs, latence 800ms)

  Étape 1 — MÉTRIQUES :
    database_requests_total : 30
    database_errors_total   : 18
    Taux d'erreur           : 60.0%
    → 🔴 ALERTE : taux_erreur 60% >> seuil 3%
    → On sait QUAND et COMBIEN

  Étape 2 — LOGS :
    Logs ERROR sur database : 36
    Logs mentionnant 'Pool' : 18
    Exemple : "Pool de connexions épuisé" — pool_actif=96/100
    → On sait QUOI : pool saturé à 96-100/100 connexions

  Étape 3 — TRACES :
    Traces avec erreur : 18
    → On sait OÙ : api-gateway → database, timeout systématique

  Remédiation :
    Court terme : pool_max 100 → 300, restart database
    Moyen terme : replica read-only pour les SELECT
    Long terme  : cache Redis + CQRS
```

### Scénario 5 — SLO & Error Budget

```
SLO cible : 99.9%  |  720 heures simulées  |  1000 req/h

  SLI global    : 99.748%
  SLO           : 99.9%  → ❌ VIOLÉ
  Total erreurs : 1 812 / 720 max = 251.7% du budget consommé

  Burn rate pendant l'incident (ticks 600-650, 50h) :
    SLI période  : 97.52%
    Burn rate    : 24.8×   🔴 CRITIQUE
    Budget épuisé en : 1.2 jours à ce rythme

  Post-incident (dernières 30h) :
    Burn rate : 1.30×  🟢 OK — retour à la normale

  Alerte pendant incident : 🔴 PAGE DÉCLENCHÉ (burn rate 24.8× > seuil 14×)
```

## SLI / SLO / SLA / Error Budget

| Concept | Définition | Exemple |
|---------|------------|---------|
| SLI | Métrique mesurée | Taux requêtes réussies |
| SLO | Cible interne | SLI ≥ 99.9% sur 30 jours |
| SLA | Contrat externe avec pénalités | 99.5% garanti |
| Error Budget | 1 - SLO | 0.1% = 43.8 min/mois |

```
SLO 99.9%   → 43.8 min/mois de panne tolérée  (B2B standard)
SLO 99.99%  → 4.38 min/mois                   (paiements)
SLO 99.999% → 26 secondes/mois                (core infrastructure)
```

## Stratégie d'alerte multi-fenêtre (Google SRE)

```
Fenêtre 1h  + Burn rate > 14× → PAGE immédiat  (budget épuisé en 2 jours)
Fenêtre 6h  + Burn rate > 6×  → PAGE           (budget épuisé en 5 jours)
Fenêtre 24h + Burn rate > 3×  → Ticket urgent  (à régler dans la journée)
Fenêtre 72h + Burn rate > 1×  → Rapport hebdo

Règle : deux fenêtres déclenchées simultanément → action immédiate
→ Évite les faux positifs (spike court) ET les pannes lentes non détectées
```

## Sampling des traces

```
En production : 100% des traces = des téraoctets/jour (coût prohibitif)

Stratégies :
  Head-based : décider à l'entrée de la requête (simple, perd les traces lentes)
  Tail-based : décider APRÈS avoir vu toute la trace (optimal mais complexe)
    → 100% des traces avec erreur
    → 100% des traces > seuil de latence (ex: > 500ms)
    → 1% du trafic nominal (échantillon représentatif)
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `observabilite.py` | `RegistreMetriques`, `Compteur`, `Jauge`, `Histogramme`, `Logger`, `BackendLog`, `Traceur`, `BackendTrace`, `Span`, `Trace`, `RegleAlerte`, `GestionnaireAlertes` |
| `simulation.py` | 5 scénarios : métriques+alertes, logs structurés, traces, corrélation incident, SLO/error budget |

## Lancer

```bash
python3 simulation.py
```

## Jour 30 → Synthèse : TrueTime & Spanner

On sait construire, tester (Chaos Engineering) et observer les systèmes distribués.  
Le dernier chapitre : comment Google garantit des transactions ACID à l'échelle planétaire ?  
TrueTime (horloges atomiques + GPS) → intervalles d'incertitude → commit wait → linearizability globale.  
Le point culminant du curriculum : tout ce qu'on a appris, utilisé ensemble.
