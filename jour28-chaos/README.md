# Jour 28 — Chaos Engineering : "Le Chaos Maîtrisé"

## Le problème

> "If you don't deliberately introduce failure, production will do it for you,
>  at the worst possible moment, without your consent." — Netflix, 2011

Un système distribué a des centaines de points de défaillance. Comment
savoir lesquels sont vraiment dangereux avant qu'un incident révèle la réponse ?

## La solution : Chaos Engineering

```
Hypothèse   : "le système DEVRAIT se comporter ainsi malgré X"
Expérience  : injecter X de façon contrôlée
Mesure      : comparer le comportement réel au comportement attendu
Correctif   : si ≠ hypothèse → faiblesse trouvée → on la corrige maintenant
```

## Vocabulaire

```
Steady State    : comportement normal mesurable (taux erreur, latence P99)
Blast Radius    : périmètre de l'expérience (1 pod, 1 zone, tout le cluster)
Abort Condition : seuil au-delà duquel l'expérience s'arrête automatiquement
Game Day        : session intensive d'injections multiples (journée dédiée)
```

## Types d'injection

| Type | Symptôme produit | Faiblesse révélée |
|------|-----------------|-------------------|
| Latence | Timeout en cascade | Pas de timeout côté client |
| Crash | Indisponibilité partielle | Pas de Circuit Breaker |
| Erreur (500) | Taux d'erreur élevé | Pas de fallback |
| Perte de paquets | Dégradation réseau | Pas de retry / idempotence |
| Partition réseau | Split-brain | Pas de gestion CAP |
| CPU saturation | Slow response | Pas de bulkhead / rate limiting |

## Résultats mesurés

### Scénario 1 — Injection de latence

```
Experience A - Latence 800ms (sous le timeout 3s) :

  Service        Phase            Dispo   Erreurs     P99
  db-service     baseline       [OK]100%    0.0%    28ms
  db-service     sous panne     [OK]100%    0.0%   831ms   <- latence visible
  db-service     recuperation   [OK]100%    0.0%    57ms

Experience B - Latence 3500ms (depasse timeout) :

  db-service     sous panne     [!!] 20%   80.0%  3525ms   <- cascade !

Conclusions :
  800ms  -> dispo=100%, P99=831ms  [OK] hypothese confirmee
  3500ms -> dispo= 20%, P99=3525ms [!!] hypothese REFUTEE — timeout cascade

Faiblesse : sans timeout client explicite, une DB lente
            bloque tous les threads → 0% de disponibilité.
```

### Scénario 2 — Crash + Circuit Breaker

```
60 appels via CB, crash injecté à #20, service rétabli à #40 :

  Appel  CB Etat       Résultat
  1      CLOSED        Succès
  20     CLOSED        Erreur 503   <- CRASH injecté
  26     OPEN          Fail-fast [OPEN]   <- CB s'est ouvert
  41     OPEN          Fail-fast [OPEN]   <- Service rétabli (CB pas encore refermé)
  60     OPEN          Fail-fast [OPEN]

Bilan :
  Succès          : 20
  Erreurs réelles : 4
  Court-circuits  : 36  (fail-fast, <1ms chacun)
  CB ouvertures   : 1

Hypothèse [OK] CONFIRMÉE : le CB absorbe la panne, payments reste réactif.
```

### Scénario 3 — Perte de paquets

```
Niveau         Baseline   Sous panne   Après   Verdict
10% perte       100.0%       91.0%   100.0%  [!!] RÉFUTÉ — sous 95%
20% perte       100.0%       75.0%   100.0%  [!!] RÉFUTÉ
50% perte        99.0%       53.0%    99.0%  [!!] RÉFUTÉ

Faiblesse découverte : sans retry + idempotence, toute perte de paquets
dégrade directement la disponibilité. L'hypothèse est réfutée dès 10%.
```

### Scénario 4 — Panne en cascade

```
Architecture : gateway -> payments -> db
                       -> fraud    -> db

Phase              gateway    payments    fraud       db
Nominal           [OK] 100%  [OK] 100%  [OK]100%  [OK] 99%
DB en panne       [OK] 100%  [OK] 100%  [OK]100%  [!!]  0%   <- CB isole !
Récupération      [~~]  98%  [OK] 100%  [OK]100%  [OK]100%

Résultat : blast radius contenu à db-service.
Gateway 100% disponible malgré la panne. Hypothèse CONFIRMÉE.
```

### Scénario 5 — Abort Condition

```
Experience                    Dispo    Abort   Verdict
A: 95% erreurs (agressive)    3.9%    [OUI]   [OK] arret automatique
B: 30% erreurs (calibrée)    72.5%    [NON]   [OK] experience complete

L'abort condition stoppe A après 25 requêtes dès que le taux
dépasse 80% — avant que le blast radius ne s'étende.
```

## Leçons tirées des 5 expériences

| # | Hypothèse | Résultat | Correctif |
|---|-----------|----------|-----------|
| 1 | 800ms tenu | ✅ Confirmée | — |
| 1 | 3500ms tenu | ❌ Réfutée | Timeout = p99 × 3 + retry exponentiel |
| 2 | Crash isolé par CB | ✅ Confirmée | — |
| 3 | 10% perte < 5% erreur | ❌ Réfutée | Retry + idempotence obligatoire |
| 4 | DB down, gateway dispo | ✅ Confirmée | — |
| 5 | Abort à 80% d'erreur | ✅ Confirmée | — |

## Configuration Circuit Breaker (optimale mesurée)

```python
CircuitBreaker(
    seuil_pct  = 50.0,   # 50% d'échecs -> ouverture
    fenetre    = 8,      # sur les 8 dernières requêtes
    timeout_s  = 1.5,    # 1.5s en OPEN avant de tester
)
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `chaos.py` | `Service`, `CircuitBreaker`, `InjecteurChaos`, `ConfigPanne`, `TypePanne`, `MetriquesService` |
| `simulation.py` | 5 scénarios : latence, crash+CB, perte paquets, cascade, abort condition |

## Lancer

```bash
python3 simulation.py
```

## Outils en production

| Outil | Éditeur | Force |
|-------|---------|-------|
| Chaos Monkey | Netflix OSS | Kill instances EC2 |
| Gremlin | SaaS | Injection précise, interface graphique |
| LitmusChaos | CNCF | Natif Kubernetes, ChaosHub |
| AWS FIS | Amazon | Natif AWS, intégré CloudWatch |
| Chaos Mesh | CNCF | Kubernetes, network partition |

## Jour 29 & 30 → Synthèse : TrueTime / Spanner

28 jours de systèmes distribués convergent ici.
Google Spanner combine **tout** ce qu'on a vu :
WAL (Jour 24 bonus) + MVCC (Jour 26 bonus) + Raft (Jour 7) +
Consistent Hashing (Jour 11) + TrueTime (horloges atomiques GPS/atome).
Résultat : une base de données distribuée sur 3 continents,
avec des transactions ACID globales et 99.999% de disponibilité.
