# Jour 21 — Circuit Breaker (Hystrix/Resilience4j)

## Le problème

`payment-svc` est en panne et timeout à 30s. 100 requêtes arrivent simultanément :

```
100 requêtes × 30s timeout = 100 threads bloqués 30 secondes
→ Pool de threads saturé
→ Toutes les nouvelles requêtes attendent un thread libre
→ L'application ENTIÈRE est figée
→ Les services appelants cascadent en timeout aussi
```

Un seul service lent peut détruire tout le système.

## La solution : Circuit Breaker

```
Disjoncteur électrique : court-circuit → disjoncteur s'ouvre immédiatement
Circuit Breaker        : trop d'échecs → circuit s'ouvre → fail-fast (0ms)
```

## Les 3 états

```
     ┌─────────────────────────────────────────────────┐
     │                                                  │
  CLOSED ──────── taux_echec > seuil ──────────▶ OPEN  │
  (normal)                                    (coupé)   │
     ▲                                           │       │
     │                                    timeout_s     │
     │                                        écoulé    │
     │                                           ▼       │
     └──── nb_sondes réussies ──────── HALF_OPEN        │
                                      (test)    │       │
                                                └───────┘
                                          sonde échouée → OPEN
```

## Résultats mesurés

### Scénario 1 — Timeline du cycle complet

```
+    42ms  ✅ succes              CLOSED    ← Phase 1 : nominal
+    83ms  ✅ succes              CLOSED
+   124ms  ✅ succes              CLOSED
+   165ms  ✅ succes              CLOSED
+   176ms  ❌ echec               CLOSED    ← Phase 2 : panne
+   187ms  ❌ echec               CLOSED
+   197ms  ❌ echec               OPEN      ← Circuit s'ouvre !
+   197ms  🚫 court_circuité      OPEN      ← Fail-fast immédiat
+   197ms  🚫 court_circuité      OPEN
+   198ms  🚫 court_circuité      OPEN      ← Phase 3 : protection
+  1830ms  ✅ succes              HALF_OPEN ← Phase 4 : sonde
+  1861ms  ✅ succes              CLOSED    ← Service rétabli !

Transitions :
  CLOSED → OPEN       (taux d'échec 50% ≥ 50%)
  OPEN   → HALF_OPEN  (timeout 1.5s écoulé)
  HALF_OPEN → CLOSED  (2 sondes réussies)
```

### Scénario 3 — Fallback cache

```
Phase 1 — service fonctionnel :
  user_1 → ['frais-user_1-A', 'frais-user_1-B']  (service réel)

Phase 2 — service en panne :
  user_1 → ['produit-A', 'produit-B', 'produit-C']  état=CLOSED  🗄️ CACHE (echec)
  user_2 → ['produit-X', 'produit-Y']               état=OPEN    🗄️ CACHE (CB ouvert)
  user_1 → ['produit-A', 'produit-B', 'produit-C']  état=OPEN    🗄️ CACHE (CB ouvert)

Appels au cache : 6
→ L'utilisateur reçoit des recommandations même quand le service est down ✅
```

### Scénario 4 — Petite vs grande fenêtre

```
Timeline (C=CLOSED, O=OPEN, H=HALF_OPEN) :

Petite fenêtre (N=4)  : CCCCCCCCCCCOOOOOOOOOOOOOOHCCCCCCCCC
                         ←nominal→  ←───panne───→  ←rétabli→

Grande fenêtre (N=20) : CCCCCCCCCCCCCCCCCCCOOOOOOHCOOOOOOOO
                         ←────── nominal ────────→  panne...

Petite : s'ouvre après 3 échecs (min_req=3) — réactif
Grande : s'ouvre après 9 échecs (min_req=8) — stable, mais tardif
```

### Scénario 5 — Isolation multi-services

```
Service           État     ✅     ❌    🚫 CB   Taux échec
analytics-svc  🟢 CLOSED   25     0       0       0%
inventory-svc  🟢 CLOSED   25     0       0       0%
payment-svc    🔴 OPEN      0     4      21     100%   ← EN PANNE
shipping-svc   🟢 CLOSED   25     0       0       0%

→ payment down, mais inventory/shipping/analytics 100% disponibles
→ Sans CB : 25 × 300ms timeout = 7.5s perdues pour tous les appelants
```

## Configuration recommandée en production

```python
CircuitBreaker(
    seuil_echec_pct   = 50.0,   # 50% d'échecs → ouverture
    taille_fenetre    = 20,     # Compter sur 20 dernières requêtes
    min_requetes      = 10,     # Attendre 10 requêtes avant d'évaluer
    timeout_ouvert_s  = 30.0,   # 30s en OPEN avant de tester
    nb_sondes         = 3,      # 3 sondes réussies pour refermer
    timeout_requete_s = 2.0,    # Timeout par requête
)
```

## Stratégies de fallback

| Stratégie | Cas d'usage |
|-----------|-------------|
| Cache local | Recommandations, prix, config (staleness OK) |
| Redis partagé | État partagé entre instances |
| Service secondaire | Replica read-only, CDN, service dégradé |
| Valeur neutre | `[]`, `0`, `""` selon le domaine |
| 503 + Retry-After | Quand aucun fallback n'est acceptable |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `circuit_breaker.py` | `CircuitBreaker`, `CircuitBreakerAvecFallback`, `EtatCircuit`, `ResultatAppel` |
| `simulation.py` | 5 scénarios : états, cascade, fallback, fenêtre, multi-services |

## Lancer

```bash
python3 simulation.py
```

## Jour 22 → Rate Limiting & Throttling

Le Circuit Breaker protège côté **client** (contre les services lents). Le Rate Limiter protège côté **serveur** (contre les clients trop voraces). 4 algorithmes avec des propriétés très différentes : **Fixed Window** (simple, burst possible), **Sliding Window** (précis, coûteux), **Token Bucket** (burst contrôlé), **Leaky Bucket** (débit constant garanti).
