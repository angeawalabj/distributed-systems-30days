# Jour 20 — Saga Pattern (Choreography + Orchestration)

## Le problème

Une commande e-commerce touche 4 services indépendants avec des bases séparées. Si `shipment-svc` échoue après que `payment-svc` a débité, il n'existe pas de `ROLLBACK` distribué — les bases sont isolées.

```
inventory-svc  →  payment-svc  →  shipment-svc ❌ CRASH
      ✅ réservé       ✅ débité          ↑ comment annuler ça ?
```

## La solution : Saga

Une séquence de **transactions locales** avec des **actions compensatoires** en cas d'échec.

```
Flux nominal :
  RÉSERVER_STOCK ✅ → DÉBITER_PAIEMENT ✅ → CRÉER_EXPÉDITION ✅ → EMAIL ✅

Flux d'échec (shipment crash) :
  RÉSERVER_STOCK ✅ → DÉBITER_PAIEMENT ✅ → CRÉER_EXPÉDITION ❌
                                                    ↓ compensations (ordre inverse)
                                          ANNULER_PAIEMENT ↩️
                                                    ↓
                                          LIBÉRER_STOCK ↩️
```

## Résultats mesurés

### Scénario 1 — Saga nominale (Orchestration)

```
Saga cmd-abc123  ✅ succes  (184ms)
  ✅  RÉSERVER_STOCK       30ms
  ✅  DÉBITER_PAIEMENT     80ms
  ✅  CRÉER_EXPÉDITION     50ms
  ✅  ENVOYER_EMAIL        20ms

= sum(30+80+50+20) ≈ 180ms + jitter ✅
```

### Scénario 2 — Compensation (shipment toujours en échec)

```
Saga cmd-FAIL-001  ❌ echoue  (198ms)
  ✅  RÉSERVER_STOCK
  ✅  DÉBITER_PAIEMENT
  ❌  CRÉER_EXPÉDITION  — [shipment-svc] timeout DB
  ✅  DÉBITER_PAIEMENT (compensation) ↩️   ← ordre inverse
  ✅  RÉSERVER_STOCK (compensation) ↩️     ← ordre inverse

Compensations exécutées : 2 (ANNULER_PAIEMENT avant LIBÉRER_STOCK)
```

**Règle clé** : les compensations s'exécutent dans l'ordre **inverse** des étapes réussies. On ne peut pas libérer le stock avant d'avoir annulé le paiement.

### Scénario 3 — Choreography via bus d'événements

```
Journal du bus (types uniques) :
  COMMANDE_CREEE       ← api-gateway
  STOCK_RESERVE        ← inventory-svc
  PAIEMENT_DEBITE      ← payment-svc
  EXPEDITION_CREEE     ← shipment-svc
  EXPEDITION_ECHOUEE   ← shipment-svc
  COMMANDE_CONFIRMEE   ← notif-svc
  PAIEMENT_ANNULE      ← payment-svc
  STOCK_LIBERE         ← inventory-svc
  COMMANDE_ANNULEE     ← saga-choreography

Pas de coordinateur — chaque service sait quelle action faire
en écoutant l'événement précédent.
```

### Scénario 4 — Idempotence

```
Payload : {commande_id: "cmd-IDEM-001", idempotency_key: "key-abc-123"}

Appel 1 : {succes: True, service: "payment-svc", action: "DEBITER_PAIEMENT"}
Appel 2 : {succes: True, idempotent: True, message: "Déjà traité"}
Appel 3 : {succes: True, idempotent: True, message: "Déjà traité"}

→ 1 seul débit effectué malgré 3 appels ✅
```

### Scénario 5 — 30 sagas parallèles

```
Configuration          N    ✅ Succès   ❌ Échecs   Lat moy
Tout fiable           30       30           0      139ms
Shipment instable     30       19          11      146ms   (~37% d'échec)
Payment instable      30       19          11      121ms
Chaos général         30       25           5      132ms
```

## Orchestration vs Choreography

```
                    Orchestration                  Choreography
────────────────    ─────────────────────────      ──────────────────────────
Coordinateur        ✅ Oui (SagaOrchestrator)     ❌ Non (inexistant)
Couplage            Moyen (dépend du coordinator)  ✅ Minimal
État visible        ✅ En un endroit               ❌ Dispersé dans les services
Debug               ✅ Facile (un seul log)        ❌ Difficile (5 logs)
SPOF                Coordinateur possible           ✅ Aucun
Scalabilité         Bonne                           ✅ Excellente
Modif. du flux      ✅ Un fichier                   ❌ Toucher N services
```

**Règle empirique** :
- Sagas complexes (5+ étapes, compensations croisées) → **Orchestration**
- Sagas simples, équipes autonomes, haute scalabilité → **Choreography**

## Idempotence : fondamentale pour les sagas

```
Problème : at-least-once delivery + retry automatique → duplicata

Sans idempotence :
  Bus retransmet → payment-svc débite 2x
  Coordinateur crash → compensation rejouée → stock libéré 2x

Avec idempotency key (UUID par saga) :
  Service stocke : "j'ai déjà traité cmd-abc123:DEBITER_PAIEMENT"
  Deuxième appel → no-op, retourne le résultat précédent
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `saga.py` | `BusEvenements`, `ServiceSimule`, `SagaCommandeChoreography`, `SagaOrchestrator`, `EtapeSaga` |
| `simulation.py` | 5 scénarios : nominal, compensation, choreography, idempotence, statistiques |

## Lancer

```bash
python3 simulation.py
```

## Jour 21 → Circuit Breaker (Hystrix/Resilience4j)

Les sagas appellent des services externes. Si `payment-svc` est en panne et met 30s à répondre, chaque requête bloque 30s → le pool de threads sature → cascade de timeouts → tout tombe. Le **Circuit Breaker** surveille le taux d'erreur et, si le seuil est dépassé, "ouvre le circuit" : les requêtes échouent immédiatement (fail-fast) jusqu'à ce que le service se rétablisse. États : **CLOSED → OPEN → HALF-OPEN**.
