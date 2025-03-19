# Jour 19 — Event Sourcing + CQRS

## Le problème avec CRUD

```sql
UPDATE accounts SET balance = 1350 WHERE id = 42
```

On sait que le solde est 1350€. On ne sait **pas** comment on y est arrivé. Impossible de répondre à : "qui a débité 100€ le 15 du mois ?" ou "quel était le solde à 14h00 le 3 mars ?"

## La solution : stocker les événements, pas l'état

```python
append(CompteOuvert(solde_initial=1000.0))
append(CompteCredite(montant=500.0, motif="Salaire"))
append(CompteDebite(montant=120.0,  motif="Loyer"))
# balance = 1000 + 500 - 120 = 1380€
# Chaque événement est immuable — jamais de UPDATE/DELETE
```

## Résultats mesurés

### Scénario 1 — Audit trail

```
Stream d'événements (acc-alice) :
  v0  [12:40:23]  CompteOuvert     solde_initial=1000.0
  v1  [12:40:23]  CompteCredite    montant=500.0  motif=Salaire mars
  v2  [12:40:23]  CompteDebite     montant=120.0  motif=Loyer
  v3  [12:40:23]  CompteDebite     montant=45.5   motif=Courses
  v4  [12:40:23]  CompteCredite    montant=80.0   motif=Remboursement
  v5  [12:40:23]  CompteDebite     montant=200.0  motif=Électricité
  v6  [12:40:23]  CompteDebite     montant=15.99  motif=Netflix
  v7  [12:40:23]  CompteCredite    montant=150.0  motif=Freelance

Total débité : 381.49€  Nb débits : 4  Plus gros : 200.00€ (Électricité)
→ Impossible à obtenir depuis un simple UPDATE balance ✅
```

### Scénario 2 — Time-travel

```
Version   Solde       Description
v1        1500.00€    après salaire   ✅
v2        1050.00€    après loyer     ✅
v3         970.00€    après assurance ✅
v4        1170.00€    après bonus     ✅
v5         870.00€    après vacances  ✅

→ Solde exact reconstitué à chaque instant passé
→ Avec CRUD : l'état passé est perdu à jamais
```

### Scénario 3 — Snapshots

```
100 événements, 20 reconstitutions mesurées :

Stratégie                  Events à rejouer   Temps moy
Sans snapshot                          101    0.021ms
Avec snapshot (tous 10)                  0    0.002ms

Snapshots créés : 10
Gain de performance : ~8.6x ✅

À 100 000 events :
  Sans snapshot → rejouer 100 000 events → secondes
  Avec snapshot → rejouer ~10 events → microsecondes
```

### Scénario 4 — Concurrence optimiste

```
Solde initial : 1000€  version=1
Alice et Bob lisent simultanément (version=1)

Alice débite 400€ → append(version_attendue=1) → ✅ stream passe à version=2
Bob   débite 700€ → append(version_attendue=1) → ❌ ConcurrencyException
                     (stream est déjà à version=2)

Solde final : 600€ (1 seul débit accepté)
Sans protection : 1000 - 400 - 700 = -100€ (découvert silencieux ❌)
```

### Scénario 5 — 3 projections CQRS depuis les mêmes événements

```
Projection 1 — Soldes (RAM, O(1) lookup) :
  acc-p1   650.00€
  acc-p2  1700.00€
  acc-p3   270.00€

Projection 2 — Historique (liste ordonnée) :
  acc-p1 : +1000€ Salaire / -800€ Loyer / -50€ Transport

Projection 3 — Transactions à risque (filtre en temps réel) :
  ⚠️ acc-p1  800.00€  Loyer
  ⚠️ acc-p2  600.00€  Voyage
```

## Architecture Event Sourcing + CQRS

```
                    COMMAND SIDE
Client ──── Command ────► Handler ──── append(Event) ──► Event Store
                              │                               │
                              └── valide règles métier        │
                                  sur l'Aggregate             │  publish
                                                              ▼
                    QUERY SIDE                        Projection Builder
Client ──── Query ─────► Read Model ◄──── update ───── (ProjectionSoldes,
           (O(1))        (dénormalisé)                   ProjectionHistorique...)
```

## Concurrence optimiste vs verrous distribués

| Approche | Mécanisme | Convient si |
|----------|-----------|-------------|
| Redlock (Jour 15) | Verrou distribué Redis | Ressource externe partagée |
| Concurrence optimiste | Version check sur append | Aggregates Event Sourcing |
| SELECT FOR UPDATE | Verrou DB transactionnel | CRUD PostgreSQL |

L'optimistic concurrency d'Event Sourcing est **sans verrou** : on essaie, on détecte le conflit, on recharge et on réessaie — idéal pour des conflits rares.

## Limites connues

| Limite | Solution |
|--------|----------|
| Schéma évolutif | Upcasting : transformer les anciens events à la lecture |
| Requêtes complexes | Projections dédiées (jamais de JOIN sur l'event store) |
| Droit à l'oubli RGPD | Crypto-shredding : chiffrer les données PII, effacer la clé |
| Event storm (trop de types) | Agréger en commandes et bounded contexts |

## Utilisé en production

| Système | Rôle |
|---------|------|
| **EventStoreDB** | Base dédiée, gRPC, subscriptions, projections natives |
| **Axon Framework** | Java, ES + CQRS out of the box |
| **Kafka** | Log d'événements distribué (pas un event store pur — pas de stream par aggregate) |
| **PostgreSQL** | Table `events(id, aggregate_id, type, payload, version)` + LISTEN/NOTIFY |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `event_sourcing.py` | `EventStore`, `Compte`, `Evenement`, `Snapshot`, `ProjectionSoldes` |
| `simulation.py` | 5 scénarios : audit trail, time-travel, snapshot, concurrence, CQRS |

## Lancer

```bash
python3 simulation.py
```

## Jour 20 → CQRS approfondi + Read Models

Event Sourcing pose les fondations. Jour 20 approfondit le **Query Side** : Read Models séparés, mise à jour asynchrone via message bus, consistance éventuelle entre write store et read store, et comment gérer les requêtes pendant la propagation.
