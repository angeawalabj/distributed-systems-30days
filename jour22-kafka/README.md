# Jour 22 — Kafka : "Le Fleuve Infini"

## Le problème

Connecter N producteurs à M consommateurs sans couplage direct :

```
Sans Kafka :
  Service A → appelle directement Service B
  Si B est lent → A attend ou perd des messages
  Si B tombe → A perd des messages
  Si A est plus rapide que B → queue interne → OOM

Avec Kafka :
  Producteur → écrit dans un Topic (log durable, append-only)
  Consommateur → lit depuis ce Topic à SON rythme
  → A et B ne se connaissent pas → découplage total
```

## Concepts fondamentaux

```
TOPIC      : canal nommé, append-only, immuable
             (comme une table de base de données, mais en flux)

PARTITION  : subdivision d'un topic pour le parallélisme
             Topic "orders" × 4 partitions → 4 flux indépendants
             Chaque partition = log ordonné par offset

OFFSET     : position d'un message dans une partition (0, 1, 2...)
             Monotone croissant, jamais réutilisé

CLEF       : hash(clef) % nb_partitions → même clef = même partition
             → Ordre garanti pour tous les messages d'un même utilisateur

RETENTION  : durée de conservation (7 jours par défaut)
             → Tout consommateur peut rejouer depuis l'offset 0
```

## Consumer Group

```
4 partitions + 2 consommateurs → 2 partitions par consommateur
4 partitions + 4 consommateurs → 1 partition par consommateur
4 partitions + 6 consommateurs → 2 consommateurs INACTIFS

Règle : 1 partition ≤ 1 consommateur dans un groupe
→ Scalabilité max = nb_partitions
```

## Résultats mesurés

### Scénario 1 — Partitionnement

```
Production avec clef (user_id) — 3 messages par user :

  user=alice  → partition 0 toujours ✅ (ordre garanti)
  user=eve    → partition 2 toujours ✅ (ordre garanti)

Distribution SANS clef (round-robin) — 12 messages :
  partition-0 : ███ (3 messages)
  partition-1 : ███ (3 messages)
  partition-2 : ███ (3 messages)
  partition-3 : ███ (3 messages)

Note : avec MD5 et 4 partitions, alice/bob/carol/diana hashent vers partition 0
→ Partition skew réel → Kafka production utilise murmur2 pour mieux distribuer
```

### Scénario 2 — Consumer Groups

```
group-C (6 consommateurs, 4 partitions) :
  c1 → partitions [0]  ✅ 20 messages
  c2 → partitions [1]  ✅ 8 messages
  c3 → partitions [2]  ✅ 8 messages
  c4 → partitions [3]  ✅ 4 messages
  c5 → partitions []   ⚠️ inactif
  c6 → partitions []   ⚠️ inactif

Rebalancing :
  c1=[0, 2], c2=[1, 3]          ← avant
  c1=[0, 3], c2=[1], c3=[2]     ← après ajout c3
  c1=[0, 2], c3=[1, 3]          ← après départ c2
→ Redistribution automatique ✅
```

### Scénario 3 — Consumer Lag

```
Lag initial : {0: 40, 1: 60} → 100 messages en retard

Traitement par lots de 10 :
   Lot    Lus   Lag total
     1     20          80
     2     20          60
     3     20          40
     4     20          20
     5     10          10
     6     10           0

Lag final : 0 ✅
```

### Scénario 4 — Replay

```
service-analytics (existant)  : lag = 0 ✅ (à jour)
service-ml (NOUVEAU)          : reçoit 60/60 messages ✅ (tout l'historique)
Breakdown : {'login': 13, 'click': 16, 'logout': 10, 'signup': 14, 'purchase': 7}
```

### Scénario 5 — Sémantiques de livraison

```
AT-LEAST-ONCE avec crash :
  💥 Crash après 8 messages — offset NON commité
  Messages traités uniques : 20/20
  Doublons détectés        : 8   ← les 8 non-commités sont rejoués

EXACTLY-ONCE avec idempotence :
  Messages traités uniques : 20/20
  Doublons ignorés         : 0  ✅
```

## Sémantiques de livraison

| Sémantique | Perte ? | Doublon ? | Latence | Usage |
|------------|---------|-----------|---------|-------|
| At-most-once | oui | non | min | métriques approximatives |
| At-least-once | non | possible | normal | + idempotence côté conso |
| Exactly-once | non | non | +2-5ms | paiements, finance |

## Consumer Lag en production

```bash
# Prometheus
kafka_consumer_group_lag{group="payment-processor"} 0

# Alerte : lag > 10 000 → consommateur trop lent
# Solution : ajouter des instances dans le consumer group
#            ou augmenter nb_partitions (puis rebalancer)
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `kafka.py` | `KafkaBroker`, `Topic`, `Partition`, `ConsumerGroup`, `Message` |
| `simulation.py` | 5 scénarios : partitionnement, consumer groups, lag, replay, sémantiques |

## Lancer

```bash
python3 simulation.py
```

## Jour 23 → Flink

Kafka **transporte** les événements.  
Flink les **traite** en temps réel avec des fenêtres temporelles.  
Tumbling, sliding, session windows. Event time vs processing time. Watermarks.
