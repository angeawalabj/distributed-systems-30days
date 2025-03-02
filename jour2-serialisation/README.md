# Jour 2 — Sérialisation : JSON vs Protobuf

## Résultats mesurés (10 000 messages)

```
┌──────────────────────────┬──────────────┬──────────────┬────────────┐
│ Mesure                   │ JSON         │ Protobuf     │ Ratio      │
├──────────────────────────┼──────────────┼──────────────┼────────────┤
│ Taille totale            │ 950.5 KB     │ 424.6 KB     │ 2.2x       │
│ Taille moyenne/message   │ 97.3 octets  │ 43.5 octets  │ 2.2x       │
│ Message minimal (1 champ)│ 65 octets    │ 6 octets     │ 10.8x 🔥  │
│ Bande passante (1M req/s)│ 92.8 MB/s   │ 41.5 MB/s   │ -51 MB/s   │
└──────────────────────────┴──────────────┴──────────────┴────────────┘
```

## Pourquoi Protobuf est plus compact ?

### 1. Pas de noms de champs dans le binaire

```
JSON  : {"id": "P001", "nom": "Clavier", "prix": 89.99}
         ^^^^           ^^^^^   ^^^^^
         Ces clés répétées à chaque message = overhead pur

Proto : 0A 04 P001  12 07 Clavier  1D [float]
         ^              ^            ^
         Tags compacts (1 octet chaque) remplacent les noms
```

### 2. VarInt : les petits nombres prennent peu de place

```
Valeur   JSON (texte)   Protobuf (VarInt)
   0     1 octet        0 octet (omis !)
   1     1 octet        1 octet
  42     2 octets       1 octet
 127     3 octets       1 octet
 128     3 octets       2 octets
 300     3 octets       2 octets
9999     4 octets       2 octets
```

### 3. Champs omis si valeur par défaut

```protobuf
// Dans le .proto : stock = 0 par défaut
// Si stock == 0, le champ n'est PAS inclus dans le binaire

Message minimal { id: "P001" } → seulement 6 octets en Proto
                                → 65 octets en JSON (on encode quand même les clés vides)
```

### 4. Float IEEE 754 vs texte

```
JSON   : "prix": 89.99   → 5 caractères de texte = 5 octets + guillemets
Protobuf: 1D E1 FA B3 42 → 4 octets IEEE 754 fixe, toujours exact
```

## Anatomie d'un message Protobuf

```
Message : {id: "P001", nom: "Clavier", prix: 89.99, stock: 42}
Binaire (28 octets) :

0A    ← Tag (field=1, type=LENGTH_DELIMITED)
04    ← Longueur: 4 octets
50 30 30 31  ← UTF-8: "P001"

12    ← Tag (field=2, type=LENGTH_DELIMITED)
07    ← Longueur: 7 octets
43 6C 61 76 69 65 72  ← UTF-8: "Clavier"

1D    ← Tag (field=3, type=32BIT)
E1 FA B3 42  ← IEEE 754 little-endian: 89.99

20    ← Tag (field=4, type=VARINT)
2A    ← VarInt: 42
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `protobuf_manuel.py` | Implémentation de l'encodage binaire Protobuf depuis zéro |
| `benchmark.py` | 5 benchmarks complets JSON vs Protobuf |

## Lancer

```bash
python3 benchmark.py
```

## Quand utiliser quoi ?

| Format | Cas d'usage |
|--------|-------------|
| **JSON** | APIs publiques, config, débogage — lisibilité > performance |
| **Protobuf** | Services internes, IoT, temps réel — performance critique |
| **MessagePack** | Compromis : binaire mais sans schéma imposé |
| **Avro** | Big Data, Kafka — schéma dans le message |

## Lien systèmes distribués

Dans notre architecture gRPC du Jour 1, chaque appel RPC
transporte des messages Protobuf. Sur 1 million de requêtes/sec
(Netflix, Google), économiser 51 MB/s de bande passante =
réduction directe des coûts réseau et de la latence perçue.

**Jour 3 → Lamport Timestamps** : comment ordonner des événements
dans un système où chaque machine a sa propre horloge qui dérive.
