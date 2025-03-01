# Jour 1 — gRPC : Communication typée entre services

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    VOTRE MACHINE                        │
│                                                         │
│  ┌──────────────┐   Protobuf binaire   ┌─────────────┐ │
│  │   CLIENT     │ ◄──────────────────► │   SERVER    │ │
│  │  client.py   │    HTTP/2 (port      │  server.py  │ │
│  │              │      50051)          │             │ │
│  └──────────────┘                      └─────────────┘ │
│                                                         │
│  Les deux parlent via le CONTRAT défini dans :          │
│                  protos/produit.proto                   │
└─────────────────────────────────────────────────────────┘
```

## Installation

```bash
pip install grpcio grpcio-tools
```

## Générer les stubs Python depuis le .proto

```bash
# Depuis le dossier racine jour1-grpc/
python -m grpc_tools.protoc \
  -I./protos \
  --python_out=./server \
  --grpc_python_out=./server \
  protos/produit.proto

# Copier dans le dossier client aussi
cp server/produit_pb2*.py client/
```

## Lancer la démo

**Terminal 1 — Démarrer le serveur :**
```bash
cd server
python server.py
```

**Terminal 2 — Lancer le client :**
```bash
cd client
python client.py
```

## Concepts clés démontrés

### 1. Les 4 types de RPC gRPC

| Type | Exemple | Quand l'utiliser |
|------|---------|-----------------|
| **Unary** | `GetProduit` | Requête/réponse classique |
| **Server Streaming** | `StreamProduits` | Grandes listes, temps réel |
| **Client Streaming** | — | Upload de fichiers |
| **Bidirectionnel** | — | Chat, jeux en ligne |

### 2. Protobuf vs JSON

```
Message Produit en JSON  : {"id":"P001","nom":"Clavier","prix":89.99,"stock":42,"categorie":"Informatique"}
Taille : 83 octets

Même message en Protobuf : 0x0a 0x04 P001 0x12 0x07 Clavier...
Taille : ~35 octets  →  58% plus léger, 3-10x plus rapide à parser
```

### 3. Gestion d'erreurs gRPC

gRPC utilise des **codes de statut standardisés** (similaires à HTTP) :
- `OK` → succès
- `NOT_FOUND` → ressource absente (≈ 404)
- `INVALID_ARGUMENT` → paramètre invalide (≈ 400)
- `UNAUTHENTICATED` → non authentifié (≈ 401)
- `INTERNAL` → erreur serveur (≈ 500)

### 4. Pourquoi gRPC plutôt que REST pour les microservices ?

```
Service A ──REST──► Service B   : JSON → parse → objet → traitement → JSON
Service A ──gRPC──► Service B   : Protobuf → struct → traitement → Protobuf

gRPC élimine la sérialisation/désérialisation coûteuse du JSON
et génère automatiquement les clients dans 10+ langages.
```

## Ce qui vient Jour 2

**Sérialisation : Benchmark JSON vs Protobuf**
On va mesurer concrètement la différence de taille et de vitesse
sur 10 000 messages.

## Lien avec les systèmes distribués

Dans un système distribué, des centaines de services se parlent
des milliers de fois par seconde. Chaque milliseconde gagnée
en sérialisation et chaque octet économisé se multiplient
par des millions de requêtes → gRPC est le standard chez
Google, Netflix, Uber, et la plupart des géants du cloud.
