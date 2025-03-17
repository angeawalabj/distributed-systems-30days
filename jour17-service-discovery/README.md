# Jour 17 — Service Discovery (Consul/Etcd)

## Le problème

Dans un cluster dynamique, les IPs changent constamment :
- Un Pod Kubernetes redémarre → nouvelle IP
- Un service scale up → nouvelles instances inconnues des clients
- La configuration statique (`hosts.conf`) est impossible à maintenir

## La solution : un registre de services

```
Service A démarre
    │
    ▼
Registre  ←── PUT /register {name: "api", ip: "10.0.1.5", port: 8080}
    │
    │  Watch (notifications)
    ▼
Client B  ──── GET /resolve("api") ──── [10.0.1.5:8080, 10.0.1.6:8080]
                                              │
                                              ▼
                                         Appel direct
```

## Résultats mesurés

### Scénario 1 — Cycle de vie basique
```
Enregistrement  → 3 instances api-utilisateurs @ 10.x.x.x:800{1,2,3}
Résolution      → 3 instances saines retournées
Désenregistrement → 2 instances restantes ✅
Service inconnu → 0 résultats → client retourne 503
```

### Scénario 2 — Health Checks TTL vs HTTP

```
paiement-svc-1 (TTL 3s)  paiement-svc-2 (HTTP check)

+    4ms  État initial : [svc-1 ✅, svc-2 ✅]
+ 1005ms  Instances saines : [svc-1, svc-2]
+ 1505ms  💥 svc-2 défaillant (HTTP check → False)
+ 2006ms  Instances saines : [svc-1]        ← HTTP check détecte en ~0.5s
+ 3005ms  💥 svc-1 arrête heartbeat
+ 7008ms  Instances saines : (aucune)        ← TTL expire en ~3s après arrêt
```

**HTTP check** (pull) : détecte la panne en 0.5s (intervalle du check)
**TTL check** (push) : expire après ttl_s sans heartbeat — l'instance contrôle quand elle est "prête"

### Scénario 3 — Watch : notifications instantanées
```
+  8713ms  Scale UP  : +2 instances
+  8714ms  Watch déclenché : 3 instances (×2 notifications)
+  9716ms  Scale DOWN: -2 instances
+  9716ms  Watch déclenché : 2 puis 1 instances
+ 10217ms  Rolling deploy: -1 instance
+ 10418ms  Watch déclenché : +1 instance v2

Catalogue final : recommendation-svc-eu-2 (v1)  +  recommendation-svc-eu-10 (v2)
```

### Scénario 4 — Routage par tags
```
Catalogue : 6 instances order-svc avec tags [version, région, env]

Filtre                    Instances  Résultat
v1 + eu-west + prod            2   order-svc-eu-west-{1,2}
us-east + prod                 2   order-svc-us-east-{4,5}
v2 + prod                      1   order-svc-eu-west-3   ← canary
staging                        1   order-svc-eu-west-6
prod (tout)                    5   (hors staging)
```

### Scénario 5 — Cache client
```
200 appels vers inventory-svc :
  Cache hits   : 199/200  (99.5%)
  Cache misses :   1/200   (0.5%)
  Durée totale : 0.1ms  (0.001ms/appel)

Impact du TTL cache sur 1000 appels :
  TTL=0s  → 1000 req au registre   ████████████████████████████████████████
  TTL=1s  →   11 req au registre   █████
  TTL=5s  →    3 req au registre   █
  TTL=30s →    2 req au registre   █
```

## Deux modèles de découverte

```
Client-side (Consul, Eureka) :
  Client → Registre : "Où est api-commandes ?"
  Registre → Client : [10.0.1.5:8080, 10.0.1.6:8080]
  Client → Backend  : choisit lui-même, gère le LB

Server-side (Kubernetes Service, AWS ALB+ECS) :
  Client → VIP stable (ex: 10.96.0.1:80)
  kube-proxy → Registre : récupère les Endpoints
  kube-proxy → Backend  : route le paquet au bon Pod
```

## Comparaison des systèmes

| Système | Protocole | Backend | Points forts |
|---------|-----------|---------|--------------|
| **Consul** | HTTP/DNS/gRPC | Raft | Service mesh, ACL, multi-DC |
| **etcd** | gRPC | Raft | Base de Kubernetes, très fiable |
| **Eureka** | HTTP/REST | Réplication pair-à-pair | Netflix/Spring, AP |
| **ZooKeeper** | Binaire | ZAB | Kafka, Hadoop, CP fort |
| **Kubernetes DNS** | DNS | etcd | Native K8s, transparent |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `service_discovery.py` | `RegistreServices`, `InstanceService`, `HealthCheck`, `ClientDecouverte` |
| `simulation.py` | 5 scénarios : base, health checks, watch, tags, cache |

## Lancer

```bash
python3 simulation.py
```

## Jour 18 → Distributed Tracing (Jaeger/Zipkin)

Le service discovery sait où sont les services. Maintenant : quand une requête traverse 5 microservices et prend 2 secondes, **où est le goulot** ? Distributed Tracing propage un `trace_id` unique à travers tous les services. Chaque service crée un **span** avec son temps de traitement. Jaeger et Zipkin reconstituent la cascade complète et montrent exactement quelle étape est lente.
