# Jour 25 — SPIFFE/SPIRE : "Le Sceau d'Identité"

## Le problème

Comment un service prouve-t-il son identité sans secret codé en dur ?

```
Secret dans l'image Docker  → fuite si l'image est extraite
Secret dans les env vars    → visible dans les logs, ps aux
Secret dans un vault        → bootstrapping problem (comment s'authentifier au vault ?)

SPIFFE : l'identité est prouvée par l'ENVIRONNEMENT D'EXÉCUTION,
         pas par un secret que le service connaît.
```

## Concepts fondamentaux

### SPIFFE ID

```
URI standard : spiffe://trust-domain/workload-path
Exemples :
  spiffe://prod.acme.com/payments-service
  spiffe://prod.acme.com/analytics/workers/batch
  spiffe://cluster-b.acme.com/fraud-detection

→ Stocké dans le champ SAN (Subject Alternative Name) d'un certificat X.509
→ Vérifiable cryptographiquement, sans base de données centrale
```

### SVID (SPIFFE Verifiable Identity Document)

```
= Certificat X.509 standard + URI SPIFFE dans SAN
Durée typique : 1 heure (recommandé)
Contient      : clef publique, trust domain, workload path, expiration
Signé par     : la CA du SPIRE Server
```

### Sélecteurs

```
Kubernetes : k8s:ns:payments       ← namespace du pod
             k8s:sa:payments-sa    ← ServiceAccount
             k8s:pod-label:version:v2  ← label

Unix       : unix:uid:1000         ← UID du processus

→ Combinaison sélecteurs → SPIFFE ID (défini dans le registre SPIRE)
```

## Architecture SPIRE

```
SPIRE SERVER (1 par cluster)
  ├── Stocke les entries : sélecteurs → SPIFFE ID
  ├── Signe les SVIDs (CA interne)
  └── Atteste les agents (node attestation)
         │ TLS
SPIRE AGENT (1 par nœud)
  ├── Atteste les workloads (workload attestation)
  ├── Cache et renouvelle les SVIDs
  └── Expose l'API Workload (socket Unix)
         │ Unix socket
WORKLOAD (ton service)
  └── Appelle l'API → reçoit son SVID → s'authentifie en mTLS
```

## Résultats mesurés

### Scénario 1 — Attestation zéro secret

```
Workloads qui démarrent (aucun ne connaît de secret) :

🚀 payments-service        (pid=1001)
   Sélecteurs     : k8s:ns:payments, k8s:sa:payments-sa
   SPIFFE ID reçu : spiffe://prod.acme.com/payments-service
   TTL            : 3600s  ✅

🚀 analytics-service       (pid=1002)
   SPIFFE ID reçu : spiffe://prod.acme.com/analytics-service ✅

🚀 fraud-detection         (pid=1003)
   SPIFFE ID reçu : spiffe://prod.acme.com/fraud-detection ✅

Stats : 2 agents attestés, 3 SVIDs émis, emis_frais=3
```

### Scénario 2 — Rotation automatique

```
Simulation (TTL=6s) :

 Temps   Vie restante   % vie  Événement
  0.0s         6.0s     100%  ✅ Cache valide
  0.9s         5.1s      86%  ✅ Cache valide
  1.7s         4.3s      71%  ✅ Cache valide
  2.6s         3.4s      57%  ✅ Cache valide
  3.4s         2.6s      43%  🔄 ROTATION — nouvelle clef émise
  4.3s      3600.0s     100%  ✅ Cache valide (nouveau SVID)

Rotations : 1  —  SVIDs émis total : 2

En production (TTL=1h) :
  Renouvellement à t=40min → overlap de 20min → 0 connexion cassée
```

### Scénario 3 — Granularité des sélecteurs

```
Pod / Processus                             SPIFFE IDs reçus
─────────────────────────────────────────────────────────────
Pod monitoring (namespace seul)           → monitoring
Pod payments v1 (ns + SA)                 → payments-service
Pod payments v2 (ns + SA + label)         → payments-service
Pod payments v2 (ns + SA + label)         → payments-service-v2  ← 2 SVIDs !
Processus Unix uid=1000                   → cron-job
Pod inconnu (sélecteurs sans entry)       → ❌ aucune identité
```

### Scénario 4 — Fédération

```
SANS fédération (cluster A) :
  ✅ A valide son propre SVID payments
  ❌ A valide le SVID analytics (cluster B) — trust domain inconnu
  ❌ A valide le SVID partner (externe)

APRÈS fédération A ↔ B :
  ✅ A valide son propre SVID payments
  ✅ A valide le SVID analytics (cluster B)   ← nouveau ✅
  ✅ B valide le SVID payments (cluster A)    ← nouveau ✅
  ❌ A valide le SVID partner (non fédéré)    ← toujours ❌
```

### Scénario 5 — Impostures rejetées

```
Tentative                                    Résultat
──────────────────────────────────────────────────────────────────────
Service légitime (bon namespace + SA)        ✅ 1 SVID émis
Mauvais namespace 'compromised'              ❌ 0 SVID
Mauvais ServiceAccount 'default'             ❌ 0 SVID
SVID forgé (fausse CA)                       ❌ Bundle CA invalide
SVID expiré rejoué                           ❌ SVID expiré ou révoqué
Agent non attesté (hacker-node)              ❌ 0 SVID
```

## Fédération : cas d'usage

| Scénario | Trust Domain A | Trust Domain B |
|----------|---------------|---------------|
| Multi-cloud | AWS prod | GCP analytics |
| Acquisition | acme.com | startup.io |
| Partenariat | acme.com | partner.com |
| Multi-cluster | cluster-eu | cluster-us |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `spiffe.py` | `SPIREServer`, `SPIREAgent`, `WorkloadAPI`, `SVID`, `TrustBundle`, `Entry` |
| `simulation.py` | 5 scénarios : attestation, rotation, sélecteurs, fédération, imposture |

## Lancer

```bash
python3 simulation.py
```

## Jour 26 → API Gateway + OPA

SPIFFE prouve **qui** est un service.  
L'API Gateway contrôle **ce qu'il peut faire** en centralisant auth, rate limiting et routing.  
OPA (Open Policy Agent) exprime les règles d'accès en Rego — versionnées dans Git, modifiables sans redéploiement.
