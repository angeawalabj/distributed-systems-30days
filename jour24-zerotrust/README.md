# Jour 24 — Zero-Trust Networking : "Confiance Zéro"

## Le problème

Le modèle de sécurité traditionnel ("château fort") :

```
Internet (dangereux)
       │
    Firewall
       │
Réseau interne ← "tout le monde ici est de confiance"
   ├── Service A
   ├── Service B   ← si A est compromis, il peut parler à B librement
   └── Database    ← et à la base de données
```

**Un seul attaquant à l'intérieur = accès total.**  
2020 : SolarWinds — mouvement latéral pendant 9 mois, non détecté.

## La solution : Zero-Trust

```
"Never trust, always verify"
Chaque connexion est authentifiée, même interne.
Chaque service prouve son identité à chaque requête.
```

## Les 4 piliers

```
1. IDENTITÉ    → SPIFFE SVID (certificat X.509 avec URI spiffe://)
2. DISPOSITIF  → attestation de l'environnement d'exécution
3. RÉSEAU      → mTLS (le client ET le serveur se vérifient mutuellement)
4. POLITIQUE   → OPA / Rego → moindre privilège par défaut
```

## mTLS vs TLS

```
TLS standard :
  Client → vérifie le certificat du Serveur ✅
  Serveur → fait confiance au Client ? Aucune vérification ❌

mTLS (Mutual TLS) :
  Client → vérifie le certificat du Serveur ✅
  Serveur → vérifie le certificat du Client ✅
  → Les deux prouvent leur identité
```

## Architecture

```
Service A                    Service B
┌─────────────┐  mTLS        ┌─────────────┐
│ SVID A      │◄────────────►│ SVID B      │
│ Agent ZT    │              │ Agent ZT    │
└──────┬──────┘              └──────┬──────┘
       │                             │
       └──────────► OPA ◄────────────┘
                (décision politique)
```

## Résultats mesurés

### Scénario 1 — Handshake mTLS

```
Test    Description                           Résultat
────────────────────────────────────────────────────────────────────────
1       Certificat révoqué                    ❌ REJETÉ
2       Certificat expiré                     ❌ REJETÉ
3       CA inconnue (auto-signé)              ❌ REJETÉ
4       analytics→payments (légitime)         ✅ ACCEPTÉ
```

### Scénario 2 — Politiques

```
Requête                                     Décision    Politique
─────────────────────────────────────────────────────────────────────────
analytics GET /data/users/profiles          ✅ ALLOW    data-users-acl
payments POST /kafka/payments (PRODUCE)     ✅ ALLOW    kafka-payments-acl
billing GET /data/users/profiles            ✅ ALLOW    data-users-acl
fraud GET /kafka/payments (CONSUME)         ✅ ALLOW    kafka-payments-acl
fraud GET /health                           ✅ ALLOW    health-check
analytics POST /kafka/payments (PRODUCE)    🔴 DENY     kafka-payments-acl
admin DELETE /data/users (sans MFA)         🔴 DENY     admin-destructive
fraud GET /admin/config                     🔴 DENY     default-deny
Bilan : 5 ALLOW / 3 DENY
```

### Scénario 3 — Rotation de certificats

```
Rotation    Fingerprint         Délai
──────────────────────────────────────────
   1        a8f3c2d1...         0.0ms
   2        9e7b4f2a...         0.1ms
   ...
  10        3d2e8f1b...         0.1ms

12 certs émis, 10 rotations, 0ms downtime ✅
Chaque rotation produit un fingerprint différent ✅
```

### Scénario 4 — Mouvement latéral bloqué

```
recommendations-service COMPROMIS

Tentative d'accès                           Résultat
──────────────────────────────────────────────────────
POST /kafka/payments (PRODUCE)              🔴 BLOQUÉ
GET /admin/config                           🔴 BLOQUÉ
DELETE /data/users                          🔴 BLOQUÉ
GET /data/users/profiles                    ⚠️  AUTORISÉ ← sur-autorisation !

→ Vecteur de mouvement latéral résiduel détecté
```

### Scénario 5 — Audit et détection d'anomalie

```
rogue-service : 3 accès DENY consécutifs
→ 🚨 ALERTE SIEM : seuil de refus atteint (3 en 300s)
→ Investigation automatique déclenchée
```

## Durée des certificats : trade-off

| Durée | Exposition si compromis | Complexité rotation |
|-------|------------------------|---------------------|
| 1 an  | 365 jours              | Faible              |
| 90 j  | 90 jours (Let's Encrypt)| Automatisable      |
| 24h   | 24 heures              | Nécessite SPIFFE   |
| 1h    | 1 heure (recommandé)   | SPIRE obligatoire  |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `zerotrust.py` | `AutoriteCertification`, `HandshakeMTLS`, `MoteurPolitique`, `AgentZeroTrust` |
| `simulation.py` | 5 scénarios : mTLS, politiques, rotation, mouvement latéral, audit |

## Lancer

```bash
python3 simulation.py
```

## Jour 25 → SPIFFE/SPIRE

Zero-Trust requiert que chaque service prouve son identité.  
Mais comment un pod Kubernetes obtient-il son certificat sans connaître de secret au démarrage ?  
SPIFFE résout l'identité de workload par attestation de l'environnement.
