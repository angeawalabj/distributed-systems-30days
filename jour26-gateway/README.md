# Jour 26 — API Gateway + OPA : "Le Gardien des Portes"

## Le problème

500 microservices, chacun implémente séparément :

```
Authentification JWT  → 500 implémentations différentes, 12 avec des bugs
Rate limiting         → chaque service a ses propres compteurs → bypass possible
CORS, TLS, logging    → incohérences, oublis
Règles d'accès        → duplication, pas de vue d'ensemble, pas d'audit
```

## La solution

```
API Gateway : point d'entrée UNIQUE pour tout le trafic externe
              Pipeline centralisé : Auth → Rate Limit → OPA → Route

OPA          : moteur de politiques DÉCOUPLÉ
              Les règles sont dans Git, pas dans le code
              Modifier une règle = PR → merge → hot-reload (< 1s)
              Sans toucher à aucun service
```

## Pipeline de traitement

```
Requête entrante
      │
      ▼
① Validation JWT ──► 401 si absent/expiré
      │
      ▼
② Rate Limiting ───► 429 si quota dépassé
      │
      ▼
③ Décision OPA ────► 403 si politique refuse  (+ raison dans le header)
      │
      ▼
④ Résolution route ► 404 si route inconnue
      │
      ▼
⑤ Proxy backend ───► 200 + headers enrichis
```

## Politiques OPA (extraits Rego)

```python
health-public      → GET **/health         toujours autorisé
admin-full-access  → rôle admin            autorisé (sauf DELETE sans MFA)
payments-read      → scope payments:read   GET /api/v1/payments/**
payments-write     → scope payments:write  POST/PUT /api/v1/payments/**
analytics-read     → rôle analyst          GET /api/v1/analytics/**
ip-allowlist       → /api/internal/**      IP 10.x.x.x seulement
default-deny       → tout le reste         refusé
```

## Résultats mesurés

### Scénario 1 — Pipeline complet

```
Requête                                       Status  Pipeline
────────────────────────────────────────────────────────────────────────
alice   GET /api/v1/analytics/dashboard       ✅ 200  analytics-service   0.0ms
bob     GET /api/v1/payments/123              ✅ 200  payments-service    0.0ms
bob     POST /api/v1/payments (écriture)      ✅ 200  payments-service    0.0ms
alice   GET /api/v1/payments (interdit)       🔴 403  Scope 'payments:read' requis
admin   DELETE /api/v1/payments/99 (sans MFA) 🔴 403  Admin : MFA requis pour DELETE
admin   DELETE /api/v1/payments/99 (avec MFA) ✅ 200  payments-service    0.0ms
token expiré  GET /api/v1/payments            🔴 401  Token absent ou expiré
GET /health (public, sans auth)               ✅ 200  health-check        0.0ms

Stats : ok=5, auth_echec=1, opa_refuse=2, latence_moy=0.02ms
```

### Scénario 2 — Politiques OPA

```
Description                                 Décision  Politique
──────────────────────────────────────────────────────────────────────
analyst lit analytics (interne)             ✅ ALLOW   analytics-read
user avec scope analytics:read              ✅ ALLOW   analytics-read
user écrit dans payments                    ✅ ALLOW   payments-write
user sans scope payments → refusé           🔴 DENY    payments-read
admin DELETE avec MFA                       ✅ ALLOW   admin-full-access
admin DELETE sans MFA → refusé              🔴 DENY    admin-full-access
IP externe sur route interne → refusé       🔴 DENY    ip-allowlist
IP interne sur route interne → autorisé     ✅ ALLOW   ip-allowlist

Bilan : 5 ALLOW / 3 DENY
```

### Scénario 3 — Rate Limiting (token bucket, 10 RPM)

```
Req #   Status   Tokens restants  Commentaire
─────────────────────────────────────────────────
    1  ✅ 200                 9
  ...
   10  ✅ 200                 0
   11  🔴 429                 —  ← 429 rate limited
   12  🔴 429                 —  ← 429 rate limited

Résultat : 10 OK, 5 rate-limited
```

### Scénario 4 — Policy-as-Code : blocage d'urgence

```
AVANT incident :
  reports-service    ✅ ALLOW  (analytics-read)
  analytics-service  ✅ ALLOW  (analytics-read)

⚠️  Incident SEC-2847 : reports-service exfiltre des données !
→ Ajout d'une règle dans OPA (1 ligne), hot-reload < 1s

APRÈS :
  reports-service    🔴 DENY  (blocage-urgence-SEC2847)
  analytics-service  ✅ ALLOW  (analytics-read)   ← non affecté

Sans Policy-as-Code : build → test → déploiement = 15-30 minutes
Avec OPA             : PR → merge → hot-reload    = < 1 seconde
```

### Scénario 5 — Détection d'anomalie

```
Audit log OPA (extrait) :
  user_0  POST /api/v1/payments   🔴 DENY   ← pas de scope payments:write
  hacker  GET  /api/v1/payments   🔴 DENY
  hacker  GET  /api/v1/analytics  🔴 DENY
  ...

⚠️  Anomalie détectée :
  🚨 hacker : 6 refus consécutifs → investigation !
```

## Token Bucket

```
Seau initial  : N tokens (= N requêtes en burst)
Consommation  : 1 token par requête
Recharge      : RPM/60 tokens par seconde
Seau vide     : → 429 Too Many Requests

Avantage vs compteur fixe :
  Burst court autorisé (seau plein = N requêtes immédiates)
  Pas de fenêtre de réinitialisation exploitable

Coordination multi-gateway : Redis INCR + EXPIRE (atomique via Lua)
```

## Headers de réponse enrichis

```http
X-Request-ID: abc123
X-Rate-Limit-Remaining: 47
X-Policy: payments-read
X-Latency-Ms: 0.21
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `gateway.py` | `APIGateway`, `MoteurOPA`, `RateLimiter`, `Routeur`, `Token`, `Requete`, `Reponse` |
| `simulation.py` | 5 scénarios : pipeline, politiques, rate limiting, policy-as-code, audit |

## Lancer

```bash
python3 simulation.py
```

## Jour 27 → Distributed Cache

Le Gateway vérifie les JWT et les politiques à chaque requête.  
Sans cache : 30-50ms de latence base à chaque fois.  
Avec Redis : < 1ms. Cache-Aside, invalidation, stampede, L1/L2.
