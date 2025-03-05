# Jour 5 — Idempotence

## Le problème

Quand un réseau coupe après l'envoi d'une requête, le client ne
sait pas si le serveur a traité l'opération. Il renvoie. Sans
protection, le résultat est un doublon — paiement débité deux fois,
commande créée deux fois, email envoyé deux fois.

**Démontré en scénario 1** :
```
Tentative 1 → ✅ tx=436c360d  solde=950€
Tentative 2 → ✅ tx=6157eed4  solde=900€   ← DOUBLON
Tentative 3 → ✅ tx=db801cc5  solde=850€   ← DOUBLON
Débit réel : 150€ pour une commande de 50€. Illégal.
```

## La solution : Clé d'idempotence

```
CLIENT génère UUID unique par intention métier
       → l'envoie avec CHAQUE tentative

SERVER mémorise le résultat indexé par UUID
       → si clé connue : rejoue le résultat sans ré-exécuter
```

## Résultats mesurés

### Scénario 2 — 10 tentatives, 1 débit
```
Tentative  1 → EXECUTION RÉELLE       tx=d1684798  (25.0ms)
Tentative  2 → REPLAY (instantané)    tx=d1684798  (0.1ms)
...
Tentative 10 → REPLAY (instantané)    tx=d1684798  (0.0ms)

Solde : 950€ (−50€ une seule fois) ✅
Transactions DB : 1 ✅
```

### Scénario 4 — Concurrence (2 threads, même clé)
```
Thread-A → tx=0d636a27  (151ms)  ← exécution réelle
Thread-B → tx=0d636a27  (130ms)  ← a ATTENDU A, puis rejoué

Thread B n'a pas ré-exécuté — il a attendu le verrou de A.
1 seule transaction en DB. ✅
```

## Architecture du store

```
┌──────────────────────────────────────────────────────┐
│                 StoreIdempotence                     │
│                                                      │
│  recevoir(clé, payload)                              │
│    ├─ clé inconnue → EN_COURS → exécuter → COMPLETE │
│    ├─ clé EN_COURS → ATTENDRE → rejouer résultat    │
│    ├─ clé COMPLETE → rejouer résultat immédiatement │
│    └─ clé + payload≠ → ERREUR 422 (conflit)         │
│                                                      │
│  TTL 24h → nettoyage automatique                     │
│  Hash payload → détection de mutation               │
└──────────────────────────────────────────────────────┘
```

## Distinction erreurs réseau vs erreurs métier

```
Erreur réseau (ConnectionError, Timeout)
  → NE PAS mémoriser → autoriser le retry avec même clé

Erreur métier (SoldeInsuffisant, ProduitInexistant)
  → MÉMORISER → rejouer la même erreur à chaque retry
  → Stripe fait exactement ça : même erreur, même clé
```

## En production : Redis

```bash
# Réservation atomique (SETNX = SET if Not eXists)
SET idempotency:{key} "EN_COURS" EX 86400 NX

# Enregistrement du résultat
SET idempotency:{key} {json_result} EX 86400
```

L'atomicité de `SETNX` garantit qu'un seul processus "gagne" la
réservation même sous forte concurrence distribuée.

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `idempotence.py` | `StoreIdempotence`, `EntreeIdempotence`, décorateur `@idempotent` |
| `simulation.py` | 5 scénarios avec mesures réelles |

## Lancer

```bash
python3 simulation.py
```

## Ce qui est idempotent nativement vs ce qui ne l'est pas

```
✅ Nativement idempotent    ⚠️  Nécessite le mécanisme
─────────────────────────  ──────────────────────────
GET    (lecture pure)       POST   (création)
HEAD                        PATCH  (modification)
PUT    (remplacement total) Paiements
DELETE                      Envoi d'emails
                            Notifications push
```

## Jour 6 → Bully Algorithm

Semaine 2 : on attaque le **consensus**. Comment les nœuds d'un
cluster élisent-ils automatiquement un nouveau leader quand l'ancien
tombe ? Le Bully Algorithm est le mécanisme le plus simple — et ses
failles illustrent pourquoi Raft et Paxos existent.
