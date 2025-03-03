# Jour 3 — Lamport Timestamps

## Le problème fondamental

Dans un système distribué, chaque machine a sa propre horloge physique
qui **dérive** (même quelques millisecondes suffisent pour créer des
incohérences). Si on trie les événements par horloge physique, on peut
obtenir un ordre causal faux — une connexion avant la création du compte,
un paiement avant la commande.

## La solution : Horloge logique de Lamport (1978)

Trois règles seulement :

```
Événement interne   →  t = t + 1
Envoi d'un message  →  t = t + 1, inclure t dans le message
Réception message   →  t = max(t_local, t_reçu) + 1
```

Le `max(...) + 1` à la réception est la clé : il garantit que l'événement
"réception" est **toujours postérieur** à l'événement "envoi", peu importe
l'état des horloges physiques.

## Ce que les benchmarks ont montré

### Scénario 2 — Correction de la causalité

```
AVANT (horloge physique) :   APRÈS (Lamport) :
  +0ms   Paris  Création       t=1  Paris   Création
  +50ms  Tokyo  Connexion      t=2  Paris   Envoi synchro
  +100ms Paris  Synchro        t=3  Tokyo   Réception synchro
  ❌ Connexion avant synchro   t=4  Tokyo   Connexion
                                ✅ Connexion toujours après synchro
```

### Scénario 4 — Concurrence visible dans le timestamp

```
t=1  Serveur-A  Alice : titre = "Rapport Q3"   ← même timestamp !
t=1  Serveur-B  Bob   : titre = "Rapport Q4"   ← concurrent détecté
```
Deux événements avec `t=1` = pas de relation causale entre eux = conflit potentiel.

### Scénario 3 — Flux e-commerce complet

```
t=1   API-Gateway  Commande reçue
t=2   API-Gateway  → Paiement
t=3   Paiement     ← Réception
t=4   Paiement     Vérification carte
t=5   Paiement     Paiement autorisé
...
t=18  API-Gateway  Email client envoyé
```
L'ordre Lamport reconstitue exactement le flux métier causal.

## Ce que Lamport garantit (et ne garantit pas)

```
GARANTIT :
  A → B  ⟹  L(A) < L(B)        Si A cause B, son timestamp est plus petit
  Ordre total avec tiebreak PID  Déterministe et reproductible

NE GARANTIT PAS :
  L(A) < L(B)  ⟹  A → B        Un petit timestamp ne prouve pas la causalité
  Détection des conflits         → Voir Vector Clocks (Jour 14)
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `lamport.py` | `HorlogeLamport` thread-safe + classe `Evenement` + `Processus` |
| `simulation.py` | 5 scénarios progressifs |

## Lancer

```bash
python3 simulation.py
```

## Utilisé en production dans

- **CockroachDB / Google Spanner** : timestamps hybrides (Lamport + physique)
- **Kafka** : les offsets sont une forme d'horloge logique par partition
- **Raft / Paxos** : les termes et log indices sont des Lamport timestamps
- **Cassandra** : `writetime()` est un Lamport timestamp avec tiebreak

## Jour 4 → Heartbeat (Détection de pannes)

Maintenant qu'on sait ordonner les événements, comment détecter
qu'un nœud est **mort** ? Un nœud silencieux est-il en train de
calculer, ou bien est-il planté ? → Le mécanisme de Heartbeat.
