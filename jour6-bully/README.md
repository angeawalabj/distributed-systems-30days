# Jour 6 — Bully Algorithm : Élection de leader

## Le problème

Un cluster distribué a besoin d'un **leader unique** pour coordonner
les décisions (qui écrit, qui distribue le travail, qui gère les verrous).
Quand ce leader tombe (détecté par le Heartbeat du Jour 4), les nœuds
restants doivent en élire un nouveau — automatiquement, sans humain.

## L'algorithme du Bully (Garcia-Molina, 1982)

**Principe** : le nœud avec le plus grand ID "intimide" les autres et s'impose.

```
3 messages seulement :

ELECTION    → "Je lance une élection, tu es plus grand que moi ?"
OK          → "Oui, je le suis, recule"
COORDINATOR → "J'ai gagné. Je suis le nouveau leader."

1 règle :
  Le nœud vivant avec le plus grand ID gagne toujours.
```

## Résultats mesurés

### Scénario 1 — Convergence initiale
```
5 nœuds démarrent → N5 élu en 0.07s
Tous les nœuds confirment : leader=N5 ✅
```

### Scénario 2 — Mort du leader, réélection
```
t+0.00s  N5 crash
t+0.41s  Timeout OK → N4 se proclame COORDINATOR
t+0.43s  Tous reconnaissent N4

Réélection complète en ~0.4s ✅
```

### Scénario 3 — Résurrection (le comportement "bully")
```
N5 revient → lance immédiatement ELECTION
N5 gagne → reprend le leadership en 0.02s

Le cluster était en pleine élection (N1, N2, N3 en état ELECTION)
→ converge quand même vers N5 ✅

Yo-Yo Problem visible : si N5 oscille, le cluster
passe son temps à réélire.
```

### Scénario 4 — 3 pannes simultanées (le cas difficile)
```
N5, N4, N3 tombent simultanément
→ N3 et N4 se proclament encore leader brièvement (race condition)
→ Les nœuds vivants (N1, N2) relancent une élection après timeout
→ N2 gagne finalement à t+1.27s ✅

Le split-brain temporaire se résout grâce aux timeouts.
```

### Scénario 5 — 15% de pertes réseau
```
Messages perdus → relances automatiques par timeout
→ Convergence quand même vers N4 ✅
Bully est résilient aux pertes occasionnelles.
```

## Complexité

```
┌─────────────────────┬───────────────┬──────────────────┐
│ Cas                 │ Messages      │ Formule          │
├─────────────────────┼───────────────┼──────────────────┤
│ Meilleur (N lance)  │ N-1           │ O(N)             │
│ Pire (N1 lance)     │ N(N-1)/2      │ O(N²)            │
│ 10 nœuds            │ ~45           │ acceptable       │
│ 100 nœuds           │ ~4 950        │ lourd            │
│ 1 000 nœuds         │ ~500 000      │ catastrophique   │
└─────────────────────┴───────────────┴──────────────────┘
```

## Machine à états d'un nœud

```
NORMAL ──── détecte absence leader ────► ELECTION
  ▲                                          │
  │                              reçoit COORDINATOR
  │                                          │
  └─────────────────────────────────── NORMAL/LEADER
```

## Garanties et limites

```
GARANTIT :
  ✅ Le nœud vivant avec le plus grand ID gagne toujours
  ✅ Converge si moins de 50% de pannes simultanées
  ✅ Simple à implémenter (~100 lignes)

NE GARANTIT PAS :
  ❌ Pas de split-brain si partition réseau persistante
  ❌ O(N²) messages → non scalable au-delà de ~100 nœuds
  ❌ Yo-Yo problem si le leader oscille
  ❌ Pas de rotation : toujours le même leader si toujours vivant
```

## Lien avec les jours précédents

- **Jour 4 (Heartbeat)** : détecte la mort du leader → déclenche l'élection
- **Jour 5 (Idempotence)** : pendant l'élection, les requêtes entrantes
  doivent être mémorisées pour ne pas se perdre
- **Jour 3 (Lamport)** : les messages ELECTION/OK/COORDINATOR sont
  des événements ordonnés par horloge logique

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `bully.py` | `Noeud`, `Reseau`, `Message`, `TypeMessage`, `EtatNoeud` |
| `simulation.py` | 5 scénarios + analyse de complexité |

## Lancer

```bash
python3 simulation.py
```

## Jour 7 → Introduction à Raft

Bully a démontré ses limites : O(N²) messages, yo-yo problem,
split-brain possible. **Raft** résout tout ça avec :
- **Terms** (mandats numérotés) : pas de leader zombie
- **Vote majoritaire** : split-brain impossible par construction
- **Log matching** : cohérence garantie des données
- **O(N) messages** par élection

C'est la base d'etcd, CockroachDB, TiKV, et l'élection de leader
dans Kubernetes.
