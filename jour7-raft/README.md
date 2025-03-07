# Jour 7 — Raft : Algorithme de consensus

## Pourquoi Raft après Bully ?

Bully (Jour 6) élit un leader mais ne garantit pas la cohérence
des données entre nœuds. Raft résout les deux problèmes ensemble :
élection **et** réplication de log avec garanties de durabilité.

## Les 4 concepts fondamentaux

```
1. TERMS (mandats numérotés)
   Chaque élection incrémente le term.
   Un message avec term < current_term est ignoré.
   → Pas de leader zombie après résurrection.

2. VOTE (quorum strict)
   Quorum = N/2 + 1 (majorité absolue)
   Un seul leader possible par term par construction.
   → Split-brain physiquement impossible.

3. LOG RÉPLIQUÉ
   Chaque commande = entrée de log (index, term, commande)
   Committée seulement quand la majorité l'a répliquée.
   → Données indestructibles même si le leader crashe.

4. RATTRAPAGE (AppendEntries)
   Le leader recule next_index jusqu'au point de divergence
   puis renvoie toutes les entrées manquantes.
   → O(missing) messages pour resynchroniser.
```

## Résultats mesurés

### Scénario 1 — Élection avec terms
```
t=0.00s  5 nœuds → FOLLOWER
t=0.62s  N3 timeout en premier → CANDIDATE (term=1)
t=0.64s  Vote N4 → (2/3) | Vote N2 → (3/3)
t=0.64s  N3 → 👑 LEADER (term=1)

N'importe quel nœud peut gagner (N3 ici, pas N5 comme Bully).
```

### Scénario 2 — Log replication
```
4 commandes soumises → répliquées → committées

N1 log: [1|t1] SET user:1=Alice | [2|t1] SET user:2=Bob | ...
N2 log: [1|t1] SET user:1=Alice | [2|t1] SET user:2=Bob | ...
N3 log: [1|t1] SET user:1=Alice | [2|t1] SET user:2=Bob | ...
...
Logs committés identiques : ✅  (5/5 nœuds)
```

### Scénario 3 — Durabilité après crash du leader
```
N1 committe : {db:host=localhost, db:port=5432, app:version=2.1}
N1 tombe
N4 élu leader (term=2)
N4 state machine : {db:host=localhost, db:port=5432, ...} ✅
Données préservées : ✅
```

### Scénario 4 — Anti split-brain
```
Partition A (N1, N2) : minorité → ne peut pas élire
Partition B (N3, N4, N5) : majorité → élit N5

La partition A ne peut jamais committer de commandes
car elle n'atteint pas le quorum (2 < 3).
→ Impossible d'avoir 2 leaders qui committent simultanément.
```

## Raft vs Bully

```
┌─────────────────────┬──────────────────┬──────────────────────┐
│ Critère             │ Bully (Jour 6)   │ Raft (Jour 7)        │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Messages/élection   │ O(N²)            │ O(N)                 │
│ Qui gagne           │ Plus grand ID    │ Timeout aléatoire    │
│ Split-brain         │ Possible         │ Impossible (quorum)  │
│ Yo-Yo problem       │ Oui              │ Non (terms)          │
│ Données répliquées  │ Non              │ Oui (log garanti)    │
│ Résurrection        │ Reprend le lead  │ Redevient follower   │
└─────────────────────┴──────────────────┴──────────────────────┘
```

## Les 5 Safety Properties de Raft

```
Election Safety     : ≤ 1 leader par term
Leader Append Only  : le leader n'écrase jamais son log
Log Matching        : même (index, term) → logs identiques jusqu'à là
Leader Completeness : tout leader futur a toutes les entrées committées
State Machine Safety: même index → même commande sur tous les nœuds
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `raft.py` | `NoeudRaft`, `Cluster`, `EntreeLog`, RPCs (`RequestVote`, `AppendEntries`) |
| `simulation.py` | 5 scénarios + comparaison Raft/Bully |

## Lancer

```bash
python3 simulation.py
```

## Utilisé en production dans

| Système | Usage |
|---------|-------|
| **etcd** | Configuration Kubernetes |
| **CockroachDB** | Consensus par range de données |
| **TiKV** | Backend distribué de TiDB |
| **Consul** | Service discovery |
| **InfluxDB** | Méta-données cluster |

## Jour 8 → Théorème CAP

Raft choisit CP (Cohérence + tolérance aux Partitions) au détriment
de la Disponibilité : pendant une partition, la minorité refuse
les écritures. Le **Théorème CAP** formalise ce trade-off.
On implémente deux bases de données : une CP et une AP, pour
observer concrètement la différence de comportement.
