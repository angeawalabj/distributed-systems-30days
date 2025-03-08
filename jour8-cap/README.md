# Jour 8 — Théorème CAP

## Le théorème en une phrase

Un système distribué ne peut garantir simultanément que **2 des 3** :
**C**ohérence, **A**vailability (Disponibilité), tolérance aux **P**artitions.

Les partitions réseau étant inévitables en production, le vrai choix est **CP ou AP**.

## Les deux bases implémentées

### Base CP — "Je préfère dire ERREUR plutôt que mentir"
```python
def ecrire(self, cle, valeur):
    if not self._a_quorum():
        return ERREUR("Partition détectée, quorum non atteint")
    # Sinon : écriture + réplication synchrone
```

### Base AP — "Je préfère répondre avec des données anciennes que ne pas répondre"
```python
def ecrire(self, cle, valeur):
    self._store[cle] = valeur   # Toujours accepté localement
    self._repliquer_async()     # Réplication en arrière-plan
    return OK
```

## Résultats mesurés

### Scénario 2 — Partition {N1,N2} | {N3,N4,N5}
```
Écriture depuis N1 (minorité, 2/5 nœuds) :

  CP → ❌ ERREUR  : "2/5 nœuds visibles, quorum=3 requis. REFUS."
  AP → ✅ OK      : valeur=50, répliqué sur N2 seulement

État AP après :
  N1 → stock=50   N2 → stock=50   (groupe A)
  N3 → stock=100  N4 → stock=100  N5 → stock=100  (groupe B)
  → Deux vérités simultanées.
```

### Scénario 3 — Surréservation AP
```
chambres_disponibles = 1
Partition : {N1,N2} | {N3,N4}
Alice écrit depuis N1 → OK (chambres=0)
Bob   écrit depuis N3 → OK (chambres=0)

❌ SURRÉSERVATION : 2 clients ont réservé la même chambre.
La base AP a honoré les deux (disponibilité maintenue).
```

### Scénario 4 — Réconciliation Last-Write-Wins
```
Pendant partition :
  Groupe A : config:timeout = "60s"
  Groupe B : config:timeout = "10s"  (timestamp > groupe A)

Après guérison :
  Tous → config:timeout = "10s"  (LWW : B gagne)
  ⚠️  La valeur "60s" est silencieusement perdue.
```

### Scénario 5 — PACELC (latence sans partition)
```
20 écritures, sans aucune partition :

  CP  ████████████████████████  10.30ms/écriture  (sync)
  AP  █                          0.80ms/écriture  (async)

Facteur : 13x plus rapide en AP.
En production multi-DC : 200ms vs 5ms.
```

## Le vrai trade-off visualisé

```
       COHÉRENCE
           │
     CP    │
  (HBase,  │
   etcd,   │    Le triangle CAP
  Spanner) │    On ne peut être
           │    que sur 2 côtés
───────────┼──────────── PARTITION
           │    TOLÉRANCE
     AP    │
 (Cassandra│
  Dynamo,  │
   Riak)   │
           │
      DISPONIBILITÉ
```

## Systèmes réels et leur position

| Système | Choix | Cohérence | Notes |
|---------|-------|-----------|-------|
| etcd | CP | Forte | Base de Kubernetes |
| CockroachDB | CP | Sérialisable | SQL distribué |
| Cassandra | AP | Éventuelle | Configurable par requête |
| DynamoDB | AP/CP | Configurable | `ConsistentRead=true` pour CP |
| MongoDB | CP | Forte | Configurable |
| Redis (cluster) | AP | Éventuelle | AOF pour durabilité |

## Quand choisir quoi

```
CP : stock, soldes, réservations uniques, auth, config critique
AP : compteurs, logs, sessions, catalogue, métriques IoT
Hybride : les deux dans la même app selon la donnée
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `cap.py` | `NoeudCP`, `NoeudAP`, `PartitionReseau`, `ResultatOp` |
| `simulation.py` | 6 scénarios dont surréservation, LWW et benchmark PACELC |

## Lancer

```bash
python3 simulation.py
```

## Jour 9 → Quorum (Lecture/Écriture)

Le théorème CAP présente CP et AP comme un choix binaire.
En réalité, Cassandra et DynamoDB exposent un **curseur** :
les niveaux de cohérence `W` (writes) et `R` (reads).

Quand `W + R > N` (nombre de nœuds), toute lecture voit
la dernière écriture — même sur une base "AP".
On implémente ce mécanisme et on visualise le trade-off
latence/cohérence pour chaque combinaison (W=1/R=N, W=N/R=1, etc.).
