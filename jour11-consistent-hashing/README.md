# Jour 11 — Consistent Hashing

## Le problème du modulo naïf

```python
noeud = hash(clé) % N
```

Quand N change de 5 à 6 : `hash(clé) % 5` ≠ `hash(clé) % 6` pour ~83% des clés.
En production avec 10M clés en cache : **8,3 millions de cache misses simultanés**
→ avalanche sur la base de données → incident garanti.

## La solution : Consistent Hashing

On place nœuds **et** clés sur un cercle `[0, 2³²)`.
Une clé appartient au premier nœud rencontré en tournant dans le sens horaire.

```
         0
    N3 ──┼── N1
   /     │     \
  /      │      \
N2       │       N4
  \      │      /
   \     │     /
    N5 ──┴── (clé ici → appartient à N4)
```

**Propriété fondamentale :**
- Ajouter 1 nœud → seulement `1/N` des clés bougent
- Retirer 1 nœud → ses clés vont **uniquement** à son successeur

## Résultats mesurés

### Scénario 1 — Ajout d'un 6ème nœud (10 000 clés)
```
Modulo naïf        : 8321/10000 réaffectées   (83.2%)  ← incident
Consistent hashing : 1608/10000 réaffectées   (16.1%)  ← maîtrisé ✅

→ 5x moins de perturbation.
Retrait d'un nœud  : 1850/10000 réaffectées   (18.5%)  ✅
```

### Scénario 2 — Vnodes : distribution avec V=1 vs V=150
```
Sans vnodes (V=1) :
  node-2 : 22711 clés (45.4%)   ← surchargé
  node-5 :   114 clés  (0.2%)   ← sous-utilisé
  CV = 84%  ❌ catastrophique

Avec vnodes (V=150) :
  node-1 :  9529 clés (19.1%)
  node-2 : 10337 clés (20.7%)
  node-3 : 10881 clés (21.8%)
  CV = 6%  ✅ quasi-uniforme
```

Sans vnodes, un nœud peut recevoir **200x plus** que le minimum.
Avec V=150, l'écart max est de ±10%.

### Scénario 3 — Chirurgie : quelles clés bougent exactement
```
Ajout de N6 (200 clés) :
  N2 → N6 : 10 clés
  N5 → N6 : 7 clés
  N3 → N6 : 5 clés
  N4 → N6 : 4 clés
  N1 → N6 : 3 clés
  Total : 29/200 (14.5%)

Observation : les clés ne vont QUE vers N6.
Aucune migration entre les anciens nœuds. ✅
```

### Scénario 4 — Réplication N=3
```
user:alice     → DC2-Node3  +  DC1-Node1  +  DC1-Node2
order:XR-9042  → DC2-Node1  +  DC2-Node2  +  DC2-Node3

Panne DC2-Node3 → lecture transparente depuis DC1-Node1 ✅
```

### Scénario 5 — Nœuds hétérogènes (poids 4:1)
```
legacy-1  (128 Go, poids=1.0) →  4327 clés   (8.7%)   ~10% attendu
legacy-2  (128 Go, poids=1.0) →  5455 clés  (10.9%)   ~10% attendu
new-gen-1 (512 Go, poids=4.0) → 20283 clés  (40.6%)   ~40% attendu ✅
new-gen-2 (512 Go, poids=4.0) → 19935 clés  (39.9%)   ~40% attendu ✅
```

## Algorithme en détail

### Placement sur l'anneau
```python
# Chaque nœud physique → V positions virtuelles
for i in range(V):
    pos = hash(f"{nom}#vnode{i}") % 2**32
    anneau[pos] = nom

# Lookup : trouver le nœud d'une clé
pos = hash(clé) % 2**32
idx = bisect_left(positions_triees, pos)
idx = idx % len(positions_triees)   # wrap-around
return anneau[positions_triees[idx]]
```

### Réplication N-way
```python
def N_noeuds_pour(cle, N):
    idx = bisect_left(positions, hash(cle))
    noeuds_distincts = []
    for i in range(len(positions)):
        noeud = anneau[positions[(idx + i) % len(positions)]]
        if noeud not in noeuds_distincts:
            noeuds_distincts.append(noeud)
        if len(noeuds_distincts) == N:
            break
    return noeuds_distincts
```

## Choisir V (nombre de vnodes)

| V | CV | Verdict |
|---|-----|---------|
| 1 | ~84% | ❌ Ne pas utiliser en prod |
| 10 | ~24% | ❌ Trop inégal |
| 50 | ~19% | ⚠️ Acceptable pour petits clusters |
| 150 | ~6% | ✅ Bon équilibre mémoire/distribution |
| 256 | ~7% | ✅ Cassandra production default |

## Utilisé en production

| Système | Détails |
|---------|---------|
| **Cassandra** | 256 vnodes par nœud, token-based partitioning |
| **DynamoDB** | Anneau de tokens, réplication sur 3 AZ |
| **Memcached** | Bibliothèque `ketama` (consistent hashing) |
| **Redis Cluster** | 16 384 hash slots répartis sur les nœuds |
| **Nginx** | `hash $request_uri consistent` |
| **Akamai CDN** | Routage des requêtes vers les edge servers |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `consistent_hashing.py` | `AnneauSimple`, `AnneauVnodes`, `ComparateurHashing` |
| `simulation.py` | 5 scénarios : modulo vs consistent, vnodes, chirurgie, réplication, poids |

## Lancer

```bash
python3 simulation.py
```

## Jour 12 → Distributed Transactions (2PC)

On sait maintenant **où** placer les données (Jour 11).
La question suivante : comment modifier atomiquement des données
sur **plusieurs nœuds** ? Transférer 100€ d'Alice vers Bob implique
un débit sur le nœud d'Alice ET un crédit sur le nœud de Bob.
Si le nœud de Bob crashe entre les deux, Alice est débitée sans
que Bob soit crédité. Le **Two-Phase Commit (2PC)** garantit
"tout ou rien" en distribué — avec ses propres limites.
