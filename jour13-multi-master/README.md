# Jour 13 — Réplication Multi-Maître

## Le problème du single-master

Avec un seul maître :
- Le maître est un **goulot d'étranglement** (toutes les écritures passent par lui)
- Le maître est un **SPOF** (s'il tombe, plus d'écritures)
- Latence élevée pour les utilisateurs géographiquement éloignés

## La solution : Multi-Maître

Chaque datacenter accepte les écritures **localement**, puis les réplique de façon **asynchrone** vers les autres.

```
DC-Paris  ←──────────────── DC-Tokyo
    │   réplication async        │
    └────────────────────────────┘
    Chacun peut écrire.
    Si deux écrivent la même clé en même temps → CONFLIT.
```

## Les 4 stratégies de résolution

### 1. Last-Write-Wins (LWW)
Le timestamp le plus récent gagne.
```
Paris  écrit bio="Ingénieure chez Acme"     ts=1234.1671
Tokyo  écrit bio="Développeuse freelance"   ts=1234.1734  ← gagne

→ bio Paris PERDUE SILENCIEUSEMENT ❌
```
**Problème** : 5ms de latence réseau peut changer le résultat. NTP garantit ~1-50ms de précision, pas la microseconde.

### 2. Dérive d'horloge (Clock Skew)
Tokyo a une horloge avancée de +200ms :
```
5 rounds consécutifs : Tokyo gagne SYSTÉMATIQUEMENT
même si Paris écrit 100ms APRÈS Tokyo.
→ LWW est imprévisible avec des horloges déphasées ❌
```

### 3. Merge automatique
```
Compteurs  : max(1050, 1080) = 1080  ✅  (pas de perte)
Listes     : union(['chaussures','casquette'], ['chaussures','veste'])
           = ['chaussures', 'casquette', 'veste']  ✅
Dicts      : {ville: Paris} + {age: 30} = {ville: Paris, age: 30}  ✅
```
Fonctionne pour les types composables. Impossible pour du texte libre.

### 4. Résolution applicative (Custom)
```
Paris  réserve chambre:101 → Alice, 150€
London réserve chambre:101 → Bob,   200€

Règle métier : tarif le plus élevé gagne
→ Bob (200€) retenu + flag _conflit=True pour alerter le service client ✅
```

## Résultats mesurés

### Convergence éventuelle (5 nœuds)
```
Latence  10ms → convergence en   50ms   (fenêtre d'incohérence)
Latence  30ms → convergence en  153ms
Latence 100ms → convergence en  558ms
```

La fenêtre d'incohérence est proportionnelle à la latence réseau. Pendant cette fenêtre, des lectures depuis différents nœuds retournent des valeurs différentes.

## Comparaison des stratégies

```
┌────────────┬──────────────┬───────────────┬──────────────────────┐
│ Stratégie  │ Perte données│ Complexité    │ Utilisé par          │
├────────────┼──────────────┼───────────────┼──────────────────────┤
│ LWW        │ ❌ Oui       │ ✅ Triviale   │ Cassandra, DynamoDB  │
│ FWW        │ ❌ Oui       │ ✅ Simple     │ Systèmes réservation │
│ Merge      │ ✅ Non       │ ⚠️  Moyenne   │ Git, Google Docs     │
│ Custom     │ ✅ Non       │ ❌ Élevée     │ CouchDB, Riak        │
└────────────┴──────────────┴───────────────┴──────────────────────┘
```

## Note sur la cohérence des listes

Dans le scénario 3b, les deux nœuds contiennent les mêmes éléments mais dans des ordres différents (`['casquette', 'veste']` vs `['veste', 'casquette']`). La comparaison par string les signale comme divergents, mais sémantiquement ils sont équivalents. Les **CRDTs** (Jour 14+) résolvent ce problème avec des ensembles ordonnés de façon déterministe.

## Utilisé en production

| Système | Stratégie | Détails |
|---------|-----------|---------|
| **Cassandra** | LWW | Timestamp par cellule |
| **DynamoDB** | LWW par défaut | Custom avec Lambda functions |
| **CouchDB** | Custom | Remonte les conflits à l'app |
| **MySQL Group Replication** | Certification-based | Détection + rollback |
| **PostgreSQL BDR** | Configurable | LWW, custom, ou abort |
| **Git** | Merge | 3-way merge, résolution manuelle si conflit |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `multi_master.py` | `NoeudMultiMaitre`, `ClusterMultiMaitre`, `EvenementConflit`, 4 stratégies |
| `simulation.py` | 5 scénarios : LWW, skew, merge, custom, convergence |

## Lancer

```bash
python3 simulation.py
```

## Jour 14 → Vector Clocks

LWW utilise les timestamps physiques — imprécis et sensibles au skew.
Les **Vector Clocks** remplacent ça par des **compteurs logiques par nœud** :
```
VC = {paris: 3, tokyo: 2, sydney: 1}
```
Deux écritures avec des VCs comparables → causalité (l'une précède l'autre).
Deux écritures avec des VCs incomparables → conflit réel détecté avec certitude.
Plus besoin d'horloges synchronisées. Utilisé par Riak, DynamoDB (versions).
