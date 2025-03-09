# Jour 9 — Quorum (Lecture/Écriture)

## Le concept

Le Jour 8 présentait CP et AP comme un choix binaire.
Quorum révèle la réalité : c'est un **curseur continu**.

```
N = répliques totales
W = confirmations requises en écriture
R = réponses requises en lecture

W + R > N  →  cohérence FORTE  (chevauchement garanti)
W + R ≤ N  →  cohérence ÉVENTUELLE (stale reads possibles)
```

## La preuve mathématique

Si on écrit sur W nœuds et lit depuis R nœuds, et que W + R > N,
alors au moins 1 nœud est dans les **deux** ensembles.
Ce nœud a forcément la dernière valeur écrite.

```
N=3, W=2, R=2 (W+R=4 > 3) :

  Écriture → [N1 ✅] [N2 ✅] [N3 ⏸]
  Lecture  → [N1 ✅] [N2 ✅]

  N1 ou N2 est dans les deux ensembles → valeur à jour garantie.
  Mathématiquement impossible de lire une valeur périmée.
```

## Résultats mesurés

### Scénario 1 — Table de vérité
```
W=2, R=2  QUORUM    → 4 > 3  ✅ FORTE
W=1, R=3  R=ALL     → 4 > 3  ✅ FORTE
W=3, R=1  W=ALL     → 4 > 3  ✅ FORTE
W=1, R=2  W+R=N     → 3 ≤ 3  ❌ ÉVENTUELLE
W=1, R=1  ONE/ONE   → 2 ≤ 3  ❌ ÉVENTUELLE
```

### Scénario 2 — Benchmark (N=5, latence=10ms/nœud)
```
Config               W  R  W+R>N   Écriture   Lecture   Cohérence
ONE/ONE              1  1  non        9ms         9ms    ÉVENT. ⚠️
W=1, R=QUORUM        1  3  non        9ms        11ms    ÉVENT. ⚠️
W=QUORUM, R=1        3  1  non       12ms         9ms    ÉVENT. ⚠️
QUORUM/QUORUM        3  3  oui       12ms        11ms    FORTE  ✅
W=1, R=ALL           1  5  oui        9ms        16ms    FORTE  ✅
W=ALL, R=1           5  1  oui       15ms         8ms    FORTE  ✅
ALL/ALL              5  5  oui       16ms        15ms    FORTE  ✅
```

QUORUM est le sweet spot : cohérence forte pour +3ms vs ONE/ONE.

### Scénario 3 — Stale reads avec W=1, R=1
```
Écriture prix=150 sur N1 seulement (W=1).
N2 et N3 ont toujours prix=100.

Lecture 1 (N1) → 150 ✅ À jour
Lecture 2 (N2) → 100 ⚠️  STALE
Lecture 3 (N3) → 100 ⚠️  STALE
Lecture 4 (N1) → 150 ✅ À jour
Lecture 5 (N2) → 100 ⚠️  STALE
Lecture 6 (N3) → 100 ⚠️  STALE

4/6 lectures retournent l'ancienne valeur — comportement attendu.
```

### Scénario 4 — Read Repair automatique
```
N3 redémarre, perd ses données.

AVANT lecture :
  N1 → 'v2.0' | N2 → 'v2.0' | N3 → ∅

READ (R=2) → retourne 'v2.0' au client ✅
             + envoie 'v2.0' à N3 en arrière-plan

APRÈS lecture :
  N1 → 'v2.0' | N2 → 'v2.0' | N3 → 'v2.0' ✅

Read repairs effectués : 2
N3 guéri sans aucune intervention humaine.
```

### Scénario 5 — Tolérance aux pannes (N=5, QUORUM W=R=3)
```
5/5 actifs (0 mort)  → ✅ OK
4/5 actifs (1 mort)  → ✅ OK
3/5 actifs (2 morts) → ✅ OK   ← limite
2/5 actifs (3 morts) → ❌ FAIL
1/5 actifs (4 morts) → ❌ FAIL

QUORUM sur N=5 tolère 2 pannes simultanées.
```

## Recettes de configuration

```
┌────────────┬─────┬─────┬────────────────────────────────────┐
│ Recette    │  W  │  R  │ Cas d'usage                        │
├────────────┼─────┼─────┼────────────────────────────────────┤
│ ONE / ONE  │  1  │  1  │ Logs, métriques IoT, compteurs     │
│ W=1, R=Q   │  1  │N/2+1│ Catalogue (100 lectures / 1 écriture)│
│ W=Q, R=1   │N/2+1│  1  │ Ingestion capteurs, write-heavy    │
│ QUORUM     │N/2+1│N/2+1│ Sessions, profils — recommandé     │
│ W=ALL, R=1 │  N  │  1  │ Clés de chiffrement, tokens        │
│ W=1, R=ALL │  1  │  N  │ Config critique en lecture seule   │
└────────────┴─────┴─────┴────────────────────────────────────┘
```

## Lien avec les autres jours

| Jour | Concept | Lien avec Quorum |
|------|---------|-----------------|
| Jour 7 (Raft) | Consensus | Raft utilise W=quorum en interne pour committer |
| Jour 8 (CAP) | CP vs AP | W+R>N = CP, W+R≤N = AP — c'est le même curseur |
| Jour 4 (Heartbeat) | Pannes | Le coordinateur sait quels nœuds sont vivants avant d'envoyer |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `quorum.py` | `NoeudReplique`, `CoordinateurQuorum`, `Versioned`, Read Repair |
| `simulation.py` | 6 scénarios : preuve, benchmark, stale reads, repair, pannes, guide |

## Lancer

```bash
python3 simulation.py
```

## Jour 10 → Gossip Protocol

Le Read Repair guérit les nœuds au fil des lectures,
mais les clés "froides" (jamais lues) restent périmées indéfiniment.
Le **Gossip Protocol** résout ça : chaque nœud parle périodiquement
à quelques voisins aléatoires pour synchroniser son état.
Comme une rumeur qui se propage dans une foule — O(log N) rounds
suffisent pour atteindre tous les 1000 nœuds d'un cluster.
