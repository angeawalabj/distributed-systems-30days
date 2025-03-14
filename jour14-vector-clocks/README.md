# Jour 14 — Vector Clocks (Horloges Vectorielles)

## Le problème du Jour 13

LWW utilise des timestamps physiques :
- Sensible au clock skew (Tokyo +200ms → gagne toujours)
- Ne distingue pas "A a causé B" de "A et B sont vraiment concurrents"
- Perte silencieuse de données

## La solution : Vector Clocks

Chaque nœud maintient un **vecteur de compteurs**, un par nœud connu.

```
VC = {paris: 3, tokyo: 2, sydney: 1}
       ↑            ↑          ↑
  Paris a vu    Tokyo a vu  Sydney a vu
  3 événements  2 événements  1 événement
```

### Règles

```
1. Événement local   → incrémenter son propre compteur
   {paris:2} → {paris:3}

2. Envoyer message   → joindre son VC au message

3. Recevoir message  → max(VC_local, VC_reçu) par composante
                       puis incrémenter son propre compteur
   Local={paris:2, tokyo:1}  +  Reçu={paris:1, tokyo:3}
   → max → {paris:2, tokyo:3} → incrémenter → {paris:3, tokyo:3}
```

### Comparaison

```
VC_A < VC_B  (A précède B) :
  ∀i : A[i] ≤ B[i]  ET  ∃j : A[j] < B[j]
  → Causalité : A a (peut-être) causé B

VC_A ∥ VC_B  (CONCURRENTS — conflit réel) :
  ∃i : A[i] > B[i]  ET  ∃j : A[j] < B[j]
  → Aucun ordre causal → conflit à résoudre
```

## Résultats mesurés

### Scénario 1 — Exemples de comparaison
```
VC{paris:1}         →  VC{paris:2, tokyo:1}    A précède B ✅
VC{paris:2, tokyo:1} ∥  VC{paris:1, tokyo:2}   CONCURRENT ⚠️
VC{paris:3, sydney:1, tokyo:0}  ∥  VC{paris:0, sydney:1, tokyo:2}  CONCURRENT ⚠️
```

### Scénario 2 — VC vs LWW
```
Cas causal : Paris écrit → Tokyo lit puis réécrit
  VC{paris:1} < VC{paris:1, tokyo:1}  → PRÉCÈDE ✅
  LWW et VC concordent.

Cas concurrent : Paris et Tokyo écrivent sans se connaître
  VC{paris:2, tokyo:0} ∥ VC{paris:0, tokyo:2} → CONCURRENT ✅
  LWW : choisit arbitrairement selon le timestamp → perte silencieuse ❌
  VC  : conflit détecté avec certitude, rien n'est perdu ✅
```

### Scénario 3 — Siblings (panier d'achat)
```
Écriture initiale : ['chaussures', 't-shirt']  VC{paris:1}

Écritures concurrentes (même VC de départ) :
  Paris  ajoute 'casquette' → VC{paris:3}
  Tokyo  ajoute 'veste'     → VC{paris:1, tokyo:2}
  Relation : CONCURRENT → 2 siblings créés ⚠️

Lecture depuis Paris :
  sibling 1 : ['chaussures', 't-shirt', 'casquette']  VC{paris:3}
  sibling 2 : ['chaussures', 't-shirt', 'veste']      VC{paris:1, tokyo:2}

Résolution (union) :
  ['chaussures', 't-shirt', 'casquette', 'veste']  VC{paris:5, tokyo:2} ✅
  → Tous les nœuds convergent ✅
```

### Scénario 4 — VC vs Lamport Timestamps
```
Événements A, B, C :
  A : Paris  (Lamport=1,  VC={paris:1})
  B : Tokyo  (Lamport=2,  VC={tokyo:1})         ← indépendant de A
  C : Sydney lit A puis réécrit (Lamport=2,  VC={paris:1, sydney:1})

Lamport : L(B) = L(C) = 2 → "indéterminé" ❌
VC      : VC(B) ∥ VC(C) → CONCURRENT  ✅
          VC(A) < VC(C) → causalité    ✅
```

### Scénario 5 — Wiki distribué (3 DCs, 3 articles)
```
Éditions concurrentes simulées sur article:python et article:rust.

Conflits détectés par VC  : 6
Conflits écrasés par LWW  : ~6  (silencieusement)

Résolution article:python :
  siblings : ['Python 3.12 : nouveautés', 'Python for AI/ML guide']
  merge    : 'Python 3.12 : nouveautés / Python for AI/ML guide'
  VC final : VC{dc-eu:7, dc-us:2, dc-ap:1}
  → plus de conflit après merge ✅
```

## VC vs Lamport vs LWW

```
┌────────────────┬──────────────┬──────────────┬──────────────────┐
│                │ Lamport      │ LWW          │ Vector Clocks    │
│                │ (Jour 3)     │ (Jour 13)    │ (Jour 14)        │
├────────────────┼──────────────┼──────────────┼──────────────────┤
│ Causalité      │ ✅ partielle │ ❌ aucune    │ ✅ exacte        │
│ Concurrence    │ ⚠️  faux pos │ ❌ ignorée   │ ✅ précise       │
│ Perte données  │ possible     │ ❌ silencieux │ ✅ jamais        │
│ Clock skew     │ sensible     │ ❌ critique  │ ✅ immunisé      │
│ Complexité     │ O(1)         │ O(1)         │ O(N nœuds)       │
└────────────────┴──────────────┴──────────────┴──────────────────┘
```

## Limites et alternatives

**Problème** : pour 1000 nœuds → 1000 entiers par entrée → overhead mémoire.

**Solutions** :
- **Dotted Version Vectors** : compresse en ne gardant que les deltas
- **Interval Tree Clocks** : distribue les "identités" dynamiquement
- **CRDTs** (Jour suivant) : structures auto-fusionnables sans conflits explicites

## Utilisé en production

| Système | Usage |
|---------|-------|
| **Riak** | Siblings + résolution applicative |
| **Amazon DynamoDB** | Version vectors internes |
| **Git** | DAG de commits (équivalent discret) |
| **CouchDB** | Détection de conflits entre révisions |
| **Voldemort** (LinkedIn) | Version vectors pour les objets |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `vector_clocks.py` | `VectorClock`, `Relation`, `NoeudVC`, `EntreeVC` |
| `simulation.py` | 5 scénarios : comparaison, VC vs LWW, siblings, vs Lamport, système complet |

## Lancer

```bash
python3 simulation.py
```

## Jour 15 → Distributed Locking (Redlock)

On sait maintenant détecter les conflits de versions (Jour 14).
Mais parfois on veut les **prévenir** plutôt que les détecter.
Le **Distributed Locking** avec Redis (algorithme Redlock) garantit
qu'un seul service à la fois peut accéder à une ressource critique —
sans les verrous bloquants de 2PC et sans coordinateur central permanent.
