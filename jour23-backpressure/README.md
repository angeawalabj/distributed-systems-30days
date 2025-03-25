# Jour 23 — Backpressure & Queue Management

## Le problème

Producteur à 1000/s, consommateur à 100/s. Sans contrôle la queue grossit sans fin → OOM.

**Différence avec Rate Limiting (Jour 22) :**
- Rate Limiting : le **serveur** dit non au client (`429 Too Many Requests`)
- Backpressure : le **consommateur** signale sa saturation au producteur pour qu'il ralentisse

## Les 4 stratégies

```
Queue pleine → que faire ?

BLOCK       : bloquer le producteur jusqu'à de la place (0 perte, propagation amont)
DROP_NEWEST : rejeter la nouvelle requête (protège ce qui est déjà en file)
DROP_OLDEST : éjecter la plus ancienne (fraîcheur des données prioritaire)
PRIORITY    : éjecter la basse priorité pour accueillir la haute
```

## Résultats mesurés

### Scénario 1 — 60 tâches, queue capacité=10, 2 workers à 20ms

```
Stratégie       Entrées  Traitées  Perdues  Max Q  Lat P50  Lat P99
BLOCK                60        60        0     10    123ms    266ms
DROP_NEWEST          60        12       48     10     62ms    103ms
DROP_OLDEST          60        12       48     10     61ms    102ms
PRIORITY             60        12       48     10     61ms    102ms
```

BLOCK = 0 perte mais latence 2× plus haute (producteurs bloqués).

### Scénario 2 — Pression comme thermomètre

```
 Temps   Tâches   Queue   Pression  Barre
  0.0s        5       5       25%  █████░░░░░░░░░░░░░░░  phase 1 (léger)
  0.3s       15      10       50%  ██████████░░░░░░░░░░  phase 2 (modéré)
  0.6s       35      20      100%  ████████████████████  phase 3 (pic)
  1.1s       35      10       50%  ██████████░░░░░░░░░░  phase 4 (récup.)
  1.9s       35       0        0%  ░░░░░░░░░░░░░░░░░░░░  phase 5 (calme)
```

### Scénario 3 — Priority Queue (SLA tiers)

```
Tier        Soumises  Traitées  Perdues  Taux OK
PREMIUM          20        15        5      75%   ← jamais éjecté
STANDARD         20         0       20       0%   ← éjecté si nécessaire
FREE             20         0       20       0%   ← éjecté en premier
```

### Scénario 4 — Work Stealing

```
Distribution fixe (80 tâches → worker 0 uniquement) :
  Tâches par worker : [80, 0, 0, 0]
  Durée             : 1216ms

Avec work stealing :
  Tâches par worker : [21, 20, 19, 20]   ← équilibré automatiquement
  Vols effectués    : 59
  Durée             : 355ms

Accélération : 3.4× (max théorique : 4×) ✅
```

### Scénario 5 — Load Shedding adaptatif

```
Phase             Pression   Rejet%   Acceptées
Léger (10/s)         20%       0%          10
Modéré (30/s)        80%      37%          19
Pic (60/s)           80%      80%          12
Très fort (100/s)    85%      86%          14
Retour calme         25%      30%           7

Régulation douce : le taux de rejet monte avec la pression (pas de cliff-edge).
```

## Comparaison des stratégies

| Stratégie | Perte | Latence | Données fraîches | SLA diff. | Usage |
|-----------|-------|---------|-----------------|-----------|-------|
| BLOCK | 0 | Haute | Non | Non | Pipelines données |
| DROP_NEWEST | Oui | Basse | Non | Non | Logs, métriques |
| DROP_OLDEST | Oui | Basse | **Oui** | Non | IoT, trading |
| PRIORITY | Oui (bas) | Basse | Non | **Oui** | APIs premium |

## Work Stealing

```
Sans : worker 0 = 1216ms   workers 1-3 = idle
Avec : tous les 4 workers actifs en parallèle = 355ms (3.4×)

Algorithme :
  1. Chaque worker a sa propre deque locale
  2. Worker idle → cherche un voisin avec > 1 tâche
  3. Vole la tâche la plus récente (fin de deque) pour minimiser la contention

Utilisé par : ForkJoinPool (Java), Tokio (Rust), Go runtime scheduler
```

## Lancer

```bash
python3 simulation.py
```

## Jour 24 → Write-Ahead Log (WAL) & Storage Engine

Comment une base de données garantit la **durabilité** des données même en cas de crash. Tout `COMMIT` = écriture dans le WAL avant les fichiers de données. On implémente un mini moteur de stockage avec WAL, recovery après crash, et checkpointing.
