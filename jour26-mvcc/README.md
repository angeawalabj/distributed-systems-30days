# Jour 26 — MVCC (Multi-Version Concurrency Control)

## Le problème

Avec des verrous, les lecteurs bloquent les écrivains et vice-versa :

```
SANS MVCC :
  T1 lit   users WHERE id=1  → pose verrou partagé
  T2 écrit users SET age=30  → bloquée en attendant T1
  → Contention, latence, deadlocks potentiels

AVEC MVCC :
  T1 voit un snapshot figé au moment de son BEGIN
  T2 crée une NOUVELLE version (ne touche pas l'ancienne)
  → Lecteurs et écrivains ne se bloquent jamais
```

## Modèle xmin / xmax (PostgreSQL)

```
Chaque version de ligne :
  xmin = XID qui a créé cette version
  xmax = XID qui l'a supprimée/remplacée (0 = vivante)

Visibilité pour transaction T :
  xmin committée AVANT snapshot de T
  ET xmax = 0  OU  xmax non-committée dans snapshot de T
```

## Résultats mesurés

### Scénario 1 — Snapshot isolation

```
État initial : alice = {solde: 1000}

T1 (xid=2) démarre — alice = {solde: 1000}
T2 (xid=3) commite — alice → {solde: 800}

T1 relit alice = {solde: 1000}  ← snapshot protège T1 ✅
T3 (xid=4, après T2) lit alice = {solde: 800}  ✅

Versions de 'alice' :
  Version(val={solde:1000}, xmin=1, xmax=3)  [morte]
  Version(val={solde:800},  xmin=3, xmax=∞)  [vivante]

Deux transactions lisent des valeurs différentes et correctes simultanément ✅
```

### Scénario 2 — 10 lecteurs + 5 écrivains en parallèle

```
Type         N    Moy     P50     P99
Lectures    10   ~0ms    ~0ms    ~0ms
Écritures    5   ~0ms    ~0ms    ~0ms

Erreurs / deadlocks : 0 ✅
Versions créées     : 15 (1 par écriture)
```

### Scénario 3 — RC vs RR

```
alice=1000 → T_ecriture commite alice=800

              Avant commit   Après commit
RC (snapshot/requête)   1000          800   ← voit le changement
RR (snapshot/BEGIN)     1000         1000   ← figé sur son snapshot

READ COMMITTED  : "non-repeatable read" possible ✅ attendu
REPEATABLE READ : lectures stables garanties     ✅ attendu
```

### Scénario 4 — Write Skew (anomalie RR)

```
Invariant : ≥1 médecin en garde
État initial : alice=garde, bob=garde

T1 lit [2 gardes] → OK → alice=congé → commit
T2 lit [2 gardes] → OK → bob=congé  → commit (voit snapshot AVANT T1)

Résultat : 0 gardes → INVARIANT VIOLÉ ❌

→ En SERIALIZABLE réel (SSI PostgreSQL) :
  T2 détecte la dépendance sur les lignes lues par T1
  → T2 abortée → invariant préservé ✅
```

### Scénario 5 — Vacuum

```
Après 10 mises à jour de compte:A :
  versions_totales  : 12   (2 initiales + 10 nouvelles)
  versions_mortes   : 10   (anciennes versions de A)

VACUUM → 10 versions supprimées
  versions_totales  : 2   ✅

Longue transaction bloque le vacuum :
  T_longue active (xid=12)
  5 mises à jour créent 5 versions mortes
  VACUUM pendant T_longue : 0 supprimées  ← bloqué
  Après fin T_longue + VACUUM : 5 supprimées ✅
```

## Anomalies par niveau d'isolation

| Niveau | Dirty Read | Non-Repeatable | Phantom | Write Skew |
|--------|-----------|---------------|---------|-----------|
| READ UNCOMMITTED | ❌ possible | ❌ | ❌ | ❌ |
| READ COMMITTED | ✅ non | ❌ possible | ❌ | ❌ |
| REPEATABLE READ | ✅ non | ✅ non | ❌ possible | ❌ possible |
| SERIALIZABLE | ✅ non | ✅ non | ✅ non | ✅ non |

## Lancer

```bash
python3 simulation.py
```

## Jour 27 → Index : B-Tree, Hash, GiST, Brin

Comment accélérer les requêtes sans scanner toute la table ? Les index sont des structures de données auxiliaires qui mapppent les valeurs aux tuples. B-Tree (la plupart des cas), Hash (equality uniquement, O(1)), GiST (geometries, full-text), BRIN (très grandes tables triées). On implémente un B-Tree et on mesure le gain sur des requêtes de range scan.
