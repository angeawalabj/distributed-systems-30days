# Jour 4 — Heartbeat : Détection de pannes

## Le problème fondamental

Un nœud silencieux dans un réseau distribué est **ambigu** :
est-il en train de calculer intensément, ou est-il planté depuis
10 minutes ? Sans mécanisme actif de signalement, on ne peut pas savoir.

## Solution : le Heartbeat

Chaque nœud envoie périodiquement un signal "je suis vivant".
Le moniteur central tient une machine à états pour chaque nœud :

```
  ALIVE ──── silence > timeout ────► SUSPECT
    ▲                                    │
    │                             silence > timeout×2
    │                                    ▼
    └──── heartbeat reçu ──────────── DEAD
```

## Résultats mesurés (exécution réelle)

### Scénario 2 — Panne franche
```
t+0.00s   DB-Primary CRASH
t+1.06s   🟡 SUSPECT détecté    (1er timeout dépassé)
t+1.96s   🔴 MORT confirmé      (2ème timeout)

→ Détection complète en ~2 secondes sur des heartbeats de 0.3s
```

### Scénario 3 — Panne lente (dégradation)
```
Intervalle initial : 0.3s  → timeout calibré à ~1.1s
Après ralentissement 0.5→0.8→1.2→1.8s :
  timeout s'adapte à 2.87s (absorbe la variabilité)
  → Aucun faux positif pendant la dégradation
  → SUSPECT déclenché seulement après arrêt complet
```

### Scénario 4 — Résurrection
```
t+1.00s   CRASH
t+2.11s   SUSPECT
t+3.16s   DEAD
t+3.71s   Redémarrage
t+3.90s   💚 ALIVE (0.19s après le premier heartbeat reçu)
```

### Scénario 5 — Cascade de pannes (8 nœuds)
```
4 nœuds tombent → 4 détectés DEAD en ~2s chacun
Vivants : API-2, API-3, Worker-2, DB-Replica
Morts   : API-1, Worker-1, DB-Primary, Cache
→ Déclenchement automatique du failover DB
```

## Le timeout adaptatif — innovation clé

Un timeout **fixe** crée deux problèmes :
- Trop court → faux positifs (nœuds déclarés morts par erreur)
- Trop long → détection lente (incident non détecté)

Notre implémentation calcule le timeout dynamiquement :
```python
timeout = (moyenne_délais + écart_max) × facteur(3.0)
```

Après calibration sur 10 heartbeats :
- Web-1 : intervalle moy=0.300s → timeout=1.19s (×4.0)
- DB-1  : intervalle moy=0.298s → timeout=1.09s (×3.7)

Le facteur absorbe naturellement les pics réseau sans faux positifs.

## Ce que le heartbeat contient

```python
@dataclass
class Heartbeat:
    expediteur:    str    # identité du nœud
    timestamp:     float  # heure d'envoi
    sequence:      int    # détecte les pertes de messages
    charge_cpu:    float  # métadonnée de diagnostic
    nb_connexions: int    # charge actuelle
```

Le **numéro de séquence** est crucial : si on reçoit seq=10 après seq=7,
on sait que 2 heartbeats ont été perdus en transit — réseau dégradé
même si le nœud est encore vivant.

## Limites du heartbeat simple

```
✅ Détecte : nœud mort, nœud lent, perte de messages
❌ Ne distingue pas : nœud mort VS réseau partitionné
   → Les deux cas produisent le même silence
   → Solution : Quorum + consensus (Jour 9 & 10)
```

## En production

| Système | Mécanisme |
|---------|-----------|
| Kubernetes | `livenessProbe` + `readinessProbe` HTTP |
| Consul | Health checks HTTP/TCP avec TTL |
| ZooKeeper | Sessions avec TTL, ephemeral znodes |
| etcd | Lease TTL (heartbeat implicite) |
| TCP OS | `keepalive` (niveau couche 4) |

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `heartbeat.py` | `MoniteurHeartbeat`, `NoeudReseau`, `EtatNoeud`, timeout adaptatif |
| `simulation.py` | 5 scénarios réels avec timings mesurés |

## Lancer

```bash
python3 simulation.py
```

## Jour 5 → Idempotence

Maintenant qu'on détecte les pannes, un nouveau problème émerge :
quand un nœud est déclaré mort et qu'un client **renvoie** sa requête
vers un autre nœud, que se passe-t-il si la première requête avait
quand même abouti ? On se retrouve avec une commande passée deux fois,
un paiement débité deux fois. L'idempotence résout ça.
