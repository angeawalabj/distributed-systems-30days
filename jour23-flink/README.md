# Jour 23 — Flink : "La Fenêtre du Temps"

## Le problème

Les événements arrivent en continu. Comment calculer "le CA des 5 dernières minutes" sans attendre la fin du monde ?

```
BATCH (Spark) : attendre toutes les données → traiter → résultat
  Latence : minutes à heures
  Adapté  : rapports quotidiens, ML training

STREAM (Flink) : traiter chaque événement à son arrivée → résultat continu
  Latence : millisecondes à secondes
  Adapté  : dashboards temps réel, détection de fraude, alertes
```

## Les 3 types de fenêtres

### Tumbling Window (fenêtre fixe)

```
├──10min──┤├──10min──┤├──10min──┤
[0-10min] [10-20min] [20-30min]

→ Chaque événement appartient exactement à 1 fenêtre
→ Agrégats périodiques : CA par heure, requêtes par minute
```

### Sliding Window (fenêtre glissante)

```
t=0  ├────10min────┤
t=5     ├────10min────┤
t=10        ├────10min────┤

Taille=10min, pas=5min → fenêtres se chevauchent
→ Chaque événement appartient à plusieurs fenêtres
→ Détection d'anomalies, moyennes mobiles
```

### Session Window (fenêtre de session)

```
[event event event]  30s gap  [event event]  30s gap  [event]
←── session 1 ────→            ←─ session 2 →          ← s3 →

→ Se ferme après N secondes d'inactivité
→ Taille variable, adaptée au comportement utilisateur
```

## Event Time vs Processing Time

```
Processing Time : l'heure de la machine qui reçoit le message
Event Time      : l'heure à laquelle l'événement s'est produit

Problème : mobile hors-ligne → batch d'événements à la reconnexion
           → event_time = hier, processing_time = maintenant

Watermark = max(event_times vus) - tolérance_retard
→ Signal "je suis sûr que tous les events avant T sont arrivés"
→ Flink peut fermer les fenêtres avant T
```

## Résultats mesurés

### Scénario 1 — Tumbling Window (CA par heure)

```
Fenêtre               Marchand     Transactions        CA
───────────────────────────────────────────────────────
Heure 1 (0–3600s)  amazon                 11    1151€
Heure 1 (0–3600s)  netflix                 5     644€
Heure 1 (0–3600s)  spotify                 9     795€
Heure 2 (3600–7200s)  spotify              8     914€
...
CA total toutes fenêtres : 6261€
```

### Scénario 2 — Sliding Window (pic CPU)

```
Métriques CPU 20 minutes, pic entre 8-12min :

  [6min–11min]   CPU=66.2%  🔴 PIC DÉTECTÉ
  [7min–12min]   CPU=77.0%  🔴 PIC DÉTECTÉ
  [8min–13min]   CPU=87.6%  🔴 PIC DÉTECTÉ  ← maximum
  [9min–14min]   CPU=75.8%  🔴 PIC DÉTECTÉ
  [10min–15min]  CPU=64.7%  🔴 PIC DÉTECTÉ

5 fenêtres en alerte, CPU max 87.6%
```

### Scénario 3 — Session Window

```
alice    session 1   t=2s     13s réelle   5 actions
alice    session 2   t=72s     6s réelle   3 actions
bob      session 1   t=5s      3s réelle   4 actions

alice : gap de 55s > 30s → nouvelle session ✅
carol : timestamps espacés de ~49s > 30s → 5 sessions d'1 action
```

### Scénario 4 — Watermarks

```
Impact de la tolérance sur les résultats :

  Tolérance    Fenêtre [0-60s]    Late events ignorés
  ─────────────────────────────────────────────────
        0s    somme=225  n=8       4 ignorés
        5s    somme=225  n=8       3 ignorés
       15s    somme=225  n=8       1 ignoré

Trade-off : tolérance haute → résultats complets, latence accrue
```

### Scénario 5 — Détection de fraude

```
Règle : dépense > 500€ en 5 minutes → alerte

✅ Normal  — alice : max 207€/5min
🚨 FRAUDE  — bob   : [8min–13min] 1436€ > 500€ (achats en rafale)
🚨 FRAUDE  — carol : [3min–8min]  673€  > 500€  (transaction 600€ unique)

Pipeline production :
  Source Kafka → Sliding Window 5min/1min → Sink Kafka "fraud-alerts"
  Latence : < 1s entre la transaction et l'alerte
```

## Architecture opérateur

```python
OperateurTumbling(taille_s=3600, fn_agregat=somme_montants)
OperateurSliding(taille_s=300, pas_s=60, fn_agregat=moyenne_cpu)
OperateurSession(gap_s=30, fn_agregat=compter)
```

## Fichiers

| Fichier | Contenu |
|---------|---------|
| `flink.py` | `OperateurTumbling`, `OperateurSliding`, `OperateurSession`, `GestionnaireWatermark`, `Evenement` |
| `simulation.py` | 5 scénarios : tumbling, sliding, session, watermarks, fraude |

## Lancer

```bash
python3 simulation.py
```

## Jour 24 → Zero-Trust Networking

Kafka et Flink communiquent entre eux sur le réseau interne.  
Dans un modèle traditionnel, ce trafic interne est "de confiance" — faux.  
Zero-Trust : chaque connexion est authentifiée, même à l'intérieur du réseau.  
mTLS, identité de workload, moindre privilège.
