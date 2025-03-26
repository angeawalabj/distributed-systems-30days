"""
Jour 23 — "La Fenêtre du Temps" — Simulation Flink
====================================================
5 scénarios :
  1. Tumbling window : CA par heure, comptage de requêtes
  2. Sliding window  : moyenne mobile, détection de pic
  3. Session window  : durée de session utilisateur
  4. Event Time vs Processing Time : événements en retard + watermarks
  5. Détection d'anomalie : fraude en temps réel avec fenêtre glissante
"""

import time
import random
from collections import defaultdict
from flink import (
    Evenement, OperateurTumbling, OperateurSliding,
    OperateurSession, GestionnaireWatermark
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def ts(t): return time.strftime("%H:%M:%S", time.localtime(t))


# ─── FONCTIONS D'AGRÉGAT ─────────────────────────────────────────────────────

def somme_montants(events):
    return sum(e.valeur.get("montant", 0) for e in events)

def moyenne_cpu(events):
    return round(sum(e.valeur.get("cpu", 0) for e in events) / len(events), 1)

def compter(events):
    return len(events)

def max_montant(events):
    return max(e.valeur.get("montant", 0) for e in events)


# ─── SCÉNARIO 1 : TUMBLING WINDOW ────────────────────────────────────────────

def scenario_tumbling():
    titre("SCÉNARIO 1 — Tumbling Window : CA horaire et comptage de requêtes")

    print("""
  Fenêtres fixes non-chevauchantes.
  Chaque événement appartient exactement à 1 fenêtre.

  [─── Heure 1 ───][─── Heure 2 ───][─── Heure 3 ───]
     purchases          purchases          purchases
     → CA H1            → CA H2            → CA H3
    """)

    # Simuler 3h de transactions (event_time = 0 à 10800s)
    T = 0         # t=0 = début de la simulation
    HEURE = 3600  # secondes

    op = OperateurTumbling(
        taille_s=HEURE,
        fn_agregat=somme_montants,
        max_retard_s=60,
    )

    # Générer des achats sur 3 heures pour 3 marchands
    marchands = ["amazon", "netflix", "spotify"]
    events_par_heure = defaultdict(lambda: defaultdict(int))

    for h in range(3):
        for _ in range(random.randint(15, 25)):
            marchand = random.choice(marchands)
            t_event  = T + h * HEURE + random.uniform(0, HEURE)
            montant  = random.randint(5, 200)
            events_par_heure[h][marchand] += montant
            e = Evenement(
                cle         = marchand,
                valeur      = {"montant": montant},
                event_time  = t_event,
            )
            op.traiter(e)

    resultats = op.vider()
    resultats.sort(key=lambda r: (r["debut"], r["cle"]))

    print(f"  CA par marchand par heure :\n")
    print(f"  {'Fenêtre':<20}  {'Marchand':<10}  {'Transactions':>13}  {'CA':>8}")
    print(f"  " + "─"*55)
    for r in resultats:
        h = int(r["debut"] / HEURE)
        print(f"  Heure {h+1} ({r['debut']:.0f}–{r['fin']:.0f}s)  "
              f"{r['cle']:<10}  {r['nb']:>13}  {r['resultat']:>6}€")

    total = sum(r["resultat"] for r in resultats)
    print(f"\n  CA total toutes fenêtres : {total}€")
    print(f"""
  Tumbling window en production :
    Rapports horaires (CA, nb commandes, taux conversion)
    Métriques d'infrastructure agrégées par minute
    Comptage de requêtes par API endpoint par heure
    """)


# ─── SCÉNARIO 2 : SLIDING WINDOW ─────────────────────────────────────────────

def scenario_sliding():
    titre("SCÉNARIO 2 — Sliding Window : moyenne mobile et détection de pic CPU")

    print("""
  Fenêtres glissantes : taille=5min, pas=1min.
  Un événement appartient à 5 fenêtres différentes.

  t=0  ├────5min────┤
  t=1     ├────5min────┤
  t=2         ├────5min────┤
  ...
  → Lissage du signal → détection des tendances
    """)

    MINUTE = 60
    T = 0

    op = OperateurSliding(
        taille_s=5 * MINUTE,    # Fenêtre de 5 minutes
        pas_s=1 * MINUTE,       # Glisse toutes les minutes
        fn_agregat=moyenne_cpu,
    )

    # Simuler 20 minutes de métriques CPU d'un serveur
    # Avec un pic anormal entre t=8min et t=12min
    print(f"  Métriques CPU sur 20 minutes (pic entre 8-12min) :\n")
    for t_min in range(20):
        for _ in range(6):  # 6 mesures par minute
            t_event = T + t_min * MINUTE + random.uniform(0, MINUTE)
            # CPU normal : 20-40%, pic : 80-95%
            if 8 <= t_min <= 12:
                cpu = random.uniform(80, 95)
            else:
                cpu = random.uniform(20, 40)
            op.traiter(Evenement(
                cle        = "server-1",
                valeur     = {"cpu": cpu},
                event_time = t_event,
            ))

    resultats = op.calculer(T, T + 20 * MINUTE, cle="server-1")

    print(f"  {'Fenêtre (min)':<18}  {'Nb mesures':>11}  {'CPU moy':>8}  Alerte")
    print(f"  " + "─"*50)
    for r in resultats:
        debut_m = int((r["debut"] - T) / MINUTE)
        fin_m   = int((r["fin"]   - T) / MINUTE)
        alerte  = "🔴 PIC DÉTECTÉ" if r["resultat"] > 60 else "🟢"
        print(f"  [{debut_m:>2}min – {fin_m:>2}min]       "
              f"{r['nb']:>11}  {r['resultat']:>7}%  {alerte}")

    pics = [r for r in resultats if r["resultat"] > 60]
    print(f"\n  Fenêtres en alerte : {len(pics)}")
    print(f"  CPU max détecté   : {max(r['resultat'] for r in resultats):.1f}%")
    print(f"""
  Sliding window en production :
    Moyenne mobile du latence P99 sur les 5 dernières minutes
    Détection de DDoS (nb requêtes/5min glissantes)
    Anomalie de fraude (montant moyen glissant par utilisateur)
    """)


# ─── SCÉNARIO 3 : SESSION WINDOW ─────────────────────────────────────────────

def scenario_session():
    titre("SCÉNARIO 3 — Session Window : durée de session utilisateur")

    print("""
  La fenêtre de session se ferme après N secondes d'inactivité.
  Pas de taille fixe — elle s'adapte au comportement de l'utilisateur.

  user1: [click click click]  30s sans activité  [click click]
          ←── session 1 ────→                     ←─ session 2 →

  Cas d'usage : temps passé sur le site, parcours d'achat, abandon panier.
    """)

    GAP = 30   # 30s d'inactivité → nouvelle session

    op = OperateurSession(gap_s=GAP, fn_agregat=compter)

    users = ["alice", "bob", "carol"]
    sessions_reelles = {
        "alice": [(0, [2, 5, 8, 12, 15]),     # Session 1 : 5 actions
                  (70, [72, 75, 78])],          # Session 2 : 3 actions (gap 55s)
        "bob":   [(5, [5, 6, 7, 8])],           # 1 session courte : 4 actions
        "carol": [(0, [1, 50, 100, 150, 200])], # 1 longue session espacée
    }

    for user, sessions in sessions_reelles.items():
        for session_debut, timestamps in sessions:
            for t in timestamps:
                op.traiter(Evenement(
                    cle        = user,
                    valeur     = {"action": "click"},
                    event_time = float(t),
                ))

    resultats = op.vider()
    resultats.sort(key=lambda r: (r["cle"], r["debut"]))

    print(f"  {'Utilisateur':<12}  {'Session':<10}  {'Début':>6}  "
          f"{'Durée réelle':>12}  {'Actions':>8}")
    print(f"  " + "─"*56)

    sessions_par_user = defaultdict(int)
    for r in resultats:
        sessions_par_user[r["cle"]] += 1
        snum = sessions_par_user[r["cle"]]
        print(f"  {r['cle']:<12}  session {snum:<3}   "
              f"t={r['debut']:.0f}s  "
              f"{r['duree']:>8.0f}s réelle  "
              f"{r['nb']:>8} actions")

    print(f"""
  alice  : 2 sessions (gap de {70-15}s > {GAP}s → nouvelle session) ✅
  bob    : 1 session courte (actions groupées)
  carol  : 1 longue session (gap entre actions < {GAP}s ? selon les timestamps)

  Session window en production :
    Google Analytics : durée de session, pages par session
    E-commerce : détection d'abandon de panier (session fermée sans achat)
    Support : durée d'une conversation avec un agent
    """)


# ─── SCÉNARIO 4 : EVENT TIME + WATERMARKS ────────────────────────────────────

def scenario_watermarks():
    titre("SCÉNARIO 4 — Event Time vs Processing Time : gérer les événements tardifs")

    print("""
  Les événements arrivent souvent en retard :
    - Mobile hors-ligne → batch d'événements à la reconnexion
    - Réseau lent → messages dans le désordre
    - Producteur différé → timestamps dans le passé

  Processing Time : l'heure qu'il est quand le message arrive
  Event Time      : l'heure à laquelle l'événement s'est PRODUIT

  Watermark = max(event_times) - tolérance
  → Flink attend jusqu'au watermark avant de fermer une fenêtre
    """)

    # Simuler des événements en retard
    BASE = 1000.0   # t=1000s comme base

    # Flux d'événements avec retards variables
    flux = [
        # (event_time, processing_delay, valeur)
        (BASE + 0,   0,    10),   # à l'heure
        (BASE + 5,   1,    20),   # 1s de retard
        (BASE + 10,  0,    15),   # à l'heure
        (BASE + 3,   12,   30),   # !! arrivé tard : event_time=3s mais arrive à t=12s
        (BASE + 8,   8,    25),   # arrivé tard
        (BASE + 60,  0,    40),   # dans la fenêtre suivante
        (BASE + 2,   65,   50),   # très tard : fenêtre probablement déjà fermée
        (BASE + 70,  0,    35),   # à l'heure (fenêtre 2)
    ]

    tolerances = [0, 5, 15]

    print(f"  Flux d'événements :\n")
    print(f"  {'Event time':>11}  {'Arr. à':>8}  {'Retard':>7}  Valeur")
    print(f"  " + "─"*42)
    for et, delay, val in flux:
        pt = et + delay
        print(f"  t={et-BASE:>4.0f}s      arr={pt-BASE:>4.0f}s  "
              f"{delay:>5}s    {val}")

    print(f"\n  Impact de la tolérance watermark sur les résultats :\n")
    print(f"  {'Tolérance':>10}  {'Fenêtre [0-60s]':>17}  "
          f"{'Fenêtre [60-120s]':>19}  Late events")
    print(f"  " + "─"*62)

    for tol in tolerances:
        wm = GestionnaireWatermark(max_retard_s=tol)
        op = OperateurTumbling(taille_s=60, fn_agregat=somme_montants,
                               max_retard_s=tol)

        for et, delay, val in sorted(flux, key=lambda x: x[0] + x[1]):
            pt = et + delay
            e  = Evenement(cle="server", valeur={"montant": val},
                           event_time=et, processing_time=pt)
            op.traiter(e)

        resultats = op.vider()
        sommes = defaultdict(int)
        nbs    = defaultdict(int)
        for r in resultats:
            fenetre = int((r["debut"] - BASE) / 60)
            sommes[fenetre] += r["resultat"]
            nbs[fenetre]    += r["nb"]

        late = sum(1 for et, delay, _ in flux
                   if delay > tol and et < BASE + 60)
        print(f"  {tol:>8}s    "
              f"somme={sommes.get(0, 0):>4}  n={nbs.get(0, 0)}      "
              f"somme={sommes.get(1, 0):>4}  n={nbs.get(1, 0)}      "
              f"{late} ignorés")

    print(f"""
  Tolérance 0s  : les événements tardifs sont perdus → somme sous-estimée
  Tolérance 15s : tous les événements de la fenêtre 1 sont inclus ✅

  Trade-off watermark :
    Tolérance haute → résultats plus complets → latence plus grande
    Tolérance basse → résultats rapides → risque d'oublier des événements tardifs
    """)


# ─── SCÉNARIO 5 : DÉTECTION DE FRAUDE ────────────────────────────────────────

def scenario_fraude():
    titre("SCÉNARIO 5 — Détection de fraude en temps réel avec fenêtre glissante")

    print("""
  Règle anti-fraude :
    Si un utilisateur dépense plus de 500€ en 5 minutes → alerte fraude.
    Utiliser une sliding window de 5min / pas 1min.
    """)

    MINUTE = 60
    T      = 0

    op = OperateurSliding(
        taille_s=5 * MINUTE,
        pas_s=1 * MINUTE,
        fn_agregat=somme_montants,
    )

    # alice  : comportement normal
    # bob    : fraude à t=10min (série d'achats en rafale)
    # carol  : comportement normal avec un pic isolé

    transactions = []
    for t_min in range(20):
        t_base = T + t_min * MINUTE
        # alice : achats réguliers ~30€/min
        transactions.append(("alice", t_base + random.uniform(0, MINUTE),
                             random.randint(20, 50)))
        # bob : normal puis fraude entre 10-13min
        if 10 <= t_min <= 13:
            for _ in range(4):
                transactions.append(("bob", t_base + random.uniform(0, MINUTE),
                                    random.randint(80, 150)))
        else:
            transactions.append(("bob", t_base + random.uniform(0, MINUTE),
                                random.randint(10, 40)))
        # carol : une transaction élevée à t=7min
        if t_min == 7:
            transactions.append(("carol", t_base + 5, 600))
        else:
            transactions.append(("carol", t_base + random.uniform(0, MINUTE),
                                random.randint(5, 30)))

    for user, t_event, montant in transactions:
        op.traiter(Evenement(
            cle        = user,
            valeur     = {"montant": montant},
            event_time = t_event,
        ))

    SEUIL = 500
    alertes = defaultdict(list)

    print(f"  Seuil d'alerte : {SEUIL}€ sur 5 minutes\n")

    for user in ["alice", "bob", "carol"]:
        resultats = op.calculer(T, T + 20 * MINUTE, cle=user)
        max_fenetre = max(resultats, key=lambda r: r["resultat"]) if resultats else None
        for r in resultats:
            if r["resultat"] > SEUIL:
                debut_m = int((r["debut"] - T) / MINUTE)
                fin_m   = int((r["fin"]   - T) / MINUTE)
                alertes[user].append((debut_m, fin_m, r["resultat"]))

    for user in ["alice", "bob", "carol"]:
        if alertes[user]:
            print(f"  🚨 FRAUDE DÉTECTÉE — {user} :")
            for debut_m, fin_m, total in alertes[user][:3]:
                print(f"     [{debut_m}min–{fin_m}min] : {total}€ > {SEUIL}€")
        else:
            max_r = max(op.calculer(T, T + 20 * MINUTE, cle=user),
                        key=lambda r: r["resultat"], default={"resultat": 0})
            print(f"  ✅ Normal — {user} : max {max_r['resultat']}€/5min "
                  f"(seuil {SEUIL}€)")

    print(f"""
  bob déclenche l'alerte entre 10-15min (achats en rafale)
  carol : une seule transaction élevée mais fenêtres adjacentes sous le seuil
  alice : comportement normal, jamais proche du seuil

  En production (Flink + Kafka) :
    Source     : Kafka topic "transactions"
    Opérateur  : Sliding window 5min / 1min
    Sink       : Kafka topic "fraud-alerts" → équipe fraude
    Latence    : < 1s entre la transaction et l'alerte
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 23 — 'LA FENÊTRE DU TEMPS' (FLINK)               ║")
    print("╚" + "═"*62 + "╝")

    scenario_tumbling()
    scenario_sliding()
    scenario_session()
    scenario_watermarks()
    scenario_fraude()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Flink — Ce qu'il faut retenir :

    Tumbling : intervalles fixes, chaque événement → 1 fenêtre
    Sliding  : fenêtres qui se chevauchent, événement → N fenêtres
    Session  : fenêtre basée sur l'inactivité, taille variable

  Event Time vs Processing Time :
    Watermark = max(event_time) - tolérance → signal de fermeture de fenêtre
    Tolérance haute → résultats complets mais latence accrue
    Tolérance basse → latence faible mais événements tardifs perdus

  Ce que nos scénarios ont prouvé :
    Scénario 1 → CA par marchand par heure en 3 fenêtres tumbling ✅
    Scénario 2 → pic CPU 8-12min détecté sur 5 fenêtres sliding ✅
    Scénario 3 → alice : 2 sessions séparées par un gap de 55s ✅
    Scénario 4 → tolérance 15s = 0 événement perdu vs 0s = late events ignorés ✅
    Scénario 5 → fraude bob détectée : >500€ en 5min (10-15min) ✅

  Utilisé en production :
    Uber      → surge pricing : tarifs mis à jour toutes les 2min (tumbling)
    LinkedIn  → détection d'activité suspecte (sliding sur connexions)
    Netflix   → monitoring qualité vidéo en temps réel (tumbling 1min)
    Alibaba   → traitement de 250 000 transactions/s lors du Single Day

  → Jour 24 — "Confiance Zéro" (Zero-Trust Networking)
    On a vu comment distribuer les données (HDFS) et les événements (Kafka/Flink).
    Maintenant : comment sécuriser les communications entre ces services ?
    Zéro confiance = chaque connexion est authentifiée, même à l'intérieur du réseau.
  """)

if __name__ == "__main__":
    main()
