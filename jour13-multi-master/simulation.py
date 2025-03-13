"""
Jour 13 — Simulation : Réplication Multi-Maître
=================================================
5 scénarios :
  1. LWW : le timestamp le plus récent gagne (et perd des données)
  2. LWW avec dérive d'horloge : résultat aléatoire selon le skew
  3. Merge automatique : compteurs, listes, dicts
  4. Résolution applicative : logique métier (fusion de paniers)
  5. Convergence éventuelle : temps pour que tous les nœuds s'accordent
"""

import time
import threading
import random
from multi_master import (
    NoeudMultiMaitre, ClusterMultiMaitre, StrategieConflit, EntreeVersionnee
)

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s): time.sleep(s)

def afficher_valeurs(cluster, cle, label=""):
    if label: print(f"\n  {label}")
    valeurs = cluster.valeurs(cle)
    coherent = len(set(str(v) for v in valeurs.values())) == 1
    for nid, val in sorted(valeurs.items()):
        print(f"    {nid:<14} → {val!r}")
    print(f"    {'─'*40}")
    print(f"    Cohérence : {'✅ tous identiques' if coherent else '⚠️  divergence en cours'}")


# ─── SCÉNARIO 1 : LWW — PERTE SILENCIEUSE ────────────────────────────────────

def scenario_lww():
    titre("SCÉNARIO 1 — Last-Write-Wins : perte silencieuse de données")

    print("""
  Alice (sur DC-Paris) et Bob (sur DC-Tokyo) modifient
  leur profil en même temps. LWW garde le plus récent timestamp.
  La modification la plus ancienne est PERDUE SILENCIEUSEMENT.
    """)

    cluster = ClusterMultiMaitre(
        ["dc-paris", "dc-tokyo", "dc-sydney"],
        strategie=StrategieConflit.LWW,
        latence_ms=30,
    )

    paris  = cluster.noeuds["dc-paris"]
    tokyo  = cluster.noeuds["dc-tokyo"]

    # Écriture initiale
    paris.ecrire("user:alice", {"nom": "Alice", "email": "alice@example.com", "bio": ""})
    attendre(0.2)

    print("  État initial :")
    afficher_valeurs(cluster, "user:alice")

    # Modification simultanée sur deux DCs
    print("\n  Alice modifie sa bio sur DC-Paris ET DC-Tokyo simultanément :\n")

    t1_res = None
    t2_res = None

    def ecrire_paris():
        nonlocal t1_res
        t1_res = paris.ecrire("user:alice", {
            "nom": "Alice", "email": "alice@example.com",
            "bio": "Ingénieure chez Acme (modif Paris)"
        })

    def ecrire_tokyo():
        nonlocal t2_res
        attendre(0.005)   # Tokyo écrit 5ms après Paris
        t2_res = tokyo.ecrire("user:alice", {
            "nom": "Alice", "email": "alice@example.com",
            "bio": "Développeuse freelance (modif Tokyo)"
        })

    th1 = threading.Thread(target=ecrire_paris)
    th2 = threading.Thread(target=ecrire_tokyo)
    th1.start(); th2.start()
    th1.join(); th2.join()

    print(f"  Paris  écrit à ts={t1_res.ts:.4f} : bio='Ingénieure chez Acme'")
    print(f"  Tokyo  écrit à ts={t2_res.ts:.4f} : bio='Développeuse freelance'")
    print(f"  Tokyo a un timestamp plus récent (+5ms)")

    cluster.attendre_convergence("user:alice", timeout=3)
    attendre(0.3)

    print()
    afficher_valeurs(cluster, "user:alice", "État après convergence LWW :")

    valeur_finale = paris.lire("user:alice")
    bio_finale = valeur_finale.get("bio", "") if valeur_finale else ""
    print(f"""
  La bio 'Ingénieure chez Acme' (Paris) est PERDUE SILENCIEUSEMENT.
  Tokyo a gagné car son timestamp était +5ms plus récent.
  Alice ne sait pas que sa modification Paris a été écrasée.

  ❌ Problème LWW :
    → Perte de données non détectée
    → 5ms de latence réseau peut inverser le résultat
    → Les horloges de deux DCs ne sont jamais parfaitement sync
       (NTP garantit ~1-50ms de précision, pas des microsecondes)
    """)

    print(f"  Conflits détectés : {cluster.nb_conflits_total()}")


# ─── SCÉNARIO 2 : DÉRIVE D'HORLOGE ───────────────────────────────────────────

def scenario_skew():
    titre("SCÉNARIO 2 — Dérive d'horloge : LWW devient arbitraire")

    print("""
  DC-Tokyo a une horloge en avance de 200ms (NTP drift).
  Même si Paris écrit APRÈS Tokyo, Tokyo gagne toujours
  car son timestamp est artificiellement plus élevé.

  C'est le problème fondamental de LWW avec des horloges physiques.
    """)

    cluster = ClusterMultiMaitre(
        ["dc-paris", "dc-tokyo"],
        strategie=StrategieConflit.LWW,
        latence_ms=10,
        skews_ms={"dc-paris": 0, "dc-tokyo": 200},  # Tokyo +200ms en avance
    )

    paris = cluster.noeuds["dc-paris"]
    tokyo = cluster.noeuds["dc-tokyo"]

    print("  Simulation : Paris et Tokyo écrivent 5 fois la même clé\n")
    print(f"  {'Écriture':<10} {'Nœud':<12} {'Valeur':<25} {'ts local':<16} {'Gagnant attendu'}")
    print("  " + "─"*72)

    for i in range(5):
        # Paris écrit toujours APRÈS Tokyo (100ms plus tard)
        tokyo.ecrire("config:limite", f"tokyo_v{i}")
        attendre(0.1)   # Paris écrit 100ms APRÈS Tokyo
        paris.ecrire("config:limite", f"paris_v{i}")
        attendre(0.3)   # Laisser converger

        val = paris.lire("config:limite")
        ts_paris = paris.lire_entree("config:limite")
        ts_tokyo = tokyo.lire_entree("config:limite")

        gagnant = "tokyo" if (ts_tokyo and ts_paris and ts_tokyo.ts > ts_paris.ts) else "paris"
        print(f"  Round {i+1:<5} {'paris (après)':<12} {'paris_v' + str(i):<25} "
              f"{'tokyo +200ms':<16} → {gagnant} gagne (skew!)")

    attendre(0.3)
    print(f"""
  Tokyo gagne SYSTÉMATIQUEMENT même si Paris écrit plus tard,
  car son horloge locale est avancée de 200ms.

  Solution : Hybrid Logical Clocks (HLC)
    Combine horloge physique + compteur logique.
    Garantit causalité même avec dérive d'horloge.
    Utilisé par : CockroachDB, YugabyteDB.

  Ou : Vector Clocks (Jour 14)
    Remplace les timestamps physiques par des compteurs logiques.
    Détecte précisément les causalités sans dépendre des horloges.
    """)


# ─── SCÉNARIO 3 : MERGE AUTOMATIQUE ──────────────────────────────────────────

def scenario_merge():
    titre("SCÉNARIO 3 — Merge automatique : compteurs, listes, dicts")

    print("""
  Certains types de données se mergent naturellement :
    → Compteurs de vues : additionner ou prendre le max
    → Panier d'achats   : union des articles
    → Tags utilisateur  : union des ensembles
  """)

    # ── Compteurs ────────────────────────────────────────────────────────────
    print("  3a. Compteur de vues (merge = max des deux valeurs)\n")

    def merge_compteur(a, b):
        """Pour un compteur incrémental : prendre le max."""
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return max(a, b)
        return b

    cluster_c = ClusterMultiMaitre(
        ["dc-eu", "dc-us"],
        strategie=StrategieConflit.MERGE,
        latence_ms=20,
        fn_merge=merge_compteur,
    )

    eu = cluster_c.noeuds["dc-eu"]
    us = cluster_c.noeuds["dc-us"]

    eu.ecrire("video:views", 1000)
    attendre(0.1)

    # Deux incréments simultanés
    eu.ecrire("video:views", 1050)   # 50 vues côté EU
    us.ecrire("video:views", 1080)   # 80 vues côté US (en même temps)

    attendre(0.5)
    cluster_c.attendre_convergence("video:views", timeout=3)

    print(f"  EU écrit 1050 vues, US écrit 1080 vues simultanément")
    afficher_valeurs(cluster_c, "video:views", "Après merge :")
    print(f"  → max(1050, 1080) = 1080 ✅  (pas de perte)")

    # ── Listes ────────────────────────────────────────────────────────────────
    print("\n  3b. Panier d'achats (merge = union)\n")

    cluster_l = ClusterMultiMaitre(
        ["mobile", "desktop"],
        strategie=StrategieConflit.MERGE,
        latence_ms=20,
    )
    mob = cluster_l.noeuds["mobile"]
    dsk = cluster_l.noeuds["desktop"]

    # Panier initial
    mob.ecrire("cart:alice", ["chaussures", "t-shirt"])
    attendre(0.15)

    # Ajouts simultanés sur deux appareils
    mob.ecrire("cart:alice", ["chaussures", "t-shirt", "casquette"])
    dsk.ecrire("cart:alice", ["chaussures", "t-shirt", "veste"])

    attendre(0.5)
    cluster_l.attendre_convergence("cart:alice", timeout=3)

    print(f"  Mobile ajoute 'casquette', Desktop ajoute 'veste' simultanément")
    afficher_valeurs(cluster_l, "cart:alice", "Après merge (union) :")
    print(f"  → Union : ['chaussures', 't-shirt', 'casquette', 'veste'] ✅")

    # ── Dicts ─────────────────────────────────────────────────────────────────
    print("\n  3c. Profil utilisateur (merge = fusion de champs)\n")

    cluster_d = ClusterMultiMaitre(
        ["dc-a", "dc-b"],
        strategie=StrategieConflit.MERGE,
        latence_ms=20,
    )
    a = cluster_d.noeuds["dc-a"]
    b = cluster_d.noeuds["dc-b"]

    a.ecrire("user:bob", {"nom": "Bob", "email": "bob@example.com"})
    attendre(0.15)

    a.ecrire("user:bob", {"nom": "Bob", "email": "bob@example.com", "ville": "Paris"})
    b.ecrire("user:bob", {"nom": "Bob", "email": "bob@example.com", "age": 30})

    attendre(0.5)
    cluster_d.attendre_convergence("user:bob", timeout=3)

    print(f"  DC-A ajoute 'ville:Paris', DC-B ajoute 'age:30' simultanément")
    afficher_valeurs(cluster_d, "user:bob", "Après merge (dict fusion) :")
    print(f"  → {{'nom': 'Bob', 'email': '...', 'ville': 'Paris', 'age': 30}} ✅")


# ─── SCÉNARIO 4 : RÉSOLUTION APPLICATIVE ─────────────────────────────────────

def scenario_custom():
    titre("SCÉNARIO 4 — Résolution applicative : logique métier")

    print("""
  Deux agents de réservation réservent la même chambre d'hôtel
  simultanément sur deux DCs différents.

  LWW  → l'un gagne arbitrairement, l'autre est perdu
  Merge → "fusionner" deux réservations n'a pas de sens
  Custom → règle métier : retenir la réservation avec le tarif le plus élevé
           (ou : signaler un conflit et alerter un agent humain)
    """)

    def resoudre_reservation(loc: EntreeVersionnee, dist: EntreeVersionnee):
        """
        Règle métier : en cas de double réservation,
        on garde celle avec le tarif le plus élevé (meilleur revenu).
        En prod : on alerterait aussi le service client.
        """
        tarif_loc  = loc.valeur.get("tarif", 0)  if isinstance(loc.valeur, dict)  else 0
        tarif_dist = dist.valeur.get("tarif", 0) if isinstance(dist.valeur, dict) else 0
        gagnant    = loc.valeur if tarif_loc >= tarif_dist else dist.valeur
        # Marquer le conflit pour traitement manuel
        return {**gagnant, "_conflit": True, "_concurrent": (loc.noeud_src, dist.noeud_src)}

    cluster = ClusterMultiMaitre(
        ["dc-paris", "dc-london"],
        strategie=StrategieConflit.CUSTOM,
        latence_ms=25,
        fn_custom=resoudre_reservation,
    )

    paris  = cluster.noeuds["dc-paris"]
    london = cluster.noeuds["dc-london"]

    # Double réservation simultanée
    paris.ecrire("chambre:101", {
        "client": "Alice", "tarif": 150, "nuits": 2, "dc": "paris"
    })
    london.ecrire("chambre:101", {
        "client": "Bob", "tarif": 200, "nuits": 3, "dc": "london"
    })

    attendre(0.5)
    cluster.attendre_convergence("chambre:101", timeout=4)

    print(f"  Paris  réserve chambre:101 → Alice,  tarif=150€")
    print(f"  London réserve chambre:101 → Bob,    tarif=200€")
    afficher_valeurs(cluster, "chambre:101", "\n  Résolution custom (tarif le plus élevé gagne) :")

    val = paris.lire("chambre:101")
    if val:
        conflit_detecte = val.get("_conflit", False)
        print(f"\n  Conflit détecté et marqué : {'✅' if conflit_detecte else '❌'}")
        print(f"  → Bob (200€) gagne, mais le flag '_conflit' permet")
        print(f"    au service client de contacter Alice pour la dédommager.")

    conflits = [c for n in cluster.noeuds.values() for c in n.conflits]
    if conflits:
        print(f"\n  Détail du conflit :")
        print(f"    {conflits[0].resume()}")


# ─── SCÉNARIO 5 : CONVERGENCE ÉVENTUELLE ─────────────────────────────────────

def scenario_convergence():
    titre("SCÉNARIO 5 — Convergence éventuelle : temps de synchronisation")

    print("""
  On mesure le temps pour que 5 nœuds convergent
  après des écritures simultanées, selon la latence réseau.
  C'est la fenêtre pendant laquelle les nœuds ont des vues différentes.
    """)

    latences = [10, 30, 100]
    print(f"  {'Latence':>10}  {'Temps convergence':>20}  {'Conflits':>10}  {'Cohérence'}")
    print("  " + "─"*55)

    for lat in latences:
        cluster = ClusterMultiMaitre(
            [f"dc-{i}" for i in range(5)],
            strategie=StrategieConflit.LWW,
            latence_ms=lat,
        )
        noeuds = list(cluster.noeuds.values())

        # Écritures simultanées sur tous les nœuds
        cle = "shared:config"
        for i, n in enumerate(noeuds):
            n.ecrire(cle, f"valeur_dc{i}")

        t0 = time.time()
        converge = cluster.attendre_convergence(cle, timeout=lat * 0.5)
        t_conv = (time.time() - t0) * 1000

        conflits = cluster.nb_conflits_total()
        coherent = cluster.coherence_globale(cle)

        print(f"  {lat:>8}ms  {t_conv:>16.0f}ms  {conflits:>10}  "
              f"{'✅' if coherent else '⚠️  pas encore'}")

    print(f"""
  Observations :
    Latence 10ms  → convergence rapide, fenêtre d'incohérence courte
    Latence 100ms → 500ms+ pendant lesquels les nœuds divergent

  C'est la "fenêtre d'incohérence" inhérente à toute réplication
  asynchrone multi-maître (cohérence éventuelle).

  En pratique :
    → Lecture depuis le nœud local (potentiellement stale)
    → Lecture depuis plusieurs nœuds + quorum (Jour 9)
    → Session consistency : garantir qu'un utilisateur voit
      ses propres écritures (sticky session)
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 13 — RÉPLICATION MULTI-MAÎTRE                    ║")
    print("╚" + "═"*62 + "╝")

    scenario_lww()
    scenario_skew()
    scenario_merge()
    scenario_custom()
    scenario_convergence()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Réplication Multi-Maître — Ce qu'il faut retenir :

    Chaque nœud accepte les écritures → haute disponibilité.
    Réplication asynchrone → cohérence éventuelle.
    Écritures simultanées sur la même clé → CONFLIT.

  Les 4 stratégies de résolution :
    LWW    → simple, perd des données silencieusement, sensible au skew
    FWW    → prévisible, mais arbitraire selon la latence réseau
    Merge  → idéal pour compteurs / listes / dicts, impossible pour tout
    Custom → logique métier respectée, le plus flexible

  Ce que nos scénarios ont prouvé :
    Scénario 1 → LWW : bio Paris perdue silencieusement ❌
    Scénario 2 → Skew 200ms : Tokyo gagne SYSTÉMATIQUEMENT ❌
    Scénario 3 → Merge : compteur max, union liste, fusion dict ✅
    Scénario 4 → Custom : Bob (200€) gagne + flag conflit ✅
    Scénario 5 → Fenêtre d'incohérence ∝ latence réseau

  Lien avec les autres jours :
    Jour 8 (CAP)   : multi-maître = choix AP
    Jour 9 (Quorum): W+R>N réduit la fenêtre d'incohérence
    Jour 14 (Vector Clocks) : détecter PRÉCISÉMENT les conflits
                              sans dépendre des timestamps physiques

  → Jour 14 : Vector Clocks
    LWW utilise les timestamps physiques — imprécis.
    Les Vector Clocks remplacent ça par des compteurs logiques
    par nœud, permettant de savoir si deux écritures sont
    causalement liées ou réellement concurrentes.
  """)


if __name__ == "__main__":
    main()
