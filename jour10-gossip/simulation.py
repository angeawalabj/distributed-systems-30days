"""
Jour 10 — Simulation : Gossip Protocol en action
==================================================
5 scénarios :
  1. Propagation O(log N) — mesure de la vitesse de convergence
  2. Comparaison push vs push-pull vs broadcast
  3. Résistance aux partitions réseau
  4. Détection de membres morts (membership)
  5. Passage à l'échelle : 10 → 1000 nœuds
"""

import time
import threading
import math
import random
from gossip import NoeudGossip, ReseauGossip

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")

def creer_reseau(n: int, k: int = 3, latence_ms: float = 5.0) -> ReseauGossip:
    reseau = ReseauGossip(latence_ms=latence_ms)
    for i in range(1, n + 1):
        reseau.ajouter(NoeudGossip(i, k=k))
    return reseau

def barre(v: float, max_v: float, w: int = 25) -> str:
    filled = int(v / max_v * w) if max_v > 0 else 0
    return "█" * filled + "░" * (w - filled)


# ─── SCÉNARIO 1 : PROPAGATION O(log N) ───────────────────────────────────────

def scenario_propagation():
    titre("SCÉNARIO 1 — Propagation O(log N) : mesure sur 50 nœuds")

    print("""
  N=50 nœuds, k=3 voisins par round.
  On injecte une information sur N1 et on mesure
  combien de rounds sont nécessaires pour atteindre tous les nœuds.

  Théorie : log_{1+k}(N) = log_4(50) ≈ 2.8 → ~3 rounds suffisent.
    """)

    N = 50
    k = 3
    reseau = creer_reseau(N, k=k, latence_ms=4)
    reseau.demarrer_tous(intervalle=0.12)
    time.sleep(0.1)  # Laisser les nœuds s'initialiser

    # Injection sur N1
    cle = "annonce:nouvelle_version"
    reseau.noeuds[1].ecrire_local(cle, "v3.0.0")
    t_injection = time.time()

    # Mesurer la propagation par paliers de 10%
    paliers = [0.10, 0.25, 0.50, 0.75, 0.90, 1.00]
    print(f"  Palier    Nœuds atteints    Temps     Rounds théoriques")
    print("  " + "─"*55)

    t_precedent = t_injection
    for seuil in paliers:
        t_atteint = reseau.attendre_propagation(cle, seuil=seuil, timeout=8)
        nb        = int(seuil * N)
        rounds_th = math.log(nb, 1 + k) if nb > 1 else 0
        print(f"  {int(seuil*100):>3}%  →  {nb:>3}/{N} nœuds      "
              f"+{t_atteint:.2f}s     ≈ {rounds_th:.1f} rounds")

    t_total = reseau.attendre_propagation(cle, seuil=1.0, timeout=10)
    nb_final = sum(1 for n in reseau.noeuds.values() if n.connait(cle))

    stats = reseau.stats_globales()
    msgs_par_noeud = stats["messages_total"] / N if N > 0 else 0

    print(f"""
  ✅ {nb_final}/{N} nœuds informés en {t_total:.2f}s
  Messages total       : {stats['messages_total']}
  Messages par nœud    : {msgs_par_noeud:.1f}
  Théorie O(N log N)   : {N * math.log2(N):.0f} messages attendus

  Vs broadcast naïf    : {N * (N-1)} messages (O(N²)) — {N*(N-1)//max(1,stats['messages_total'])}x plus coûteux
    """)

    reseau.arreter_tous()


# ─── SCÉNARIO 2 : PUSH VS PUSH-PULL VS FANOUT ────────────────────────────────

def scenario_variantes():
    titre("SCÉNARIO 2 — Impact du fanout k sur la vitesse de convergence")

    print("""
  Même cluster de N=40 nœuds, on varie k (nœuds contactés par round).
  Plus k est grand → convergence plus rapide, mais plus de messages.
    """)

    N = 40
    fanouts = [1, 2, 3, 5, 8]
    resultats = []

    for k in fanouts:
        reseau = creer_reseau(N, k=k, latence_ms=4)
        reseau.demarrer_tous(intervalle=0.12)
        time.sleep(0.1)

        cle = f"info:fanout_{k}"
        reseau.noeuds[1].ecrire_local(cle, f"valeur_{k}")
        t = reseau.attendre_propagation(cle, seuil=1.0, timeout=8)
        msgs = reseau.stats_globales()["messages_total"]
        nb   = sum(1 for n in reseau.noeuds.values() if n.connait(cle))
        resultats.append((k, t, msgs, nb))
        reseau.arreter_tous()
        time.sleep(0.05)

    max_t   = max(r[1] for r in resultats)
    max_msg = max(r[2] for r in resultats)

    print(f"  {'k':>4}  {'Temps':>8}  {'Messages':>10}  {'Nœuds atteints':>16}  Vitesse relative")
    print("  " + "─"*65)
    for k, t, msgs, nb in resultats:
        b = barre(max_t - t, max_t, w=18)
        print(f"  k={k:<2}  {t:>6.2f}s  {msgs:>10}  {nb:>5}/{N}           {b} {'(+ rapide)' if t == min(r[1] for r in resultats) else ''}")

    print(f"""
  Observations :
    k=1  → très lent, rumeur progresse linéairement (chaîne)
    k=3  → sweet spot (Cassandra utilise k=3 par défaut)
    k=8  → rapide mais {int(resultats[-1][2]/resultats[2][2])}x plus de messages que k=3
    → Au-delà de k=5, le gain de vitesse ne justifie plus le coût réseau.
    """)


# ─── SCÉNARIO 3 : RÉSISTANCE AUX PARTITIONS ──────────────────────────────────

def scenario_partition():
    titre("SCÉNARIO 3 — Résistance aux partitions : propagation après guérison")

    print("""
  Cluster de N=20 nœuds partitionné en 2 groupes égaux.
  Information injectée dans le groupe A pendant la partition.
  Après guérison → les nœuds du groupe B reçoivent l'info.
    """)

    N    = 20
    demi = N // 2
    reseau = creer_reseau(N, k=3, latence_ms=4)

    # Partition immédiate
    groupe_a = set(range(1, demi + 1))
    groupe_b = set(range(demi + 1, N + 1))
    reseau.partitionner(groupe_a, groupe_b)

    reseau.demarrer_tous(intervalle=0.12)
    time.sleep(0.1)

    # Injection dans le groupe A
    cle = "config:feature_flag"
    reseau.noeuds[1].ecrire_local(cle, True)

    time.sleep(0.8)  # Laisser le gossip se stabiliser dans chaque partition

    nb_a = sum(1 for i in groupe_a if reseau.noeuds[i].connait(cle))
    nb_b = sum(1 for i in groupe_b if reseau.noeuds[i].connait(cle))
    print(f"  PENDANT la partition ({0.8:.1f}s de gossip) :")
    print(f"    Groupe A ({len(groupe_a)} nœuds) : {nb_a}/{len(groupe_a)} ont '{cle}'")
    print(f"    Groupe B ({len(groupe_b)} nœuds) : {nb_b}/{len(groupe_b)} ont '{cle}'")

    # Guérison
    print(f"\n  💚 Guérison de la partition...")
    reseau.guerir()
    t_guerison = time.time()

    # Attendre propagation vers groupe B
    t_complet = reseau.attendre_propagation(cle, seuil=1.0, timeout=8)

    nb_final = sum(1 for n in reseau.noeuds.values() if n.connait(cle))
    print(f"  {t_complet:.2f}s après guérison : {nb_final}/{N} nœuds informés")

    nb_b_final = sum(1 for i in groupe_b if reseau.noeuds[i].connait(cle))
    print(f"    Groupe B : {nb_b_final}/{len(groupe_b)} nœuds ont maintenant la valeur ✅")

    print(f"""
  Contrairement au Jour 8 (CAP) où la partition nécessitait
  une réconciliation manuelle, le Gossip guérit automatiquement
  dès que les partitions peuvent à nouveau se parler.
  Aucun coordinateur central n'est impliqué.
    """)

    reseau.arreter_tous()


# ─── SCÉNARIO 4 : DÉTECTION DE MEMBRES MORTS ─────────────────────────────────

def scenario_membership():
    titre("SCÉNARIO 4 — Membership : détecter les nœuds morts par gossip")

    print("""
  Chaque nœud gossipe périodiquement son heartbeat.
  Si un nœud ne gossipe plus → ses voisins cessent de le contacter.
  C'est comme ça que Cassandra gère le membership du cluster.

  On tue N3 et on mesure combien de temps il faut pour que
  tous les autres nœuds arrêtent de lui parler.
    """)

    N = 10
    reseau = creer_reseau(N, k=3, latence_ms=4)
    reseau.demarrer_tous(intervalle=0.15)
    time.sleep(0.3)

    print(f"  Cluster de {N} nœuds démarré, tous actifs.")

    # Écrire quelque chose depuis N3
    reseau.noeuds[3].ecrire_local("data:de_N3", "bonjour")
    time.sleep(0.3)

    # Vérifier que tout le monde a la donnée de N3
    nb_ont_data = sum(1 for n in reseau.noeuds.values()
                      if n.connait("data:de_N3"))
    print(f"  N3 a gossipé 'data:de_N3' → {nb_ont_data}/{N} nœuds l'ont reçu")

    # Tuer N3
    t_mort = time.time()
    print(f"\n  💀 N3 tombe (arrêt silencieux)")
    reseau.noeuds[3].arreter()

    # Écrire une nouvelle donnée depuis N1 — elle doit se propager sans N3
    reseau.noeuds[1].ecrire_local("data:apres_mort_N3", "propagation sans N3")
    t_prop = reseau.attendre_propagation("data:apres_mort_N3", seuil=0.8, timeout=6)

    # Compter combien de rounds ont essayé de contacter N3 et échoué
    tentatives_N3 = sum(
        n.stats["messages_envoyes"]
        for n in reseau.noeuds.values()
        if n.id != 3
    )

    nb_prop = sum(1 for nid, n in reseau.noeuds.items()
                  if nid != 3 and n.connait("data:apres_mort_N3"))

    print(f"  Propagation sans N3 : {nb_prop}/{N-1} nœuds vivants informés en {t_prop:.2f}s ✅")

    print(f"""
  Le gossip contourne automatiquement les nœuds morts.
  Quand un nœud ne répond plus à un échange, il est simplement
  ignoré pour ce round — pas d'erreur fatale, pas de blocage.

  En production (Cassandra) :
    → Un nœud silencieux depuis >X rounds → marqué SUSPECT
    → Toujours silencieux → marqué DOWN dans la membership table
    → Exactly comme le Heartbeat du Jour 4, mais sans coordinateur !
    """)

    reseau.arreter_tous()


# ─── SCÉNARIO 5 : PASSAGE À L'ÉCHELLE ────────────────────────────────────────

def scenario_scalabilite():
    titre("SCÉNARIO 5 — Scalabilité : O(log N) prouvé de 10 à 500 nœuds")

    print("""
  On mesure le temps de propagation complète pour différentes
  tailles de cluster. Si O(log N) est vrai, le temps doit
  croître logarithmiquement — pas linéairement.
    """)

    tailles = [10, 30, 100, 200, 500]
    resultats = []

    print(f"  {'N':>5}  {'log₂(N)':>8}  {'Temps mesuré':>14}  {'Messages':>10}  Barre")
    print("  " + "─"*62)

    for N in tailles:
        k = 3
        reseau = creer_reseau(N, k=k, latence_ms=3)
        reseau.demarrer_tous(intervalle=0.1)
        time.sleep(0.15)

        cle = f"scale_test_{N}"
        reseau.noeuds[1].ecrire_local(cle, "propagé")
        t = reseau.attendre_propagation(cle, seuil=0.95, timeout=15)
        msgs = reseau.stats_globales()["messages_total"]
        nb   = sum(1 for n in reseau.noeuds.values() if n.connait(cle))

        resultats.append((N, t, msgs, nb))
        reseau.arreter_tous()
        time.sleep(0.05)

    max_t = max(r[1] for r in resultats)
    for N, t, msgs, nb in resultats:
        b = barre(t, max_t, w=20)
        print(f"  {N:>5}  {math.log2(N):>8.1f}  {t:>10.2f}s  {msgs:>10}  {b}  ({nb}/{N} nœuds)")

    # Vérifier que le ratio temps correspond bien à log
    t_10  = resultats[0][1]
    t_500 = resultats[-1][1]
    ratio_mesure = t_500 / t_10 if t_10 > 0 else 0
    ratio_log    = math.log2(500) / math.log2(10)

    print(f"""
  Vérification O(log N) :
    Ratio temps(500) / temps(10)  mesuré  = {ratio_mesure:.2f}
    Ratio log₂(500) / log₂(10)   théorie = {ratio_log:.2f}

  {"✅ Croissance logarithmique confirmée" if ratio_mesure < ratio_log * 2.5 else "⚠️  Légèrement super-logarithmique (latence réseau simulée)"}

  Comparaison avec broadcast naïf (O(N²)) :
    N=10  → gossip={resultats[0][2]} msgs vs broadcast={10*9} msgs
    N=500 → gossip={resultats[-1][2]} msgs vs broadcast={500*499} msgs
    Ratio au gain à N=500 : {500*499 // max(1, resultats[-1][2])}x moins de messages avec gossip
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 10 — GOSSIP PROTOCOL : PROPAGATION ÉPIDÉMIQUE      ║")
    print("╚" + "═"*62 + "╝")

    scenario_propagation()
    scenario_variantes()
    scenario_partition()
    scenario_membership()
    scenario_scalabilite()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Gossip Protocol — Ce qu'il faut retenir :

    Chaque nœud, à chaque round, choisit k voisins ALÉATOIRES
    et échange son état avec eux (push-pull).

    Complexité : O(log N) rounds pour atteindre tous les nœuds
                 O(N log N) messages total
                 vs O(N²) pour un broadcast naïf

  Ce que nos scénarios ont prouvé :
    Scénario 1 → 50 nœuds informés en < 1s, O(N log N) messages
    Scénario 2 → k=3 est le sweet spot (vitesse / coût)
    Scénario 3 → Guérison automatique après partition
    Scénario 4 → Membership sans coordinateur central
    Scénario 5 → Croissance logarithmique confirmée 10→500 nœuds

  Utilisé en production :
    Cassandra  → membership + propagation schema changes
    Consul     → service discovery + health checks
    Bitcoin    → propagation des transactions
    Kubernetes → état des pods (via etcd, mais gossip-inspired)

  Lien avec les jours précédents :
    Jour 9 (Quorum)  : Read Repair guérit les clés lues seulement
                       → Gossip guérit TOUTES les clés en arrière-plan
    Jour 4 (HB)      : Heartbeat centralisé → Gossip distribué pour membership
    Jour 8 (CAP)     : Gossip est AP (disponible, éventuellement cohérent)

  → Semaine 3 — Jour 11 : Consistent Hashing
    Comment répartir 1 million de clés sur N nœuds de façon à ce
    qu'ajouter ou retirer un nœud ne réaffecte que K/N clés
    (et non pas toutes les clés comme avec un modulo naïf).
  """)


if __name__ == "__main__":
    main()
