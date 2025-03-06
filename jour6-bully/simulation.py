"""
Jour 6 — Simulation : Bully Algorithm
=======================================
5 scénarios :
  1. Élection initiale au démarrage
  2. Mort du leader → réélection automatique
  3. Résurrection du leader → reprend le pouvoir (bully !)
  4. Pannes multiples simultanées → tempête de messages
  5. Réseau avec pertes → robustesse de l'algorithme
"""

import time
import threading
from bully import Noeud, Reseau, EtatNoeud

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=62): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")

def creer_cluster(n: int, latence_ms=20, taux_perte=0.0) -> tuple[Reseau, dict[int, Noeud]]:
    """Crée un cluster de n nœuds avec IDs 1..n."""
    reseau = Reseau(latence_ms=latence_ms, taux_perte=taux_perte)
    noeuds = {}
    for i in range(1, n + 1):
        noeud = Noeud(i, reseau)
        reseau.ajouter(noeud)
        noeuds[i] = noeud
    return reseau, noeuds

def attendre_leader(reseau: Reseau, timeout=5.0) -> int | None:
    """Attend qu'un leader stable émerge."""
    debut = time.time()
    while time.time() - debut < timeout:
        leaders = [n for n in reseau.noeuds.values()
                   if n.etat == EtatNoeud.LEADER and n._actif]
        if len(leaders) == 1:
            # Vérifier que tous les vivants connaissent ce leader
            vivants = [n for n in reseau.noeuds.values() if n._actif]
            if all(n.leader_id == leaders[0].id for n in vivants):
                return leaders[0].id
        time.sleep(0.1)
    return None

def arreter_tous(reseau: Reseau):
    for n in reseau.noeuds.values():
        n._actif = False


# ─── SCÉNARIO 1 : ÉLECTION INITIALE ──────────────────────────────────────────

def scenario_election_initiale():
    titre("SCÉNARIO 1 — Élection initiale au démarrage du cluster")

    print("""
  5 nœuds démarrent simultanément, personne n'est leader.
  Chacun lance une élection → propagation des messages
  → le nœud N5 (ID le plus élevé) doit gagner.
    """)

    reseau, noeuds = creer_cluster(5, latence_ms=15)

    # Tous démarrent en même temps
    for n in noeuds.values():
        n.demarrer()

    # Chaque nœud lance une élection au démarrage
    for n in noeuds.values():
        threading.Thread(target=n.lancer_election, args=("démarrage",),
                         daemon=True).start()

    leader_id = attendre_leader(reseau, timeout=5)

    reseau.afficher_journal(n=25)
    reseau.afficher_etat()

    print(f"\n  Leader élu : N{leader_id} {'✅' if leader_id == 5 else '❌ (attendu N5)'}")
    print(f"""
  Déroulement type :
    N1 envoie ELECTION à N2,N3,N4,N5
    N2,N3,N4,N5 répondent OK → N1 abandonne
    N2 envoie ELECTION à N3,N4,N5 → idem
    ...
    N5 envoie ELECTION → personne de supérieur
    N5 broadcast COORDINATOR à N1,N2,N3,N4
    """)

    arreter_tous(reseau)


# ─── SCÉNARIO 2 : MORT DU LEADER ─────────────────────────────────────────────

def scenario_mort_du_leader():
    titre("SCÉNARIO 2 — Mort du leader → réélection automatique")

    print("""
  Le leader N5 tombe en panne. Les autres nœuds détectent
  l'absence de heartbeat → lancent une nouvelle élection.
  N4 (prochain plus grand ID vivant) doit gagner.
    """)

    reseau, noeuds = creer_cluster(5, latence_ms=15)
    for n in noeuds.values():
        n.demarrer()

    # Election initiale
    for n in noeuds.values():
        threading.Thread(target=n.lancer_election, args=("init",), daemon=True).start()

    leader_id = attendre_leader(reseau, timeout=5)
    print(f"\n  Leader initial : N{leader_id}")

    t_crash = time.time() - reseau.t0
    print(f"\n  💥 t+{t_crash:.2f}s  N{leader_id} CRASH !")
    noeuds[leader_id].arreter()

    # Attendre la réélection
    leader2 = attendre_leader(reseau, timeout=6)
    t_elu = time.time() - reseau.t0

    reseau.afficher_journal(n=30)
    reseau.afficher_etat()

    attendu = max(nid for nid in reseau.noeuds if noeuds[nid]._actif)
    print(f"\n  Nouveau leader : N{leader2} {'✅' if leader2 == attendu else f'❌ (attendu N{attendu})'}")
    print(f"  Temps de réélection : {t_elu - t_crash:.2f}s")

    arreter_tous(reseau)


# ─── SCÉNARIO 3 : RÉSURRECTION DU LEADER ─────────────────────────────────────

def scenario_resurrection_leader():
    titre("SCÉNARIO 3 — Résurrection : l'ancien leader revient et reprend le pouvoir")

    print("""
  C'est le comportement "Bully" caractéristique :
  N5 revient en ligne → lance immédiatement une élection
  → avec le plus grand ID, il reprend le leadership.

  Avantage  : prévisible, toujours le plus puissant dirige
  Inconvénient : instabilité si N5 oscille (yo-yo problem)
    """)

    reseau, noeuds = creer_cluster(5, latence_ms=15)
    for n in noeuds.values():
        n.demarrer()
    for n in noeuds.values():
        threading.Thread(target=n.lancer_election, args=("init",), daemon=True).start()

    leader_id = attendre_leader(reseau, timeout=5)
    print(f"  Leader initial : N{leader_id}")

    print(f"\n  💥 N5 tombe...")
    noeuds[5].arreter()

    leader2 = attendre_leader(reseau, timeout=6)
    print(f"  Nouveau leader : N{leader2}")

    time.sleep(0.3)
    print(f"\n  🔄 N5 redémarre → lance immédiatement une élection")
    noeuds[5].ressusciter()

    leader3 = attendre_leader(reseau, timeout=5)

    reseau.afficher_journal(n=20)
    reseau.afficher_etat()

    print(f"\n  Leader final : N{leader3} {'✅ (N5 a repris le pouvoir)' if leader3 == 5 else '❌'}")
    print(f"""
  📌 Le Yo-Yo Problem :
     Si N5 crashe et redémarre en boucle, le cluster passe
     son temps à réélire au lieu de travailler.
     Solution : période de "cooldown" avant de reprendre le leadership,
     ou utiliser Raft (Jour 7) qui évite ce problème.
    """)

    arreter_tous(reseau)


# ─── SCÉNARIO 4 : PANNES MULTIPLES SIMULTANÉES ───────────────────────────────

def scenario_pannes_multiples():
    titre("SCÉNARIO 4 — Pannes multiples : tempête de messages")

    print("""
  N5, N4, N3 tombent en même temps.
  Seuls N1 et N2 survivent → N2 doit gagner.

  Complexité : chaque nœud qui détecte une panne lance
  une élection → plusieurs élections simultanées,
  messages qui se croisent → convergence quand même garantie.
    """)

    reseau, noeuds = creer_cluster(5, latence_ms=20)
    for n in noeuds.values():
        n.demarrer()
    for n in noeuds.values():
        threading.Thread(target=n.lancer_election, args=("init",), daemon=True).start()

    attendre_leader(reseau, timeout=5)

    print(f"\n  💥 N5, N4, N3 tombent simultanément !")
    for nid in [5, 4, 3]:
        noeuds[nid].arreter()

    leader_final = attendre_leader(reseau, timeout=8)

    reseau.afficher_journal(n=30)
    reseau.afficher_etat()

    print(f"\n  Leader final : N{leader_final} {'✅' if leader_final == 2 else f'❌ (attendu N2)'}")
    print(f"""
  Nombre de messages générés :
  Avec N nœuds, dans le pire cas, Bully génère O(N²) messages.
  → Sur 1000 nœuds : jusqu'à ~1M messages pour une élection !
  → Raison principale pour laquelle Raft limite le nombre
    de candidats (Jour 7).
    """)

    arreter_tous(reseau)


# ─── SCÉNARIO 5 : RÉSEAU AVEC PERTES ─────────────────────────────────────────

def scenario_reseau_avec_pertes():
    titre("SCÉNARIO 5 — Réseau instable : 15% de perte de messages")

    print("""
  Le réseau perd 15% des messages.
  L'algorithme doit quand même converger vers un leader stable
  grâce aux timeouts et aux relances automatiques.
    """)

    reseau, noeuds = creer_cluster(4, latence_ms=25, taux_perte=0.15)
    for n in noeuds.values():
        n.demarrer()
    for n in noeuds.values():
        threading.Thread(target=n.lancer_election, args=("init",), daemon=True).start()

    leader_id = attendre_leader(reseau, timeout=10)

    reseau.afficher_journal(n=30)
    reseau.afficher_etat()

    if leader_id:
        print(f"\n  ✅ Leader élu malgré les pertes : N{leader_id}")
        attendu = max(nid for nid in reseau.noeuds if noeuds[nid]._actif)
        ok = "✅" if leader_id == attendu else f"❌ (attendu N{attendu})"
        print(f"  Leader attendu : N{attendu} {ok}")
    else:
        print(f"\n  ⏰ Pas de convergence dans les temps (15% de perte = cas difficile)")

    print(f"""
  📌 Impact des pertes :
     • Un message ELECTION perdu → timeout → relance
     • Un message OK perdu → lanceur se proclame à tort
     • Un message COORDINATOR perdu → nœud reste sans leader

  Bully résiste aux pertes OCCASIONNELLES mais reste fragile
  face aux partitions réseau persistantes.
  → Raft (Jour 7) gère ça avec le concept de "quorum".
    """)

    arreter_tous(reseau)


# ─── ANALYSE THÉORIQUE ────────────────────────────────────────────────────────

def analyser_complexite():
    titre("ANALYSE — Complexité et limites du Bully Algorithm")

    print(f"""
  Complexité en messages (N nœuds) :
  ┌──────────────────────────────────────────────────────┐
  │ Cas                  │ Messages      │ Formule       │
  ├──────────────────────┼───────────────┼───────────────┤
  │ Meilleur cas         │              │               │
  │ (N le plus grand     │ N-1 msgs     │ O(N)          │
  │ lance l'élection)    │              │               │
  ├──────────────────────┼───────────────┼───────────────┤
  │ Pire cas             │ O(N²) msgs   │ ≈ N(N-1)/2    │
  │ (le plus petit lance)│              │               │
  ├──────────────────────┼───────────────┼───────────────┤
  │ 10 nœuds             │ ~45 msgs     │ réaliste      │
  │ 100 nœuds            │ ~4950 msgs   │ lourd         │
  │ 1000 nœuds           │ ~500k msgs   │ catastrophique │
  └──────────────────────┴───────────────┴───────────────┘

  Garanties :
    ✅ Converge toujours (si < 50% de pannes)
    ✅ Élit toujours le nœud vivant avec le plus grand ID
    ✅ Simple à implémenter et à comprendre

  Limites :
    ❌ O(N²) messages dans le pire cas
    ❌ Yo-Yo problem si le leader oscille
    ❌ Sensible aux partitions réseau (split-brain possible)
    ❌ ID = priorité : pas de rotation du leadership

  Alternatives :
    → Raft (Jour 7)    : terms, log matching, quorum strict
    → Paxos (Jour 7)   : plus flexible, plus complexe
    → ZooKeeper        : Zab protocol (basé sur Paxos)
    → etcd             : Raft
    → Kubernetes       : leader election via etcd/Raft
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*60 + "╗")
    print("║   JOUR 6 — BULLY ALGORITHM : ÉLECTION DE LEADER         ║")
    print("╚" + "═"*60 + "╝")

    scenario_election_initiale()
    scenario_mort_du_leader()
    scenario_resurrection_leader()
    scenario_pannes_multiples()
    scenario_reseau_avec_pertes()
    analyser_complexite()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Bully Algorithm — 3 messages, 1 règle :

  ELECTION    → "Es-tu plus grand que moi ?"
  OK          → "Oui, recule"
  COORDINATOR → "J'ai gagné, suivez-moi"

  Règle : le nœud vivant avec le plus grand ID gagne toujours.

  Ce que les scénarios ont montré :
    Scénario 1 → Convergence initiale, N5 gagne (prévisible)
    Scénario 2 → Réélection en ~2s après mort du leader
    Scénario 3 → Résurrection : N5 reprend immédiatement
    Scénario 4 → 3 pannes simultanées → converge vers N2
    Scénario 5 → 15% pertes réseau → converge quand même

  → Jour 7 : Introduction à Raft
    L'algorithme de consensus qui résout les limites de Bully.
    Terms, log matching, quorum, vote — la base d'etcd et CockroachDB.
  """)


if __name__ == "__main__":
    main()
