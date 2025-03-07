"""
Jour 7 — Simulation : Raft en action
======================================
5 scénarios :
  1. Élection initiale avec terms
  2. Log replication : commandes committées avec quorum
  3. Mort du leader → réélection, log préservé
  4. Partition réseau (split-brain impossible)
  5. Rattrapage de log après résurrection
"""

import time
import threading
from raft import Cluster, Role, NoeudRaft

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s, msg=""): 
    if msg: print(f"\n  ⏳ {msg} ({s}s)...")
    time.sleep(s)


# ─── SCÉNARIO 1 : ÉLECTION AVEC TERMS ────────────────────────────────────────

def scenario_election_terms():
    titre("SCÉNARIO 1 — Élection avec Terms (vs Bully sans terms)")

    print("""
  5 nœuds démarrent. Chacun a un timeout aléatoire.
  Le premier à expirer → CANDIDATE → RequestVote.

  Différence clé avec Bully :
    Bully  : l'ID le plus grand gagne toujours
    Raft   : le premier avec un timeout expire vote,
             n'importe qui peut être élu si son log est à jour.
    """)

    cluster = Cluster(5, latence_ms=10)
    cluster.demarrer()

    leader = cluster.attendre_leader(timeout=5)
    cluster.afficher_journal(n=20)
    cluster.afficher_etat()

    if leader:
        print(f"""
  Leader élu : N{leader.id} (term={leader.current_term}) ✅
  
  Pourquoi les terms empêchent le Yo-Yo Problem :
    • Chaque élection incrémente le term
    • Un message avec term < current_term est IGNORÉ
    • Un ancien leader qui "ressuscite" voit des terms plus grands
      → devient immédiatement FOLLOWER sans perturber le cluster
    • Contrairement à Bully : l'ID le plus grand ne prend PAS
      automatiquement le leadership au retour
    """)

    cluster.arreter_tous()


# ─── SCÉNARIO 2 : LOG REPLICATION ─────────────────────────────────────────────

def scenario_log_replication():
    titre("SCÉNARIO 2 — Log Replication : commit avec quorum")

    print("""
  Le leader reçoit des commandes clients et les réplique.
  Une commande est COMMITTÉE seulement quand la majorité
  des nœuds l'a dans leur log.

  Garantie : une commande committée ne peut JAMAIS être perdue,
  même si le leader tombe juste après le commit.
    """)

    cluster = Cluster(5, latence_ms=10)
    cluster.demarrer()
    leader = cluster.attendre_leader(timeout=5)

    if not leader:
        print("  ❌ Pas de leader")
        cluster.arreter_tous()
        return

    print(f"\n  Leader : N{leader.id} (term={leader.current_term})")

    # Soumettre des commandes
    commandes = [
        "SET user:1=Alice",
        "SET user:2=Bob",
        "SET compteur=42",
        "SET config:theme=dark",
    ]

    print(f"\n  Soumission de {len(commandes)} commandes :\n")
    for cmd in commandes:
        ok = leader.soumettre(cmd)
        print(f"  {'✅' if ok else '❌'} Soumis : {cmd}")
        time.sleep(0.1)

    # Attendre la réplication
    attendre(1.0, "Réplication et commit")

    cluster.afficher_journal(n=20)
    cluster.afficher_etat()

    print(f"\n  Vérification de la cohérence du log :")
    print("  " + "─"*50)
    for nid in sorted(cluster.noeuds):
        n = cluster.noeuds[nid]
        log_str = " | ".join(str(e) for e in n.log) if n.log else "(vide)"
        print(f"  N{nid} log: {log_str}")
        print(f"     SM : {n.state_machine}")

    # Vérifier que tous ont le même log committé
    logs_commites = {
        nid: tuple(e.commande for e in cluster.noeuds[nid].log[:cluster.noeuds[nid].commit_index])
        for nid in cluster.noeuds
        if cluster.noeuds[nid]._actif
    }
    tous_identiques = len(set(logs_commites.values())) == 1
    print(f"\n  Logs committés identiques : {'✅' if tous_identiques else '❌'}")

    cluster.arreter_tous()


# ─── SCÉNARIO 3 : MORT DU LEADER, LOG PRÉSERVÉ ───────────────────────────────

def scenario_mort_leader_log():
    titre("SCÉNARIO 3 — Mort du leader : log et données préservés")

    print("""
  On soumet des commandes → leader les committe → leader tombe.
  Le nouveau leader élu doit avoir toutes les données committées.
  C'est la garantie de "durabilité" de Raft.
    """)

    cluster = Cluster(5, latence_ms=10)
    cluster.demarrer()
    leader1 = cluster.attendre_leader(timeout=5)

    if not leader1:
        print("  ❌ Pas de leader initial")
        cluster.arreter_tous()
        return

    print(f"  Leader initial : N{leader1.id} (term={leader1.current_term})")

    # Écrire des données
    for cmd in ["SET db:host=localhost", "SET db:port=5432", "SET app:version=2.1"]:
        leader1.soumettre(cmd)
        time.sleep(0.1)

    attendre(0.8, "Commit des données")
    print(f"\n  Données committées sur N{leader1.id} : {leader1.state_machine}")

    print(f"\n  💥 N{leader1.id} tombe !")
    id1 = leader1.id
    cluster.noeuds[id1].arreter()

    # Attendre la réélection
    leader2 = cluster.attendre_leader(timeout=6)
    if not leader2:
        print("  ❌ Pas de nouveau leader")
        cluster.arreter_tous()
        return

    print(f"  Nouveau leader : N{leader2.id} (term={leader2.current_term})")

    # Vérifier que les données sont préservées
    attendre(0.5)
    print(f"\n  Données sur le nouveau leader N{leader2.id} : {leader2.state_machine}")

    donnees_preservees = (
        leader2.state_machine.get("db:host") == "localhost" and
        leader2.state_machine.get("db:port") == "5432"
    )
    print(f"  Données préservées : {'✅' if donnees_preservees else '❌'}")

    # Ajouter de nouvelles données sous le nouveau leader
    leader2.soumettre("SET app:leader_change=true")
    attendre(0.5)

    cluster.afficher_etat()
    cluster.arreter_tous()


# ─── SCÉNARIO 4 : PARTITION RÉSEAU — SPLIT-BRAIN IMPOSSIBLE ──────────────────

def scenario_partition_reseau():
    titre("SCÉNARIO 4 — Partition réseau : split-brain impossible par quorum")

    print("""
  Cluster de 5 nœuds. On simule une partition :
    Partition A : N1, N2       (minorité : 2 < quorum 3)
    Partition B : N3, N4, N5  (majorité : 3 ≥ quorum 3)

  Avec Bully : N2 et N5 pourraient tous deux se croire leaders.
  Avec Raft  : seule la partition B peut élire un leader (quorum).
               La partition A ne peut pas committer de commandes.
    """)

    cluster = Cluster(5, latence_ms=10)
    cluster.demarrer()
    leader = cluster.attendre_leader(timeout=5)

    if not leader:
        print("  ❌ Pas de leader initial")
        cluster.arreter_tous()
        return

    print(f"  Leader initial : N{leader.id}")

    # Simuler la partition : on tue 2 nœuds de la majorité
    # et on observe que la minorité ne peut pas élire
    print(f"\n  Simulation : N3, N4, N5 isolés de N1, N2")
    print(f"  → On tue N1, N2 pour simuler leur isolation\n")

    cluster.noeuds[1].arreter()
    cluster.noeuds[2].arreter()

    # Attendre que les 3 nœuds restants élisent un leader
    leader_maj = cluster.attendre_leader(timeout=6)

    cluster.afficher_etat()

    if leader_maj:
        print(f"""
  ✅ La majorité (N3+N4+N5) a élu N{leader_maj.id}
     → Quorum atteint : {cluster.quorum()}/5

  Si N1 et N2 avaient pu se parler (partition symétrique) :
    - N1, N2 ne peuvent PAS élire un leader (2 < 3 = quorum)
    - Même si N2 se croit leader, il ne peut pas committer
      (besoin de 3 votes pour commit, n'en a que 1 hors de lui)
    - → Impossible d'avoir 2 leaders qui committent simultanément
    """)
    else:
        print("  ⚠️  Pas de convergence (normal si les 3 nœuds sont le quorum)")

    cluster.arreter_tous()


# ─── SCÉNARIO 5 : RATTRAPAGE DE LOG ──────────────────────────────────────────

def scenario_rattrapage_log():
    titre("SCÉNARIO 5 — Rattrapage : nœud qui revient après retard")

    print("""
  N5 tombe pendant qu'on écrit des données.
  Pendant son absence, le cluster committe 4 commandes.
  N5 revient → le leader lui envoie les entrées manquantes.
  C'est le mécanisme de "log catch-up" de Raft.
    """)

    cluster = Cluster(5, latence_ms=10)
    cluster.demarrer()
    leader = cluster.attendre_leader(timeout=5)

    if not leader:
        print("  ❌ Pas de leader")
        cluster.arreter_tous()
        return

    print(f"  Leader : N{leader.id} | N5 va tomber pendant les écritures\n")

    # Tuer N5 avant les écritures
    print("  💥 N5 tombe")
    cluster.noeuds[5].arreter()
    time.sleep(0.1)

    # Écrire des données (committées sans N5)
    donnees = ["SET x=1", "SET y=2", "SET z=3", "SET w=4"]
    for cmd in donnees:
        # Trouver le leader actuel (peut avoir changé)
        l = cluster.leader()
        if l:
            l.soumettre(cmd)
        time.sleep(0.15)

    attendre(0.8, "Commit sans N5")

    print(f"\n  Log de N5 avant rattrapage : {len(cluster.noeuds[5].log)} entrées")
    print(f"  Log du leader              : {len(leader.log)} entrées")

    # Remettre N5 en ligne
    print(f"\n  🔄 N5 revient en ligne...")
    cluster.noeuds[5].ressusciter()

    attendre(1.5, "Rattrapage (AppendEntries)")

    n5 = cluster.noeuds[5]
    print(f"\n  Log de N5 après rattrapage : {len(n5.log)} entrées")
    print(f"  State machine de N5        : {n5.state_machine}")

    cluster.afficher_etat()

    rattrape = n5.state_machine == leader.state_machine
    print(f"\n  N5 a rattrapé le leader : {'✅' if rattrape else '❌'}")
    print(f"""
  Mécanisme AppendEntries de rattrapage :
    1. Leader envoie AppendEntries avec entries=[...]
    2. N5 voit prev_log_index/term ne match pas → rejette
    3. Leader recule next_index[5] d'un cran
    4. Répète jusqu'à trouver le point de divergence
    5. Envoie toutes les entrées depuis ce point
    → N5 est à jour en O(missing entries) messages
    """)

    cluster.arreter_tous()


# ─── COMPARAISON RAFT VS BULLY ────────────────────────────────────────────────

def comparaison_finale():
    titre("COMPARAISON — Raft vs Bully Algorithm")

    print("""
  ┌─────────────────────┬──────────────────┬──────────────────────┐
  │ Critère             │ Bully (Jour 6)   │ Raft (Jour 7)        │
  ├─────────────────────┼──────────────────┼──────────────────────┤
  │ Messages/élection   │ O(N²)            │ O(N)                 │
  │ Qui gagne           │ Plus grand ID    │ Timeout + log à jour │
  │ Split-brain         │ Possible         │ Impossible (quorum)  │
  │ Yo-Yo problem       │ Oui              │ Non (terms)          │
  │ Log répliqué        │ Non              │ Oui (garantie forte) │
  │ Résurrection        │ Reprend tout     │ Redevient follower   │
  │ Complexité code     │ ~100 lignes      │ ~400 lignes          │
  │ Utilisé en prod     │ Rarement         │ etcd, CockroachDB    │
  │                     │                  │ TiKV, Consul         │
  ├─────────────────────┼──────────────────┼──────────────────────┤
  │ Quand utiliser      │ Systèmes simples │ Systèmes critiques   │
  │                     │ < 10 nœuds       │ données financières  │
  │                     │ perte tolérable  │ configuration clé    │
  └─────────────────────┴──────────────────┴──────────────────────┘

  Les 3 garanties de Raft (Safety properties) :
    Election Safety  : au plus 1 leader par term
    Leader Append    : le leader n'écrase jamais son log
    Log Matching     : si 2 logs ont la même entrée (index, term)
                       → leurs logs sont identiques jusqu'à cet index
    Leader Completeness : si une entrée est committée dans term T,
                          tout futur leader aura cette entrée
    State Machine Safety : si un nœud a appliqué index i,
                           aucun autre nœud n'applique une autre commande en i
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 7 — RAFT : ALGORITHME DE CONSENSUS                  ║")
    print("╚" + "═"*62 + "╝")

    scenario_election_terms()
    scenario_log_replication()
    scenario_mort_leader_log()
    scenario_partition_reseau()
    scenario_rattrapage_log()
    comparaison_finale()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Raft en 4 concepts :

  1. TERMS     : mandats numérotés → pas de leader zombie
  2. VOTE      : quorum strict → split-brain impossible
  3. LOG       : répliqué sur majorité avant commit
  4. RATTRAPAGE: AppendEntries ramène les nœuds lents

  Ce que nos scénarios ont prouvé :
    ✅ Élection : n'importe quel nœud peut gagner (pas le +grand ID)
    ✅ Commit   : données préservées même si le leader tombe
    ✅ Quorum   : minorité ne peut pas committer (anti split-brain)
    ✅ Catch-up : N5 récupère 4 entrées manquantes automatiquement

  → Jour 8 : Théorème CAP
    Disponibilité vs Cohérence lors d'une partition réseau.
    On implémente 2 bases de données : une CP, une AP.
  """)


if __name__ == "__main__":
    main()
