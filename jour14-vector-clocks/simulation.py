"""
Jour 14 — Simulation : Vector Clocks en action
================================================
5 scénarios :
  1. Comparaison de VCs : précède, suit, concurrent
  2. Détection de causalité : LWW se trompe, VC a raison
  3. Siblings (conflits non résolus) et leur résolution
  4. Vector Clocks vs Lamport Timestamps
  5. Système complet : 3 nœuds, écritures concurrentes, résolution
"""

import time
import threading
from vector_clocks import (
    VectorClock, Relation, NoeudVC, EntreeVC
)

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s): time.sleep(s)

def afficher_relation(vc_a: VectorClock, vc_b: VectorClock, label_a="A", label_b="B"):
    rel = vc_a.relation_avec(vc_b)
    symboles = {
        Relation.PRECEDE:    f"{label_a} → {label_b}  (A précède B, causalité)",
        Relation.SUIT:       f"{label_b} → {label_a}  (B précède A, causalité)",
        Relation.IDENTIQUE:  f"{label_a} = {label_b}  (identiques)",
        Relation.CONCURRENT: f"{label_a} ∥ {label_b}  ⚠️  CONCURRENT — conflit potentiel",
    }
    icones = {
        Relation.PRECEDE: "→ ", Relation.SUIT: "← ",
        Relation.IDENTIQUE: "= ", Relation.CONCURRENT: "∥ ",
    }
    print(f"    {vc_a}  {icones[rel]} {vc_b}")
    print(f"    {symboles[rel]}")


# ─── SCÉNARIO 1 : COMPARAISON DE VCS ─────────────────────────────────────────

def scenario_comparaison():
    titre("SCÉNARIO 1 — Comparaison de Vector Clocks : les 3 relations")

    print("""
  Règle de comparaison :
    VC_A < VC_B (A précède B) : ∀i A[i] ≤ B[i]  ET  ∃j A[j] < B[j]
    VC_A ∥ VC_B (concurrent)  : ∃i A[i] > B[i]  ET  ∃j A[j] < B[j]
    """)

    cas = [
        ("A précède B",
         VectorClock({"paris": 1, "tokyo": 0}),
         VectorClock({"paris": 2, "tokyo": 1})),
        ("B précède A",
         VectorClock({"paris": 3, "tokyo": 2}),
         VectorClock({"paris": 1, "tokyo": 1})),
        ("Identiques",
         VectorClock({"paris": 2, "tokyo": 2}),
         VectorClock({"paris": 2, "tokyo": 2})),
        ("CONCURRENT (conflit)",
         VectorClock({"paris": 2, "tokyo": 1}),   # paris a avancé
         VectorClock({"paris": 1, "tokyo": 2})),   # tokyo a avancé
        ("CONCURRENT (2 nœuds ignorants l'un de l'autre)",
         VectorClock({"paris": 3, "tokyo": 0, "sydney": 1}),
         VectorClock({"paris": 0, "tokyo": 2, "sydney": 1})),
    ]

    for label, vc_a, vc_b in cas:
        print(f"\n  Cas : {label}")
        afficher_relation(vc_a, vc_b, "A", "B")

    print(f"""
  La règle clé :
    Deux VCs sont CONCURRENTS si et seulement si chacun a
    au moins une composante STRICTEMENT supérieure à l'autre.
    → Aucun ordre causal possible → conflit réel à résoudre.
    """)


# ─── SCÉNARIO 2 : VC vs LWW ──────────────────────────────────────────────────

def scenario_vc_vs_lww():
    titre("SCÉNARIO 2 — VC détecte ce que LWW rate : causalité vs concurrence")

    print("""
  Cas 1 : A écrit, B lit et réécrit. LWW et VC s'accordent.
  Cas 2 : A et B écrivent en même temps. LWW choisit
          arbitrairement, VC détecte le vrai conflit.
    """)

    print("  ── Cas 1 : Mise à jour causale ──────────────────────────────")
    print("""
  Paris  écrit doc:1 → VC{paris:1}
  Tokyo  reçoit la réplication, lit doc:1, puis réécrit
  Tokyo  écrit doc:1 (basé sur paris:1) → VC{paris:1, tokyo:1}
    """)

    vc_paris = VectorClock({"paris": 1})
    vc_tokyo_apres_lecture = VectorClock({"paris": 1, "tokyo": 1})

    print(f"  VC Paris  = {vc_paris}")
    print(f"  VC Tokyo  = {vc_tokyo_apres_lecture}")
    rel = vc_paris.relation_avec(vc_tokyo_apres_lecture)
    print(f"  Relation  : {rel.value}")
    print(f"  → Tokyo a lu la version de Paris PUIS a réécrit.")
    print(f"    Pas de conflit. LWW et VC concordent ici. ✅")

    print("\n  ── Cas 2 : Écritures vraiment concurrentes ──────────────────")
    print("""
  Paris  écrit doc:1 sans connaître tokyo → VC{paris:2, tokyo:0}
  Tokyo  écrit doc:1 sans connaître paris → VC{paris:0, tokyo:2}

  LWW : choisit celui avec le timestamp physique le plus grand
        → arbitraire, dépend du skew d'horloge, perte silencieuse

  VC  : détecte la concurrence → CONFLIT signalé, 2 siblings stockés
        → l'application peut merger proprement
    """)

    vc_paris_c = VectorClock({"paris": 2, "tokyo": 0})
    vc_tokyo_c = VectorClock({"paris": 0, "tokyo": 2})

    print(f"  VC Paris  = {vc_paris_c}")
    print(f"  VC Tokyo  = {vc_tokyo_c}")
    afficher_relation(vc_paris_c, vc_tokyo_c, "Paris", "Tokyo")

    print(f"""
  ✅ VC détecte avec certitude que les deux écritures sont concurrentes.
     Aucune horloge physique n'est consultée.
     Le conflit est signalé — rien n'est perdu silencieusement.
    """)


# ─── SCÉNARIO 3 : SIBLINGS ET RÉSOLUTION ─────────────────────────────────────

def scenario_siblings():
    titre("SCÉNARIO 3 — Siblings : conflits non résolus et leur résolution")

    print("""
  Riak (base de données) stocke les deux valeurs conflictuelles
  comme "siblings". Quand le client lit, il reçoit les deux et
  doit décider comment les fusionner.

  Simulation : 3 nœuds, Alice modifie son panier sur mobile ET desktop
  simultanément avant que la réplication ait eu le temps de se propager.
    """)

    paris  = NoeudVC("paris",  latence_ms=50)
    tokyo  = NoeudVC("tokyo",  latence_ms=50)
    sydney = NoeudVC("sydney", latence_ms=50)

    pairs = {"paris": paris, "tokyo": tokyo, "sydney": sydney}
    paris.enregistrer_pairs(pairs)
    tokyo.enregistrer_pairs(pairs)
    sydney.enregistrer_pairs(pairs)

    # Écriture initiale sur Paris
    e0 = paris.ecrire("cart:alice", ["chaussures", "t-shirt"])
    print(f"  Écriture initiale (Paris) : {e0.valeur}  {e0.vc}")
    attendre(0.15)   # Laisser répliquer

    entrees, conflit = paris.lire("cart:alice")
    print(f"  Après réplication initiale : {entrees[0].valeur}  {entrees[0].vc}")

    # Contexte lu par les deux clients (même version)
    vc_lu = entrees[0].vc.copie()
    print(f"\n  Alice lit le panier sur MOBILE et DESKTOP (même VC : {vc_lu})")
    print(f"  Les deux appareils ajoutent un article simultanément :\n")

    # Écriture concurrente SANS réplication entre les deux
    # (on écrit directement sur deux nœuds différents)
    e_paris = paris.ecrire("cart:alice", ["chaussures", "t-shirt", "casquette"],
                           vc_contexte=vc_lu)
    e_tokyo = tokyo.ecrire("cart:alice", ["chaussures", "t-shirt", "veste"],
                           vc_contexte=vc_lu)

    print(f"  Paris (mobile)   ajoute 'casquette' → {e_paris.vc}")
    print(f"  Tokyo (desktop)  ajoute 'veste'     → {e_tokyo.vc}")

    rel = e_paris.vc.relation_avec(e_tokyo.vc)
    print(f"\n  Relation entre les deux versions : {rel.value}")
    print(f"  → {'✅ causalité claire' if rel != Relation.CONCURRENT else '⚠️  CONCURRENT — siblings créés'}")

    # Forcer la réception croisée pour créer les siblings
    paris.recevoir_replication(e_tokyo)
    tokyo.recevoir_replication(e_paris)
    attendre(0.1)

    # Lire depuis Paris → 2 siblings
    entrees, conflit = paris.lire("cart:alice")
    print(f"\n  Lecture depuis Paris ({'conflit détecté !' if conflit else 'pas de conflit'}) :")
    for e in entrees:
        print(f"    sibling : {e.valeur}  {e.vc}  (écrit sur {e.noeud!r})")

    if conflit:
        print(f"\n  Résolution : l'application merge les deux paniers (union) :")
        # Merger les deux listes
        union = list(dict.fromkeys(
            entrees[0].valeur + entrees[1].valeur
        ))
        e_resolved = paris.resoudre_conflit("cart:alice", union)
        print(f"    Panier final : {e_resolved.valeur}  {e_resolved.vc}")
        attendre(0.2)

        # Vérifier que tous les nœuds convergent
        for nid, n in pairs.items():
            entrees_n, c = n.lire("cart:alice")
            val = entrees_n[0].valeur if entrees_n else None
            print(f"    {nid:<10} → {val}  {'⚠️  encore en conflit' if c else '✅'}")


# ─── SCÉNARIO 4 : VC vs LAMPORT TIMESTAMPS ───────────────────────────────────

def scenario_vc_vs_lamport():
    titre("SCÉNARIO 4 — Vector Clocks vs Lamport Timestamps")

    print("""
  Lamport (Jour 3) = 1 compteur global.
    ✅ Préserve la causalité : A→B implique L(A) < L(B)
    ❌ L'inverse est FAUX : L(A) < L(B) n'implique PAS A→B
       → Faux positifs de causalité (events peut-être non liés)

  Vector Clocks = N compteurs (un par nœud).
    ✅ Préserve la causalité : A→B implique VC(A) < VC(B)
    ✅ L'inverse EST VRAI : VC(A) < VC(B) implique A→B
    ✅ Détecte les vrais concurrents sans faux positifs
    ❌ Taille = O(N nœuds) — problème pour grands clusters
    """)

    print("  Exemple où Lamport échoue :\n")

    # Simuler Lamport sur 3 événements parallèles
    # A (paris) : L=1
    # B (tokyo, indépendant) : L=2
    # C (sydney, basé sur A) : L=2

    print("  Événements :")
    print("    A : Paris  écrit  (Lamport=1, VC={paris:1})")
    print("    B : Tokyo  écrit  (Lamport=2, VC={tokyo:1})  ← indépendant de A")
    print("    C : Sydney lit A et réécrit (Lamport=2, VC={paris:1, sydney:1})")
    print()

    vc_a = VectorClock({"paris": 1})
    vc_b = VectorClock({"tokyo": 1})
    vc_c = VectorClock({"paris": 1, "sydney": 1})  # Sydney a vu A

    lamport = {"A": 1, "B": 2, "C": 2}

    print(f"  Lamport — peut-on ordonner B et C ?")
    print(f"    L(B) = {lamport['B']}  L(C) = {lamport['C']}")
    print(f"    L(B) == L(C) → Lamport dit : 'même instant, indéterminé'")
    print(f"    Mais B (tokyo indépendant) et C (basé sur A) sont bien concurrents !")

    print(f"\n  Vector Clocks — réponse précise :")
    print(f"    VC(B) = {vc_b}")
    print(f"    VC(C) = {vc_c}")
    afficher_relation(vc_b, vc_c, "B", "C")

    print(f"\n  VC(A) vs VC(C) :")
    print(f"    VC(A) = {vc_a}")
    print(f"    VC(C) = {vc_c}")
    afficher_relation(vc_a, vc_c, "A", "C")

    print(f"""
  Résumé :
    Lamport : B et C sont "au même moment" → indéterminé
    VC      : B ∥ C (concurrents), A → C (causalité) ✅

  Coût de cette précision :
    Lamport : 1 entier par événement → O(1) espace
    Vector  : N entiers par événement → O(N) espace
    → Pour 1000 nœuds : 1000 entiers par entrée
    → Solution : Dotted Version Vectors, Interval Tree Clocks
    """)


# ─── SCÉNARIO 5 : SYSTÈME COMPLET ────────────────────────────────────────────

def scenario_systeme_complet():
    titre("SCÉNARIO 5 — Système complet : Wikipedia distribué (3 articles, 3 DCs)")

    print("""
  Simulation d'un wiki multi-DC : chaque DC peut éditer les articles.
  On simule des éditions concurrentes sur le même article et on
  mesure combien de conflits VC détecte (vs combien LWW en raterait).
    """)

    noeuds_ids = ["dc-eu", "dc-us", "dc-ap"]
    noeuds = {nid: NoeudVC(nid, latence_ms=20) for nid in noeuds_ids}
    for n in noeuds.values():
        n.enregistrer_pairs(noeuds)

    articles = ["article:python", "article:rust", "article:go"]

    # Écritures initiales depuis dc-eu
    for art in articles:
        noeuds["dc-eu"].ecrire(art, f"Version initiale de {art}")
    attendre(0.3)

    # Série d'écritures concurrentes
    scenarios_edits = [
        ("dc-eu", "article:python", "Python 3.12 : nouveautés"),
        ("dc-us", "article:python", "Python for AI/ML guide"),   # Concurrent !
        ("dc-eu", "article:rust",   "Rust 2024 edition"),
        ("dc-ap", "article:rust",   "Rust en Asie : adoption"),  # Concurrent !
        ("dc-us", "article:go",     "Go 1.22 release notes"),
    ]

    print(f"  Éditions concurrentes simulées :\n")
    resultats = []
    for nid, art, val in scenarios_edits:
        e = noeuds[nid].ecrire(art, val)
        resultats.append((nid, art, val, e.vc))
        print(f"  {nid:<10} édite {art:<20} → {e.vc}")
        attendre(0.01)

    attendre(0.5)  # Laisser la réplication se propager

    print(f"\n  État des conflits par nœud :\n")
    total_conflits = 0
    for nid, n in noeuds.items():
        print(f"  {nid} :")
        for art in articles:
            entrees, conflit = n.lire(art)
            nb = len(entrees)
            statut = f"⚠️  {nb} siblings" if conflit else "✅ cohérent"
            val = entrees[0].valeur if entrees else "(vide)"
            print(f"    {art:<22} {statut:<20} val={val!r}")
        total_conflits += n.nb_conflits()
        print()

    print(f"  Conflits détectés par VC (total) : {total_conflits}")
    print(f"  Conflits que LWW aurait silencieusement écrasés : ~{total_conflits}")

    # Résolution des conflits sur article:python
    print(f"\n  Résolution manuelle du conflit sur article:python :")
    entrees, conflit = noeuds["dc-eu"].lire("article:python")
    if conflit:
        valeurs = [e.valeur for e in entrees]
        print(f"    Siblings : {valeurs}")
        valeur_merge = " / ".join(valeurs)
        e_r = noeuds["dc-eu"].resoudre_conflit("article:python", valeur_merge)
        print(f"    Merge    : {e_r.valeur!r}")
        print(f"    Nouveau VC : {e_r.vc}")
        attendre(0.3)

        entrees_final, conflit_final = noeuds["dc-eu"].lire("article:python")
        print(f"    Après résolution : {'✅ plus de conflit' if not conflit_final else '⚠️  encore des siblings'}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 14 — VECTOR CLOCKS (HORLOGES VECTORIELLES)        ║")
    print("╚" + "═"*62 + "╝")

    scenario_comparaison()
    scenario_vc_vs_lww()
    scenario_siblings()
    scenario_vc_vs_lamport()
    scenario_systeme_complet()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Vector Clocks — Ce qu'il faut retenir :

    VC = {nœud: compteur} pour chaque nœud participant.

    Règles :
      Événement local    → incrémenter son propre compteur
      Envoyer message    → joindre son VC
      Recevoir message   → max(VC_local, VC_reçu) + incrémenter

    Comparaison :
      VC_A < VC_B  → causalité (A précède B)
      VC_A ∥ VC_B  → conflit réel (concurrents)

  Ce que nos scénarios ont prouvé :
    Scénario 1 → précède / suit / concurrent sur exemples concrets
    Scénario 2 → VC distingue causalité et concurrence — LWW non
    Scénario 3 → Siblings stockés, résolution par merge applicatif
    Scénario 4 → Lamport dit "indéterminé", VC répond avec précision
    Scénario 5 → Système complet : conflits détectés et résolus

  Avantages vs LWW (Jour 13) :
    ✅ Pas de faux positifs ni faux négatifs
    ✅ Aucune dépendance aux horloges physiques
    ✅ Conflits signalés, rien n'est perdu silencieusement

  Inconvénients :
    ❌ Taille O(N nœuds) par entrée → lourd pour grands clusters
    ❌ Complexité implémentation
    → Solutions : Dotted Version Vectors, Interval Tree Clocks

  Utilisé par :
    Riak       → siblings + résolution par l'application
    DynamoDB   → version vectors internes
    Git        → DAG de commits (équivalent VC discret)
    CRDTs      → base théorique pour les types auto-fusionnables

  → Jour 15 : Distributed Locking (Redlock)
    Empêcher deux services d'accéder à la même ressource
    en même temps — verrous distribués avec Redis.
  """)


if __name__ == "__main__":
    main()
