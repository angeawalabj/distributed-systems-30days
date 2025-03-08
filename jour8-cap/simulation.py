"""
Jour 8 — Simulation : CAP en action
=====================================
6 scénarios :
  1. Fonctionnement normal — CP et AP identiques
  2. Partition réseau — CP refuse, AP accepte
  3. Divergence AP — deux nœuds écrivent des valeurs différentes
  4. Guérison de partition — réconciliation AP
  5. Comparaison de latence (PACELC)
  6. Décision : quel système pour quel usage ?
"""

import time
import threading
from cap import (
    PartitionReseau, creer_cluster_cp, creer_cluster_ap,
    StatutOp, NoeudAP, NoeudCP
)

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s, msg=""):
    if msg: print(f"\n  ⏳ {msg} ({s}s)...")
    time.sleep(s)

def afficher_resultat(label: str, res, montrer_valeur=True):
    icones = {StatutOp.OK: "✅", StatutOp.ERREUR: "❌", StatutOp.OBSOLETE: "⚠️ "}
    icone = icones[res.statut]
    valeur_str = f" valeur={res.valeur!r}" if montrer_valeur and res.valeur is not None else ""
    msg_str = f" → {res.message}" if res.message else ""
    print(f"  {icone} N{res.noeud_id} [{res.statut.value:<8}]{valeur_str} "
          f"(v{res.version}, {res.latence_ms:.1f}ms){msg_str}")

def afficher_etat_cluster(label: str, noeuds):
    print(f"\n  {label} :")
    for n in noeuds:
        print(f"    N{n.id} → {n.etat()}")


# ─── SCÉNARIO 1 : FONCTIONNEMENT NORMAL ──────────────────────────────────────

def scenario_normal():
    titre("SCÉNARIO 1 — Fonctionnement normal : CP et AP identiques")

    print("""
  Sans partition réseau, CP et AP se comportent pareil.
  La différence n'apparaît QUE sous partition.
    """)

    partition = PartitionReseau()
    cp = creer_cluster_cp(3, partition)
    ap = creer_cluster_ap(3, partition)

    print("  ── Base CP ──")
    r = cp[0].ecrire("solde:alice", 1000)
    afficher_resultat("  WRITE solde:alice=1000", r)
    r = cp[1].lire("solde:alice")
    afficher_resultat("  READ  solde:alice (N2)", r)

    print("\n  ── Base AP ──")
    r = ap[0].ecrire("solde:alice", 1000)
    afficher_resultat("  WRITE solde:alice=1000", r)
    time.sleep(0.05)  # Laisser la réplication async se faire
    r = ap[1].lire("solde:alice")
    afficher_resultat("  READ  solde:alice (N2)", r)

    print("""
  ✅ Les deux bases répondent correctement.
  Le théorème CAP ne fait aucune différence ici —
  c'est pourquoi tant de systèmes naïfs ignorent ce choix
  jusqu'au premier incident réseau.
    """)


# ─── SCÉNARIO 2 : PARTITION — LE MOMENT DE VÉRITÉ ────────────────────────────

def scenario_partition():
    titre("SCÉNARIO 2 — Partition réseau : le moment de vérité")

    print("""
  Cluster de 5 nœuds, partition en 2 groupes :
    Groupe A : N1, N2       (minorité)
    Groupe B : N3, N4, N5   (majorité)

  On tente d'écrire depuis N1 (côté minorité) sur CP et AP.
    """)

    partition = PartitionReseau()
    cp = creer_cluster_cp(5, partition)
    ap = creer_cluster_ap(5, partition)

    # Écriture initiale avant la partition
    cp[0].ecrire("stock:produit_A", 100)
    ap[0].ecrire("stock:produit_A", 100)
    time.sleep(0.05)

    print("  État initial (avant partition) :")
    print(f"    CP N1 : {cp[0].etat()}")
    print(f"    AP N1 : {ap[0].etat()}")

    # Coupure réseau
    print("\n  💥 PARTITION RÉSEAU : {N1,N2} ↔ {N3,N4,N5} coupés\n")
    partition.partitionner({1, 2}, {3, 4, 5})

    # Tentative d'écriture depuis N1 (minorité)
    print("  Tentative d'écriture depuis N1 (minorité) :")
    print()
    print("  Base CP (Cohérence prioritaire) :")
    r_cp = cp[0].ecrire("stock:produit_A", 50)
    afficher_resultat("  WRITE stock=50 (N1)", r_cp)

    print()
    print("  Base AP (Disponibilité prioritaire) :")
    r_ap = ap[0].ecrire("stock:produit_A", 50)
    afficher_resultat("  WRITE stock=50 (N1)", r_ap)

    time.sleep(0.05)

    print(f"""
  Résultat :
    CP → ❌ ERREUR : 2/5 nœuds visibles < quorum(3). REFUS.
         → Protège la cohérence : N3,N4,N5 ne verront JAMAIS stock=50
           car l'écriture n'a pas été acceptée.

    AP → ✅ OK     : écriture acceptée localement, réplication async.
         → Mais N3, N4, N5 ont toujours stock=100 !
           Deux versions de la vérité coexistent.

  C'est le trade-off fondamental du théorème CAP.
    """)

    # Vérification de la divergence AP
    print("  État AP après écriture sous partition :")
    for n in ap:
        print(f"    N{n.id} → {n.etat()}")

    partition.guerir()


# ─── SCÉNARIO 3 : DIVERGENCE AP ──────────────────────────────────────────────

def scenario_divergence_ap():
    titre("SCÉNARIO 3 — Divergence AP : deux vérités simultanées")

    print("""
  Scénario réel : deux clients réservent la DERNIÈRE chambre d'hôtel
  simultanément, chacun sur un nœud différent de la partition.
  La base AP accepte les deux → surréservation !
    """)

    partition = PartitionReseau()
    ap = creer_cluster_ap(4, partition)

    # Setup initial
    for n in ap:
        ap[0].ecrire("chambres_disponibles", 1)
    time.sleep(0.05)
    print(f"  État initial : chambres_disponibles=1 sur tous les nœuds")

    # Partition
    print("\n  💥 Partition : {N1,N2} | {N3,N4}\n")
    partition.partitionner({1, 2}, {3, 4})
    time.sleep(0.02)

    # Deux clients écrivent simultanément sur des partitions différentes
    resultats = {}

    def reserver(noeud, client_nom):
        r = noeud.ecrire("chambres_disponibles", 0)
        resultats[client_nom] = r

    t1 = threading.Thread(target=reserver, args=(ap[0], "Client-Alice (N1)"))
    t2 = threading.Thread(target=reserver, args=(ap[2], "Client-Bob   (N3)"))

    t1.start(); t2.start()
    t1.join();  t2.join()

    print("  Résultat des réservations :")
    for client, res in resultats.items():
        afficher_resultat(f"  {client}", res)

    time.sleep(0.05)
    print("\n  État après réservations (divergence) :")
    for n in ap:
        print(f"    N{n.id} (groupe {'A' if n.id <= 2 else 'B'}) → chambres={n.etat().get('chambres_disponibles', '?')}")

    print(f"""
  ❌ SURRÉSERVATION : Alice ET Bob ont tous deux réservé la chambre.
  La base AP a accepté les deux écritures (disponibilité maintenue).
  Mais les données sont incohérentes : 2 nœuds disent 0, 2 disent 0,
  mais depuis des écritures DIFFÉRENTES avec des timestamps distincts.

  Solutions en pratique :
    → Saga Pattern : transaction distribuée avec compensation
    → CRDT          : types de données auto-fusionnables (Jour 14+)
    → Réserver sur CP pour les données critiques (stock, argent)
    → Idempotence (Jour 5) + verrou distribué (Jour 15)
    """)

    partition.guerir()


# ─── SCÉNARIO 4 : GUÉRISON ET RÉCONCILIATION ─────────────────────────────────

def scenario_guerison():
    titre("SCÉNARIO 4 — Guérison de partition : réconciliation AP")

    print("""
  Après une partition, les nœuds AP doivent se resynchroniser.
  On simule le mécanisme d'anti-entropie (Cassandra, DynamoDB).
  Stratégie : Last-Write-Wins (le timestamp le plus récent gagne).
    """)

    partition = PartitionReseau()
    ap = creer_cluster_ap(4, partition)

    # Données initiales
    ap[0].ecrire("config:timeout", "30s")
    ap[0].ecrire("config:retries", "3")
    time.sleep(0.05)

    print("  État avant partition :")
    for n in ap:
        print(f"    N{n.id} → {n.etat()}")

    # Partition
    partition.partitionner({1, 2}, {3, 4})
    time.sleep(0.02)
    print("\n  💥 Partition : {N1,N2} | {N3,N4}")

    # Chaque partition fait ses modifications
    time.sleep(0.02)
    ap[0].ecrire("config:timeout", "60s")   # Groupe A modifie
    time.sleep(0.01)
    ap[2].ecrire("config:timeout", "10s")   # Groupe B modifie aussi (CONFLIT !)
    ap[2].ecrire("config:max_conn", "100")  # Groupe B ajoute une clé

    time.sleep(0.05)
    print("\n  État PENDANT la partition (divergence) :")
    for n in ap:
        groupe = "A" if n.id <= 2 else "B"
        print(f"    N{n.id} (groupe {groupe}) → {n.etat()}")

    # Guérison
    print("\n  💚 Partition guérie — réconciliation en cours...")
    partition.guerir()
    time.sleep(0.05)

    # Réconciliation Last-Write-Wins
    for n in ap:
        n.reconcilier(ap)

    time.sleep(0.1)
    print("\n  État APRÈS réconciliation (LWW) :")
    for n in ap:
        print(f"    N{n.id} → {n.etat()}")

    print(f"""
  Résultat LWW :
    config:timeout → "10s" (timestamp de B > timestamp de A)
    config:max_conn → "100" (seulement dans B, propagé partout)
    config:retries  → "3" (inchangé)

  ⚠️  Limitation de LWW :
    La valeur "60s" écrite par le groupe A est PERDUE.
    Aucune trace de ce conflit → données silencieusement écrasées.

  → Vector Clocks (Jour 14) permettent de DÉTECTER ce conflit
    et de demander à l'application comment le résoudre.
    """)


# ─── SCÉNARIO 5 : PACELC — LATENCE VS COHÉRENCE ──────────────────────────────

def scenario_pacelc():
    titre("SCÉNARIO 5 — PACELC : Latence vs Cohérence sans partition")

    print("""
  Le théorème CAP ne parle que des partitions.
  PACELC (Daniel Abadi, 2012) complète :
    "En l'Absence de Partition : trade-off entre Latence et Cohérence"

  Cohérence forte (CP-style) = réplication synchrone = plus de latence
  Cohérence faible (AP-style) = réplication async   = moins de latence
    """)

    partition = PartitionReseau()  # Pas de partition ici
    cp = creer_cluster_cp(3, partition)
    ap = creer_cluster_ap(3, partition)

    N = 20
    temps_cp = []
    temps_ap = []

    print(f"  Benchmark : {N} écritures consécutives\n")

    for i in range(N):
        debut = time.perf_counter()
        cp[0].ecrire(f"key:{i}", f"val_{i}")
        temps_cp.append((time.perf_counter() - debut) * 1000)

        debut = time.perf_counter()
        ap[0].ecrire(f"key:{i}", f"val_{i}")
        temps_ap.append((time.perf_counter() - debut) * 1000)

    moy_cp = sum(temps_cp) / len(temps_cp)
    moy_ap = sum(temps_ap) / len(temps_ap)
    max_cp = max(temps_cp)
    max_ap = max(temps_ap)

    barre = lambda v, max_v, w=30: "█" * int(v / max_v * w)
    max_v = max(moy_cp, moy_ap) * 1.2

    print(f"  Latence moyenne d'écriture :")
    print(f"    CP  {barre(moy_cp, max_v):<32} {moy_cp:.2f}ms (sync)")
    print(f"    AP  {barre(moy_ap, max_v):<32} {moy_ap:.2f}ms (async)")
    print(f"\n  Latence max :")
    print(f"    CP  max={max_cp:.2f}ms")
    print(f"    AP  max={max_ap:.2f}ms")

    print(f"""
  En production sur un cluster réel (nœuds dans des DCs différents) :
    CP sur 3 continents  → ~200ms/écriture (aller-retour réseau × 2)
    AP sur 3 continents  → ~5ms/écriture   (local + async en arrière-plan)

  C'est pourquoi les bases AP comme Cassandra sont choisies pour :
    → Compteurs de vues (YouTube, Twitter)
    → Logs d'activité
    → Sessions utilisateurs
    → Données IoT (milliards de points/seconde)
    """)


# ─── SCÉNARIO 6 : GUIDE DE DÉCISION ──────────────────────────────────────────

def scenario_guide_decision():
    titre("SCÉNARIO 6 — Guide de décision : CP ou AP ?")

    print("""
  ┌──────────────────────────────────────────────────────────────┐
  │                    CHOISIR CP quand...                       │
  ├──────────────────────────────────────────────────────────────┤
  │ ✅ Données financières (soldes, transactions)                │
  │ ✅ Stock de produits (éviter la surréservation)              │
  │ ✅ Configuration critique (déploiements, feature flags)      │
  │ ✅ Réservations uniques (sièges, chambres)                   │
  │ ✅ Authentification et droits d'accès                        │
  │                                                              │
  │ Systèmes : etcd, CockroachDB, Spanner, PostgreSQL            │
  │ Trade-off : indisponible sous partition (rare en prod)       │
  └──────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────┐
  │                    CHOISIR AP quand...                       │
  ├──────────────────────────────────────────────────────────────┤
  │ ✅ Compteurs (vues, likes) — une erreur de ±1 est OK        │
  │ ✅ Catalogue produits — lire une version légèrement ancienne │
  │ ✅ Profils utilisateurs — délai de mise à jour acceptable    │
  │ ✅ Logs et métriques — volume extrême, perte mineure OK      │
  │ ✅ Shopping cart — Amazon l'a prouvé en 2007 (Dynamo paper) │
  │                                                              │
  │ Systèmes : Cassandra, DynamoDB, CouchDB, Riak, Redis        │
  │ Trade-off : données parfois obsolètes ou conflictuelles      │
  └──────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────┐
  │              La réalité : systèmes HYBRIDES                  │
  ├──────────────────────────────────────────────────────────────┤
  │ DynamoDB : AP par défaut, CP optionnel (strongly consistent) │
  │ Cassandra : niveau de cohérence configurable par requête     │
  │             (ONE, QUORUM, ALL)                               │
  │ MongoDB   : CP par défaut, AP possible (read preference)     │
  │ CockroachDB : CP strict, mais AP pour les lectures stale     │
  │                                                              │
  │ → Le CAP est un spectre, pas une case à cocher              │
  └──────────────────────────────────────────────────────────────┘

  Architecture recommandée pour une app complète :
    ┌─────────────┐    ┌──────────────┐    ┌─────────────┐
    │  etcd/      │    │  PostgreSQL  │    │  Cassandra  │
    │  Consul     │    │  (CP)        │    │  (AP)       │
    │  (CP)       │    │              │    │             │
    │  Config &   │    │  Commandes,  │    │  Logs,      │
    │  service    │    │  Paiements,  │    │  Métriques, │
    │  discovery  │    │  Stock       │    │  Sessions   │
    └─────────────┘    └──────────────┘    └─────────────┘
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 8 — THÉORÈME CAP : COHÉRENCE VS DISPONIBILITÉ     ║")
    print("╚" + "═"*62 + "╝")

    scenario_normal()
    scenario_partition()
    scenario_divergence_ap()
    scenario_guerison()
    scenario_pacelc()
    scenario_guide_decision()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Théorème CAP — Ce qu'il faut retenir :

    P (Partition) est INÉVITABLE en production.
    Le vrai choix : CP ou AP ?

    CP → cohérence garantie, disponibilité sacrifiée sous partition
         "Je préfère dire ERREUR plutôt que mentir"

    AP → disponibilité garantie, cohérence éventuelle
         "Je préfère répondre avec une valeur ancienne que ne pas répondre"

  Ce que nos scénarios ont montré :
    Scénario 2 → CP refuse (❌) | AP accepte avec divergence (⚠️)
    Scénario 3 → AP : 2 réservations pour 1 chambre (surréservation !)
    Scénario 4 → Réconciliation LWW : données de A silencieusement perdues
    Scénario 5 → AP est plus rapide même sans partition (PACELC)

  → Jour 9 : Quorum (Lecture/Écriture)
    Le curseur entre CP et AP. On ajuste W+R > N pour
    garantir qu'une lecture voit toujours la dernière écriture.
  """)


if __name__ == "__main__":
    main()
