"""
Jour 9 — Simulation : Quorum en action
========================================
6 scénarios :
  1. Démonstration de W+R>N (preuve par l'exemple)
  2. Benchmark de toutes les combinaisons W/R
  3. Stale reads quand W+R ≤ N
  4. Read Repair en action
  5. Nœuds défaillants — résistance du quorum
  6. Guide de configuration Cassandra/DynamoDB
"""

import time
import threading
import random
from quorum import NoeudReplique, CoordinateurQuorum, StatutOp

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")

def creer_cluster(n: int, latence_ms=10.0, fiabilite=1.0) -> list[NoeudReplique]:
    return [NoeudReplique(i+1, latence_ms=latence_ms, fiabilite=fiabilite)
            for i in range(n)]

def afficher_resultat(label: str, res):
    icones = {StatutOp.OK: "✅", StatutOp.TIMEOUT: "⏰", StatutOp.INCOHERENCE: "⚠️"}
    ic = icones.get(res.statut, "?")
    inco = f" ⚠️ {res.incoherences} nœud(s) incohérent(s)" if res.incoherences > 0 else ""
    print(f"  {ic} {label:<28} {res.reponses}/{res.requis} nœuds  "
          f"{res.latence_ms:6.1f}ms  "
          f"{'valeur=' + repr(res.valeur) if res.valeur is not None else 'TIMEOUT'}"
          f"{inco}")


# ─── SCÉNARIO 1 : PREUVE DE W+R > N ──────────────────────────────────────────

def scenario_preuve_coherence():
    titre("SCÉNARIO 1 — Preuve de W+R > N : cohérence mathématiquement garantie")

    print("""
  N=3 répliques. On écrit sur W nœuds, on lit depuis R nœuds.
  Si W+R > N → les deux ensembles se chevauchent obligatoirement.
  Au moins 1 nœud a FORCÉMENT la dernière valeur.

  Démonstration avec W=2, R=2 (QUORUM) :
    Écriture confirmée sur N1, N2  (W=2)
    Lecture depuis N2, N3          (R=2)
    → N2 est dans les deux ! → lecture cohérente garantie.
    """)

    configs = [
        (2, 2, "QUORUM    (W+R=4 > N=3)"),
        (1, 3, "R=ALL     (W+R=4 > N=3)"),
        (3, 1, "W=ALL     (W+R=4 > N=3)"),
        (1, 2, "W+R=N     (W+R=3 = N=3, cohérence NON garantie)"),
        (1, 1, "ONE/ONE   (W+R=2 < N=3, cohérence NON garantie)"),
    ]

    print(f"  {'Configuration':<40} {'W+R>N?':<8} {'Cohérence forte?'}")
    print("  " + "─"*62)
    for W, R, label in configs:
        N = 3
        forte = (W + R) > N
        symbole = "✅" if forte else "❌"
        print(f"  W={W}, R={R}  {label:<36} "
              f"{W+R} {'>' if forte else '≤'} {N}  {symbole}")

    print(f"""
  Démonstration pratique (W=2, R=2 sur N=3) :
    """)

    noeuds = creer_cluster(3, latence_ms=5)
    coord  = CoordinateurQuorum(noeuds, W=2, R=2)

    # Écriture
    r_w = coord.ecrire("compte:alice", 1500)
    print(f"  WRITE compte:alice=1500  → {r_w.statut.value} ({r_w.reponses}/{r_w.requis} nœuds, {r_w.latence_ms:.1f}ms)")

    # Simuler un nœud lent qui n'a pas encore la valeur
    noeuds[2]._store.clear()   # N3 "rate" la réplication

    print(f"\n  N3 n'a pas reçu la réplication. État des nœuds :")
    for n in noeuds:
        print(f"    N{n.id} → {n.etat('compte:alice')}")

    # Lecture : QUORUM garantit qu'on touche N1 ou N2
    r_r = coord.lire("compte:alice")
    print(f"\n  READ compte:alice  → {r_r.statut.value} valeur={r_r.valeur} ({r_r.reponses}/{r_r.requis} nœuds, {r_r.latence_ms:.1f}ms)")
    print(f"  ✅ La valeur 1500 est retournée malgré N3 en retard.")
    print(f"     Mathématiquement impossible de lire une valeur périmée avec W+R>N.")


# ─── SCÉNARIO 2 : BENCHMARK TOUTES COMBINAISONS ──────────────────────────────

def scenario_benchmark():
    titre("SCÉNARIO 2 — Benchmark : latence et cohérence de toutes les combinaisons")

    N = 5
    configs = [
        (1, 1, "ONE/ONE"),
        (1, 3, "W=1, R=QUORUM"),
        (3, 1, "W=QUORUM, R=1"),
        (3, 3, "QUORUM/QUORUM"),
        (1, 5, "W=1, R=ALL"),
        (5, 1, "W=ALL, R=1"),
        (5, 5, "ALL/ALL"),
    ]

    ITERATIONS = 8
    print(f"  N={N} nœuds, {ITERATIONS} opérations par config, latence=10ms/nœud\n")
    print(f"  {'Config':<20} {'W'} {'R'} {'W+R>N':<6} {'Écriture ms':<14} {'Lecture ms':<14} {'Cohérence'}")
    print("  " + "─"*72)

    for W, R, label in configs:
        noeuds = creer_cluster(N, latence_ms=10)
        coord  = CoordinateurQuorum(noeuds, W=W, R=R, timeout_ms=800)

        # Benchmark écriture
        temps_w = []
        for i in range(ITERATIONS):
            r = coord.ecrire(f"key:{i}", f"val_{i}")
            if r.statut == StatutOp.OK:
                temps_w.append(r.latence_ms)
            time.sleep(0.01)

        # Benchmark lecture
        temps_r = []
        for i in range(ITERATIONS):
            r = coord.lire(f"key:{i % max(1, len(temps_w))}")
            if r.statut == StatutOp.OK:
                temps_r.append(r.latence_ms)

        moy_w = sum(temps_w)/len(temps_w) if temps_w else 0
        moy_r = sum(temps_r)/len(temps_r) if temps_r else 0
        forte = "FORTE ✅" if (W+R) > N else "ÉVENT. ⚠️"

        barre_w = "█" * int(moy_w / 3)
        barre_r = "█" * int(moy_r / 3)

        print(f"  {label:<20} {W} {R} {'oui' if (W+R)>N else 'non':<6} "
              f"{moy_w:5.0f}ms {barre_w:<8} {moy_r:5.0f}ms {barre_r:<8} {forte}")

    print(f"""
  Observations :
    W=1, R=1  → le plus rapide, aucune garantie de cohérence
    QUORUM    → équilibre parfait latence/cohérence (le choix par défaut)
    W=ALL     → écriture sûre même si N-1 nœuds tombent après
    R=ALL     → lecture toujours cohérente mais lente (bloquée par le + lent)
    ALL/ALL   → maximum de garanties, latence = nœud le plus lent
    """)


# ─── SCÉNARIO 3 : STALE READS QUAND W+R ≤ N ─────────────────────────────────

def scenario_stale_reads():
    titre("SCÉNARIO 3 — Stale reads : W+R ≤ N permet des lectures périmées")

    print("""
  On écrit une nouvelle valeur sur W=1 nœud.
  On lit depuis R=1 nœud différent (W+R=2 ≤ N=3).
  → On peut lire l'ancienne valeur (stale read).
    """)

    noeuds = creer_cluster(3, latence_ms=5)
    coord  = CoordinateurQuorum(noeuds, W=1, R=1, timeout_ms=200)

    # Écriture initiale sur tous (simuler état cohérent)
    for n in noeuds:
        n.ecrire("prix:article", 100, time.time())
    time.sleep(0.05)

    print(f"  État initial (prix=100 sur tous les nœuds) :")
    for n in noeuds: print(f"    N{n.id} → {n.etat('prix:article')}")

    # Nouvelle écriture W=1 : seulement N1 reçoit le nouveau prix
    print(f"\n  WRITE prix:article=150 (W=1, seulement N1 confirme)\n")
    ts_nouveau = time.time() + 1   # timestamp plus récent
    noeuds[0].ecrire("prix:article", 150, ts_nouveau)

    print(f"  État après écriture :")
    for n in noeuds: print(f"    N{n.id} → {n.etat('prix:article')}")

    # Lectures multiples — résultat aléatoire selon le nœud touché
    print(f"\n  Lectures successives (R=1, nœud aléatoire) :")
    lectures = []
    for i in range(6):
        # Forcer la lecture sur un nœud différent à chaque fois
        noeud_cible = noeuds[i % 3]
        v = noeud_cible.lire("prix:article")
        valeur = v.valeur if v else None
        stale  = valeur == 100
        lectures.append(valeur)
        print(f"    Lecture {i+1} (N{noeud_cible.id}) → {valeur} "
              f"{'⚠️  STALE (périmé)' if stale else '✅ À jour'}")

    stale_count = lectures.count(100)
    print(f"""
  {stale_count}/6 lectures ont retourné l'ancienne valeur (100).
  C'est le comportement normal et attendu avec W+R ≤ N.

  En pratique (Cassandra) :
    ConsistencyLevel.ONE  → rapide, stale reads possibles
    ConsistencyLevel.QUORUM → équilibre (recommandé par défaut)
    ConsistencyLevel.ALL  → cohérence forte, lent
    → On peut choisir PAR REQUÊTE selon l'importance de la donnée.
    """)


# ─── SCÉNARIO 4 : READ REPAIR ────────────────────────────────────────────────

def scenario_read_repair():
    titre("SCÉNARIO 4 — Read Repair : auto-guérison sans écriture explicite")

    print("""
  N3 a raté une réplication (nœud lent, redémarrage...).
  Sa valeur est périmée. Personne ne le sait encore.

  Quand on lit avec R=2 (QUORUM) :
    1. On lit N1 (valeur à jour) + N2 (valeur à jour)
    2. On retourne la bonne valeur au client
    3. EN PARALLÈLE : on envoie silencieusement la bonne valeur à N3
    → N3 est guéri sans aucune action explicite.

  C'est le mécanisme "anti-entropie par lecture" de Cassandra.
    """)

    noeuds = creer_cluster(3, latence_ms=5)
    coord  = CoordinateurQuorum(noeuds, W=3, R=2, timeout_ms=300)

    # Écriture sur tous
    coord.ecrire("config:version", "v1.0")
    time.sleep(0.1)

    print(f"  État initial (tous à jour) :")
    for n in noeuds: print(f"    N{n.id} → {n.etat('config:version')}")

    # Simuler N3 en retard : vider son store
    noeuds[2]._store.clear()
    print(f"\n  💥 N3 redémarre et perd ses données")

    # Mise à jour sans N3
    noeuds[0].ecrire("config:version", "v2.0", time.time())
    noeuds[1].ecrire("config:version", "v2.0", time.time())

    print(f"\n  État AVANT lecture (N3 est périmé) :")
    for n in noeuds: print(f"    N{n.id} → {n.etat('config:version')}")

    print(f"\n  READ config:version (R=2) — déclenche Read Repair en arrière-plan...")
    r = coord.lire("config:version", read_repair=True)
    print(f"  → Retourné au client : {r.valeur!r} ✅")

    # Attendre le Read Repair async
    time.sleep(0.2)

    print(f"\n  État APRÈS lecture (Read Repair a guéri N3) :")
    for n in noeuds: print(f"    N{n.id} → {n.etat('config:version')}")
    print(f"\n  Read repairs effectués : {coord.stats['read_repairs']}")
    print(f"""
  ✅ N3 est maintenant à jour sans qu'aucun admin n'ait intervenu.
  La simple lecture a déclenché la guérison automatique.

  Limites du Read Repair :
    → Ne répare que les clés lues (les clés "froides" restent périmées)
    → Solution complémentaire : "Anti-Entropy" (gossip protocol, Jour 10)
       qui compare les données entre nœuds périodiquement.
    """)


# ─── SCÉNARIO 5 : RÉSISTANCE AUX PANNES ──────────────────────────────────────

def scenario_pannes():
    titre("SCÉNARIO 5 — Résistance aux pannes : combien de nœuds peuvent tomber ?")

    print("""
  Avec N nœuds et un quorum W (ou R), combien de pannes tolère-t-on ?
    Pannes tolérées en écriture = N - W
    Pannes tolérées en lecture  = N - R

  Si QUORUM : W = R = N/2+1  → tolère N/2 pannes (arrondi inférieur)
    """)

    N = 5
    noeuds = creer_cluster(N, latence_ms=8, fiabilite=1.0)
    coord  = CoordinateurQuorum(noeuds, W=3, R=3, timeout_ms=500)  # QUORUM sur 5

    # Écriture initiale
    coord.ecrire("data:critique", "valeur_importante")
    time.sleep(0.1)

    print(f"  Cluster N={N}, W=3, R=3 (QUORUM). Pannes simulées progressivement :\n")
    print(f"  {'Nœuds actifs':<16} {'Écriture':<20} {'Lecture':<20} {'Opérations OK ?'}")
    print("  " + "─"*62)

    for nb_pannes in range(N + 1):
        # Remettre tout le monde en ligne
        for n in noeuds: n._actif = True
        # Tuer nb_pannes nœuds
        for i in range(nb_pannes):
            noeuds[i]._actif = False

        actifs = N - nb_pannes
        r_w    = coord.ecrire("data:critique", f"v{nb_pannes}")
        time.sleep(0.05)
        r_r    = coord.lire("data:critique")

        w_ok = r_w.statut == StatutOp.OK
        r_ok = r_r.statut == StatutOp.OK

        symbole = "✅" if (w_ok and r_ok) else ("⚠️ " if (w_ok or r_ok) else "❌")
        print(f"  {actifs}/{N} actifs ({nb_pannes} mort{'s' if nb_pannes>1 else ''})   "
              f"{'✅ OK' if w_ok else '❌ FAIL':<18}  "
              f"{'✅ OK' if r_ok else '❌ FAIL':<18}  {symbole}")

    # Remettre tout le monde
    for n in noeuds: n._actif = True

    print(f"""
  Avec QUORUM (W=R=3, N=5) : tolère {N//2} panne(s) simultanée(s).
  À {N//2 + 1} pannes → plus de quorum → cluster indisponible (CP).

  Règle générale :
    Tolérance aux pannes = N - W  (pour les écritures)
    Augmenter N (plus de répliques) augmente la tolérance.
    Diminuer W  augmente aussi la tolérance mais réduit la cohérence.
    """)


# ─── SCÉNARIO 6 : GUIDE DE CONFIGURATION ─────────────────────────────────────

def scenario_guide():
    titre("SCÉNARIO 6 — Guide de configuration : choisir W et R")

    print("""
  ┌──────────────────────────────────────────────────────────────┐
  │          RECETTES DE CONFIGURATION QUORUM                    │
  │                   (Cassandra / DynamoDB)                     │
  ├────────────┬─────┬─────┬────────────────────────────────────┤
  │ Recette    │  W  │  R  │ Cas d'usage                        │
  ├────────────┼─────┼─────┼────────────────────────────────────┤
  │ ONE / ONE  │  1  │  1  │ Logs, métriques IoT, compteurs     │
  │            │     │     │ Perte possible, latence minimale   │
  ├────────────┼─────┼─────┼────────────────────────────────────┤
  │ W=1, R=Q  │  1  │ N/2+1│ Lecture-intensive, écriture rapide│
  │            │     │     │ Ex : catalogue produit (lu 100x    │
  │            │     │     │ pour 1 écriture)                   │
  ├────────────┼─────┼─────┼────────────────────────────────────┤
  │ W=Q, R=1  │N/2+1│  1  │ Écriture-intensive, lecture rapide │
  │            │     │     │ Ex : ingestion de capteurs         │
  ├────────────┼─────┼─────┼────────────────────────────────────┤
  │ QUORUM    │N/2+1│N/2+1│ Équilibre. Recommandé par défaut.  │
  │            │     │     │ Sessions, profils utilisateur      │
  ├────────────┼─────┼─────┼────────────────────────────────────┤
  │ W=ALL, R=1│  N  │  1  │ Écriture ultra-sûre                │
  │            │     │     │ Ex : clés de chiffrement, tokens   │
  ├────────────┼─────┼─────┼────────────────────────────────────┤
  │ W=1, R=ALL│  1  │  N  │ Lecture ultra-cohérente            │
  │            │     │     │ Ex : config critique en lecture    │
  └────────────┴─────┴─────┴────────────────────────────────────┘

  DynamoDB — niveaux de cohérence :
    Eventual Consistency (défaut) = W=1, R=1  (2x moins cher)
    Strong Consistency            = W=Q, R=Q  (standard AWS)

  Cassandra — par requête :
    session.execute(query, consistency_level=ConsistencyLevel.QUORUM)
    → On peut mixer ONE pour les logs et QUORUM pour les paiements
      dans la même application !

  MongoDB — writeConcern + readPreference :
    {w: "majority", readPreference: "primary"}  → CP
    {w: 1,          readPreference: "secondary"} → AP

  Règle pratique :
    ┌─────────────────────────────────────────────────┐
    │ Données qui coûtent de l'argent → QUORUM ou +  │
    │ Données qui coûtent du temps    → ONE ou QUORUM │
    │ Données éphémères               → ONE / ONE     │
    └─────────────────────────────────────────────────┘
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 9 — QUORUM (LECTURE/ÉCRITURE) : W + R > N         ║")
    print("╚" + "═"*62 + "╝")

    scenario_preuve_coherence()
    scenario_benchmark()
    scenario_stale_reads()
    scenario_read_repair()
    scenario_pannes()
    scenario_guide()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Quorum — Ce qu'il faut retenir :

    N  = répliques totales
    W  = confirmations requises en écriture
    R  = réponses requises en lecture

    W + R > N  →  cohérence FORTE (chevauchement garanti)
    W + R ≤ N  →  cohérence ÉVENTUELLE (stale reads possibles)

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Preuve mathématique : W=2,R=2,N=3 → cohérence garantie
    Scénario 2 → Benchmark : ONE/ONE=fastest, ALL/ALL=slowest, QUORUM=sweet spot
    Scénario 3 → W=1,R=1 → stale reads démontrés sur 3/6 lectures
    Scénario 4 → Read Repair guérit N3 sans intervention humaine
    Scénario 5 → QUORUM/5 tolère 2 pannes, 3 pannes = cluster mort

  Lien avec les jours précédents :
    Jour 7 (Raft)  : W=N, R=1 en interne (log committé sur quorum)
    Jour 8 (CAP)   : W+R>N = CP | W+R≤N = AP — c'est le même curseur
    Jour 4 (HB)    : le coordinateur utilise HB pour savoir quels
                     nœuds sont disponibles avant d'envoyer les RPCs

  → Jour 10 : Gossip Protocol
    Comment une information se propage dans un cluster de 1000 nœuds
    sans coordinateur central — comme une rumeur dans une foule.
  """)


if __name__ == "__main__":
    main()
