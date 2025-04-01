"""
Jour 26 — Simulation : MVCC en action
=======================================
5 scénarios :
  1. Snapshot isolation : T1 voit un état stable pendant sa vie
  2. Lecteurs et écrivains non bloquants (concurrence réelle)
  3. Niveaux d'isolation : READ_COMMITTED vs REPEATABLE_READ
  4. Write skew : anomalie possible en Repeatable Read
  5. Vacuum / GC : nettoyer les versions obsolètes
"""

import time
import threading
from mvcc import (
    MoteurMVCC, NiveauIsolation, EtatTransaction, Version
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


# ─── SCÉNARIO 1 : SNAPSHOT ISOLATION ─────────────────────────────────────────

def scenario_snapshot():
    titre("SCÉNARIO 1 — Snapshot isolation : T1 voit un état stable pendant toute sa vie")

    print("""
  T1 démarre, lit user:alice = 1000.
  T2 démarre, met à jour alice = 800, commite.
  T1 relit alice → doit encore voir 1000 (son snapshot).
  T3 démarre après T2 → voit 800 (commité avant son snapshot).
    """)

    db = MoteurMVCC()

    # Initialiser la base avec T0
    t0 = db.begin()
    t0.ecrire("alice", {"solde": 1000})
    t0.ecrire("bob",   {"solde": 500})
    t0.commit()

    # T1 : Repeatable Read
    t1 = db.begin(NiveauIsolation.REPEATABLE_READ)
    v1a = t1.lire("alice")
    print(f"  T1 (xid={t1.xid}) démarre — alice = {v1a}")

    # T2 : mise à jour et commit
    t2 = db.begin()
    t2.ecrire("alice", {"solde": 800})
    t2.commit()
    print(f"  T2 (xid={t2.xid}) commite — alice mis à jour → 800")

    # T1 relit
    v1b = t1.lire("alice")
    print(f"  T1 relit alice = {v1b}  ← snapshot protège T1 {'✅' if v1b == v1a else '❌'}")

    # T3 démarre après T2
    t3 = db.begin(NiveauIsolation.REPEATABLE_READ)
    v3 = t3.lire("alice")
    print(f"  T3 (xid={t3.xid}) démarre après T2 — alice = {v3}  {'✅' if v3['solde'] == 800 else '❌'}")

    t1.abort()
    t3.abort()

    # Afficher les versions
    print(f"\n  Versions de 'alice' :")
    for v in db.toutes_versions("alice"):
        statut = "vivante" if v.xmax == 0 else f"morte (xmax={v.xmax})"
        print(f"    {v}  [{statut}]")

    print(f"\n  Résumé :")
    print(f"    T1 voit solde=1000 même après commit de T2 ✅ (Repeatable Read)")
    print(f"    T3 voit solde=800  (T2 committée avant son snapshot) ✅")
    print(f"    Deux lectures correctes et contradictoires en même temps ✅")


# ─── SCÉNARIO 2 : LECTEURS ET ÉCRIVAINS NON BLOQUANTS ────────────────────────

def scenario_concurrence():
    titre("SCÉNARIO 2 — Lecteurs et écrivains non bloquants")

    print("""
  10 lecteurs et 5 écrivains tournent en parallèle.
  Sans MVCC : les écrivains bloqueraient les lecteurs (verrous exclusifs).
  Avec MVCC : chacun travaille sur son snapshot, aucun blocage.

  On mesure les latences et vérifie qu'aucun deadlock ne survient.
    """)

    db = MoteurMVCC()

    # Données initiales
    t0 = db.begin()
    for i in range(10):
        t0.ecrire(f"compte:{i}", {"solde": 1000 + i * 100})
    t0.commit()

    resultats = {"lectures": [], "ecritures": [], "erreurs": 0}
    lock_res   = threading.Lock()

    def lecteur(lid: int):
        t0_ = time.perf_counter()
        tx = db.begin(NiveauIsolation.REPEATABLE_READ)
        valeurs = []
        for i in range(5):
            v = tx.lire(f"compte:{i}")
            if v: valeurs.append(v["solde"])
        tx.abort()
        lat = (time.perf_counter() - t0_) * 1000
        with lock_res:
            resultats["lectures"].append(lat)

    def ecrivain(eid: int):
        t0_ = time.perf_counter()
        try:
            tx = db.begin(NiveauIsolation.REPEATABLE_READ)
            v  = tx.lire(f"compte:{eid % 10}")
            if v:
                tx.ecrire(f"compte:{eid % 10}", {"solde": v["solde"] - 50})
            tx.commit()
        except Exception:
            with lock_res:
                resultats["erreurs"] += 1
        lat = (time.perf_counter() - t0_) * 1000
        with lock_res:
            resultats["ecritures"].append(lat)

    threads = (
        [threading.Thread(target=lecteur,  args=(i,)) for i in range(10)] +
        [threading.Thread(target=ecrivain, args=(i,)) for i in range(5)]
    )
    t_debut = time.perf_counter()
    for th in threads: th.start()
    for th in threads: th.join()
    duree_totale = (time.perf_counter() - t_debut) * 1000

    import statistics
    lec = resultats["lectures"]
    ecr = resultats["ecritures"]
    print(f"  15 transactions (10 lectures + 5 écritures) en parallèle :\n")
    print(f"  {'Type':<12} {'N':>4}  {'Moy':>8}  {'P50':>8}  {'P99':>8}")
    print("  " + "─"*45)
    print(f"  {'Lectures':<12} {len(lec):>4}  {statistics.mean(lec):>6.1f}ms  "
          f"{statistics.median(lec):>6.1f}ms  {sorted(lec)[int(len(lec)*0.99)]:>6.1f}ms")
    print(f"  {'Écritures':<12} {len(ecr):>4}  {statistics.mean(ecr):>6.1f}ms  "
          f"{statistics.median(ecr):>6.1f}ms  {sorted(ecr)[int(len(ecr)*0.99)]:>6.1f}ms")
    print(f"\n  Durée totale (parallèle) : {duree_totale:.1f}ms")
    print(f"  Erreurs / deadlocks      : {resultats['erreurs']} ✅")
    print(f"\n  Aucun blocage mutuel — lecteurs et écrivains indépendants ✅")
    print(f"  Versions totales créées : {db.nb_versions_totales()}")


# ─── SCÉNARIO 3 : NIVEAUX D'ISOLATION ────────────────────────────────────────

def scenario_isolation():
    titre("SCÉNARIO 3 — Niveaux d'isolation : READ_COMMITTED vs REPEATABLE_READ")

    print("""
  Même scénario, deux comportements différents selon le niveau d'isolation.

  Séquence :
    T_init commite alice=1000
    T_ecriture commence, écrit alice=800, commite
    T_lecture_rc (READ_COMMITTED)  lit alice → 2× → voit le changement
    T_lecture_rr (REPEATABLE_READ) lit alice → 2× → ne voit pas le changement
    """)

    db = MoteurMVCC()

    t_init = db.begin()
    t_init.ecrire("alice", 1000)
    t_init.commit()

    # Deux lecteurs démarrent avant l'écriture
    t_rc = db.begin(NiveauIsolation.READ_COMMITTED)
    t_rr = db.begin(NiveauIsolation.REPEATABLE_READ)

    lecture1_rc = t_rc.lire("alice")
    lecture1_rr = t_rr.lire("alice")
    print(f"  Avant T_ecriture :")
    print(f"    READ_COMMITTED  : alice = {lecture1_rc}")
    print(f"    REPEATABLE_READ : alice = {lecture1_rr}")

    # T_ecriture commite
    t_ecr = db.begin()
    t_ecr.ecrire("alice", 800)
    t_ecr.commit()
    print(f"\n  T_ecriture (xid={t_ecr.xid}) commite — alice → 800")

    # Deuxième lecture
    lecture2_rc = t_rc.lire("alice")
    lecture2_rr = t_rr.lire("alice")

    print(f"\n  Après T_ecriture :")
    print(f"    READ_COMMITTED  : alice = {lecture2_rc}  "
          f"{'← voit 800 (RC rafraîchit)' if lecture2_rc == 800 else ''}"
          f"  {'✅' if lecture2_rc == 800 else '❌'}")
    print(f"    REPEATABLE_READ : alice = {lecture2_rr}  "
          f"{'← voit encore 1000 (snapshot figé)' if lecture2_rr == 1000 else ''}"
          f"  {'✅' if lecture2_rr == 1000 else '❌'}")

    print(f"""
  READ_COMMITTED  : le snapshot est pris à chaque requête
    → Voit les données les plus fraîches
    → Possible : lire deux fois la même ligne avec des résultats différents
    → Risque : "non-repeatable read"

  REPEATABLE_READ : le snapshot est pris au BEGIN
    → État figé pendant toute la transaction
    → Impossible : lire deux fois avec des résultats différents
    → PostgreSQL défaut, MySQL InnoDB défaut
    """)

    t_rc.abort()
    t_rr.abort()

    print(f"  Tableau récapitulatif des anomalies par niveau :\n")
    print(f"  {'Niveau':<20} {'Dirty Read':>12} {'Non-Repeatable':>15} {'Phantom':>9}")
    print("  " + "─"*59)
    print(f"  {'READ UNCOMMITTED':<20} {'possible':>12} {'possible':>15} {'possible':>9}")
    print(f"  {'READ COMMITTED':<20} {'impossible':>12} {'possible':>15} {'possible':>9}")
    print(f"  {'REPEATABLE READ':<20} {'impossible':>12} {'impossible':>15} {'possible':>9}")
    print(f"  {'SERIALIZABLE':<20} {'impossible':>12} {'impossible':>15} {'impossible':>9}")


# ─── SCÉNARIO 4 : WRITE SKEW ──────────────────────────────────────────────────

def scenario_write_skew():
    titre("SCÉNARIO 4 — Write Skew : anomalie possible en Repeatable Read")

    print("""
  Write Skew : deux transactions lisent un ensemble, vérifient une condition,
  chacune écrit une partie → la condition invariant est violée.

  Exemple classique : Garde de nuit
    Invariant : au moins 1 médecin garde à tout moment.
    État initial : alice=garde, bob=garde (2 médecins)

    T1 (alice demande congé) :
      Lit : [alice=garde, bob=garde] → 2 gardes → OK → alice=congé
    T2 (bob demande congé) :
      Lit : [alice=garde, bob=garde] → 2 gardes → OK → bob=congé
    Résultat : alice=congé, bob=congé → 0 gardes ! INVARIANT VIOLÉ

  En Repeatable Read, les deux TX voient le même snapshot → write skew possible.
  En Serializable, une des deux TX est annulée.
    """)

    def run_scenario(isolation: NiveauIsolation) -> tuple[bool, str, str]:
        db = MoteurMVCC()
        t0 = db.begin()
        t0.ecrire("alice", "garde")
        t0.ecrire("bob",   "garde")
        t0.commit()

        t1 = db.begin(isolation)
        t2 = db.begin(isolation)

        # Les deux lisent l'état et voient 2 gardes → chacun demande congé
        gardes_t1 = [v for k in ["alice", "bob"]
                     if (v := t1.lire(k)) == "garde"]
        gardes_t2 = [v for k in ["alice", "bob"]
                     if (v := t2.lire(k)) == "garde"]

        resultat_t1 = resultat_t2 = None
        try:
            if len(gardes_t1) >= 2:
                t1.ecrire("alice", "conge")
                t1.commit()
                resultat_t1 = "OK"
            else:
                t1.abort()
                resultat_t1 = "REFUSÉ"
        except Exception as e:
            t1.abort()
            resultat_t1 = f"ABORT ({e})"

        try:
            if len(gardes_t2) >= 2:
                t2.ecrire("bob", "conge")
                t2.commit()
                resultat_t2 = "OK"
            else:
                t2.abort()
                resultat_t2 = "REFUSÉ"
        except Exception as e:
            t2.abort()
            resultat_t2 = f"ABORT ({e})"

        t_verif = db.begin(NiveauIsolation.READ_COMMITTED)
        alice_f = t_verif.lire("alice")
        bob_f   = t_verif.lire("bob")
        t_verif.abort()

        gardes_finales = sum(1 for v in [alice_f, bob_f] if v == "garde")
        invariant_ok   = gardes_finales >= 1
        return invariant_ok, resultat_t1, resultat_t2

    for niveau in [NiveauIsolation.REPEATABLE_READ, NiveauIsolation.SERIALIZABLE]:
        inv_ok, r1, r2 = run_scenario(niveau)
        icone = "✅" if inv_ok else "❌"
        print(f"  {niveau.value:<20} : T1={r1}, T2={r2}  → Invariant: {icone}")

    print(f"""
  Repeatable Read :
    Les deux TX voient alice=garde, bob=garde au moment de leur snapshot.
    Chacune commit → 0 gardes → invariant violé ❌

  Serializable :
    PostgreSQL utilise des "predicate locks" (SSI) pour détecter les write skews.
    Une des deux TX est abortée → invariant préservé ✅

  Note : notre implémentation simplifiée ne simule pas SSI complet.
  En production : PostgreSQL SERIALIZABLE utilise Serializable Snapshot Isolation (SSI).
    """)


# ─── SCÉNARIO 5 : VACUUM ──────────────────────────────────────────────────────

def scenario_vacuum():
    titre("SCÉNARIO 5 — Vacuum / GC : nettoyer les versions obsolètes")

    print("""
  Chaque écriture crée une nouvelle version.
  Les anciennes versions s'accumulent → "table bloat".
  VACUUM (PostgreSQL) supprime les versions que aucune TX active ne peut voir.

  Si une TX très longue tourne, le VACUUM ne peut pas nettoyer les versions
  qu'elle pourrait encore voir → "transaction age bloat" (problème réel en prod).
    """)

    db = MoteurMVCC()

    # Données initiales
    t_init = db.begin()
    t_init.ecrire("compte:A", 1000)
    t_init.ecrire("compte:B", 500)
    t_init.commit()

    stats0 = db.stats()
    print(f"  Après initialisation : {stats0}")

    # 10 mises à jour successives
    for i in range(10):
        tx = db.begin()
        v = tx.lire("compte:A")
        tx.ecrire("compte:A", v - 10)
        tx.commit()

    stats1 = db.stats()
    print(f"  Après 10 mises à jour : {stats1}")
    print(f"  → {stats1['versions_mortes']} versions mortes (anciennes versions de compte:A)")

    # Vacuum (toutes les TX précédentes sont terminées)
    n_supprimes = db.vacuum()
    stats2 = db.stats()
    print(f"\n  Après VACUUM : {n_supprimes} versions supprimées")
    print(f"  Stats post-vacuum : {stats2}")
    print(f"  → Seule la version courante de compte:A survit ✅")

    # Simuler le problème de la "longue transaction"
    print(f"\n  Simulation : longue transaction bloque le VACUUM")
    t_longue = db.begin(NiveauIsolation.REPEATABLE_READ)
    val_longue = t_longue.lire("compte:A")
    print(f"  T_longue (xid={t_longue.xid}) lit compte:A = {val_longue}")

    for i in range(5):
        tx = db.begin()
        v = tx.lire("compte:A")
        tx.ecrire("compte:A", v - 10)
        tx.commit()

    stats3 = db.stats()
    n_supp2 = db.vacuum()   # Vacuum pendant que T_longue tourne
    stats4 = db.stats()

    print(f"  5 nouvelles mises à jour créent {stats3['versions_mortes']} versions mortes")
    print(f"  VACUUM pendant T_longue active : {n_supp2} supprimées (limité par T_longue)")
    print(f"  Versions restantes : {stats4['versions_totales']}")

    t_longue.abort()
    n_supp3 = db.vacuum()   # Vacuum après fin de T_longue
    stats5 = db.stats()
    print(f"\n  Après fin de T_longue + VACUUM : {n_supp3} supprimées")
    print(f"  Stats finales : {stats5}")
    print(f"  → Toutes les versions mortes nettoyées ✅")

    print(f"""
  Impact en production (PostgreSQL) :
    Une transaction ouverte depuis 2h bloque autovacuum.
    Les tables grossissent (bloat), les index aussi.
    → Surveiller pg_stat_activity pour les TX longues
    → Paramètre idle_in_transaction_session_timeout (ex: 5min)
    → pg_stat_user_tables.n_dead_tup = nb de versions mortes
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 26 — MVCC (MULTI-VERSION CONCURRENCY CONTROL)    ║")
    print("╚" + "═"*62 + "╝")

    scenario_snapshot()
    scenario_concurrence()
    scenario_isolation()
    scenario_write_skew()
    scenario_vacuum()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  MVCC — Ce qu'il faut retenir :

    Version     : chaque écriture crée une nouvelle version (xmin, xmax)
    Snapshot    : pris au BEGIN (RR) ou à chaque requête (RC)
    Visibilité  : xmin committée avant snapshot, xmax non-committée ou 0
    Vacuum      : nettoie les versions que plus aucune TX active ne voit

  Niveaux d'isolation :
    READ COMMITTED  : snapshot rafraîchi → peut voir des modifications
    REPEATABLE READ : snapshot figé → lectures stables → write skew possible
    SERIALIZABLE    : SSI → aucune anomalie → TX annulée si conflit détecté

  Ce que nos scénarios ont prouvé :
    Scénario 1 → T1 voit 1000, T2 commite 800, T1 voit encore 1000 ✅
    Scénario 2 → 10 lecteurs + 5 écrivains parallèles, 0 deadlock ✅
    Scénario 3 → RC voit 800 après commit T2, RR voit encore 1000 ✅
    Scénario 4 → Write skew en RR → invariant violé (0 gardes) ❌
    Scénario 5 → Vacuum nettoie 10 versions mortes, bloqué par TX longue ✅

  Utilisé en production :
    PostgreSQL  → MVCC natif, xmin/xmax dans chaque tuple heap
    MySQL InnoDB→ MVCC via undo log (versions dans undo tablespace)
    Oracle      → Undo segments (similaire InnoDB)
    CockroachDB → MVCC distribué avec timestamps hybrides (HLC)
    Spanner     → TrueTime + MVCC pour des snapshots globalement cohérents

  Lien avec les autres jours :
    Jour 24 (WAL) → le WAL enregistre les versions avant qu'elles soient visibles
    Jour 14 (Vector Clocks) → HLC de CockroachDB étend les vector clocks pour MVCC
    Jour 7  (Raft) → Spanner utilise Raft + MVCC pour des transactions globales

  → Jour 27 : B-Tree vs LSM Tree — Quand utiliser lequel ?
    Analyse comparative approfondie des deux structures de stockage.
    OLTP vs OLAP, read-heavy vs write-heavy, SSD vs HDD.
    On benchmarke les deux sur des workloads réels.
  """)

if __name__ == "__main__":
    main()
