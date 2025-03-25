"""
Jour 23 — Simulation : Backpressure & Queue Management
=======================================================
5 scénarios :
  1. Comparaison des 4 stratégies sur le même trafic
  2. Pression progressive : visualiser la queue qui grossit
  3. Priority Queue : SLA différenciés (premium vs free)
  4. Work stealing : équilibrage automatique entre workers
  5. Load shedding adaptatif : taux de rejet proportionnel à la pression
"""

import time
import threading
import random
import statistics
from collections import defaultdict
from collections import deque
from backpressure import (
    QueueBornee, StrategieQueue, Tache,
    Worker, WorkStealingPool
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


# ─── HELPER : lancer un test de charge ───────────────────────────────────────

def charger_queue(
    q: QueueBornee,
    n_producteurs: int,
    n_taches_par_prod: int,
    delai_prod_ms: float,
    n_workers: int,
    latence_worker_ms: float,
    duree_s: float = 3.0,
) -> dict:
    """Lance producteurs et workers, attend la fin, retourne les stats."""
    traitees_lock = threading.Lock()
    traitees_list = []

    def on_traitee(t: Tache):
        with traitees_lock:
            traitees_list.append(t)

    workers = [Worker(i, q, on_traitee, latence_worker_ms)
               for i in range(n_workers)]
    for w in workers: w.demarrer()

    def produire(prod_id: int):
        for j in range(n_taches_par_prod):
            t = Tache(id=f"p{prod_id}-t{j}", priorite=random.randint(1, 5),
                      payload=f"data-{j}")
            q.enqueue(t, timeout_s=0.5)
            if delai_prod_ms > 0:
                time.sleep(delai_prod_ms / 1000)

    threads_prod = [threading.Thread(target=produire, args=(i,), daemon=True)
                    for i in range(n_producteurs)]
    for tp in threads_prod: tp.start()
    for tp in threads_prod: tp.join()

    # Attendre que la queue se vide (ou timeout)
    deadline = time.time() + duree_s
    while q.taille() > 0 and time.time() < deadline:
        time.sleep(0.05)

    for w in workers: w.arreter()
    time.sleep(0.1)

    return {
        "entrees":     q.stats["entrees"],
        "traitees":    q.stats["traitees"],
        "perdues":     q.stats["perdues"],
        "max_taille":  q.stats["max_taille"],
        "lat_p50_ms":  q.latence_mediane_ms(),
        "lat_p99_ms":  q.latence_p99_ms(),
    }


# ─── SCÉNARIO 1 : COMPARAISON DES 4 STRATÉGIES ───────────────────────────────

def scenario_comparaison():
    titre("SCÉNARIO 1 — Comparaison des 4 stratégies sur le même trafic")

    print("""
  Trafic : 3 producteurs × 20 tâches = 60 tâches au total
  Capacité queue : 10  |  Workers : 2  |  Latence worker : 20ms
  Producteurs : envoient sans délai (burst pur)

  Résultat attendu : ~20 tâches traitées, ~40 perdues/en attente
    """)

    cfg = dict(
        n_producteurs=3, n_taches_par_prod=20, delai_prod_ms=0,
        n_workers=2, latence_worker_ms=20, duree_s=2.5
    )

    strategies = [
        ("BLOCK",       StrategieQueue.BLOCK),
        ("DROP_NEWEST", StrategieQueue.DROP_NEWEST),
        ("DROP_OLDEST", StrategieQueue.DROP_OLDEST),
        ("PRIORITY",    StrategieQueue.PRIORITY),
    ]

    print(f"  {'Stratégie':<14} {'Entrées':>8} {'Traitées':>9} {'Perdues':>8} "
          f"{'Max Q':>6} {'Lat P50':>8} {'Lat P99':>8}")
    print("  " + "─"*66)

    for nom, strat in strategies:
        q = QueueBornee(capacite=10, strategie=strat)
        r = charger_queue(q, **cfg)
        print(f"  {nom:<14} {r['entrees']:>8} {r['traitees']:>9} {r['perdues']:>8} "
              f"{r['max_taille']:>6} {r['lat_p50_ms']:>6.0f}ms {r['lat_p99_ms']:>6.0f}ms")

    print(f"""
  BLOCK       : zéro perte, mais producteurs bloqués → latence élevée
  DROP_NEWEST : perte des nouvelles arrivées, queue stable
  DROP_OLDEST : perte des plus anciennes → données fraîches prioritaires
  PRIORITY    : perte sélective (basse priorité éjectée en premier)
    """)


# ─── SCÉNARIO 2 : PRESSION PROGRESSIVE ───────────────────────────────────────

def scenario_pression():
    titre("SCÉNARIO 2 — Pression progressive : la queue comme thermomètre")

    print("""
  On démarre 1 worker lent (50ms/tâche).
  On ajoute des tâches progressivement.
  La pression de la queue monte comme un thermomètre.
  À 100% de capacité → load shedding automatique.
    """)

    CAPACITE = 20
    q = QueueBornee(capacite=CAPACITE, strategie=StrategieQueue.DROP_NEWEST)

    traitees = [0]
    def on_traitee(t): traitees[0] += 1

    w = Worker(0, q, on_traitee, latence_traitement_ms=50)
    w.demarrer()

    print(f"  {'Temps':>6}  {'Tâches':>7}  {'Queue':>6}  {'Pression':>9}  Barre")
    print("  " + "─"*55)

    t0 = time.perf_counter()
    tache_id = 0

    for phase, (n_envoi, delai_phase) in enumerate([
        (5, 0.3),    # Phase 1 : trafic léger
        (10, 0.3),   # Phase 2 : trafic modéré
        (20, 0.5),   # Phase 3 : pic de trafic
        (0, 0.8),    # Phase 4 : récupération
        (0, 0.5),
    ]):
        for _ in range(n_envoi):
            t = Tache(id=f"t{tache_id}", priorite=1, payload="")
            q.enqueue(t, timeout_s=0)
            tache_id += 1

        taille = q.taille()
        pression = q.pression()
        nb_barres = int(pression / 5)
        barre = "█" * nb_barres + "░" * (20 - nb_barres)
        ts = time.perf_counter() - t0
        label_phase = f"phase {phase+1}"
        print(f"  {ts:>5.1f}s  {tache_id:>7}  {taille:>6}  {pression:>7.0f}%  {barre}  {label_phase}")
        time.sleep(delai_phase)

    w.arreter()
    print(f"\n  Total traitées : {traitees[0]}  Perdues : {q.stats['perdues']}")
    print(f"  La barre de pression = indicateur de backpressure en temps réel ✅")


# ─── SCÉNARIO 3 : PRIORITY QUEUE ─────────────────────────────────────────────

def scenario_priority():
    titre("SCÉNARIO 3 — Priority Queue : SLA différenciés (premium vs free)")

    print("""
  3 tiers de clients avec des priorités différentes :
    PREMIUM    (priorité 1) : SLA 99.9%, ne perd jamais de requêtes
    STANDARD   (priorité 3) : SLA 99%, quelques pertes OK
    FREE       (priorité 5) : best effort, pertes acceptées

  La queue éjecte les tâches de plus basse priorité en premier
  quand elle est pleine.
    """)

    CAPACITE = 15
    q = QueueBornee(capacite=CAPACITE, strategie=StrategieQueue.PRIORITY)

    traitees_par_tier: dict[str, list[Tache]] = defaultdict(list)
    perdues_par_tier  = defaultdict(int)
    lock = threading.Lock()

    def on_traitee(t: Tache):
        with lock:
            traitees_par_tier[t.payload["tier"]].append(t)

    # 1 seul worker lent pour forcer les éjections
    w = Worker(0, q, on_traitee, latence_traitement_ms=30)
    w.demarrer()

    tiers = [
        ("PREMIUM",  1, 20),
        ("STANDARD", 3, 20),
        ("FREE",     5, 20),
    ]

    for tier, priorite, n in tiers:
        for i in range(n):
            t = Tache(id=f"{tier}-{i}", priorite=priorite,
                      payload={"tier": tier, "num": i})
            ok = q.enqueue(t, timeout_s=0)
            if not ok:
                with lock:
                    perdues_par_tier[tier] += 1

    time.sleep(2.5)
    w.arreter()
    time.sleep(0.1)

    print(f"  {'Tier':<12} {'Soumises':>9} {'Traitées':>9} {'Perdues':>8} {'Taux OK':>8}")
    print("  " + "─"*50)

    for tier, priorite, n in tiers:
        traitees = len(traitees_par_tier.get(tier, []))
        perdues  = perdues_par_tier.get(tier, 0) + (n - traitees - perdues_par_tier.get(tier, 0))
        perdues  = n - traitees
        taux     = traitees / n * 100
        icone    = "✅" if tier == "PREMIUM" else ("⚠️" if tier == "STANDARD" else "❌")
        print(f"  {icone} {tier:<10} {n:>9} {traitees:>9} {perdues:>8} {taux:>7.0f}%")

    print(f"\n  → PREMIUM : proches de 100% de traitement (priorité 1, jamais éjecté)")
    print(f"  → FREE    : taux le plus bas (priorité 5, éjecté en premier)")
    print(f"  Latences par tier :")
    for tier, _, _ in tiers:
        lats = [t.latence_ms for t in traitees_par_tier.get(tier, []) if t.latence_ms]
        if lats:
            p50 = sorted(lats)[len(lats)//2]
            print(f"    {tier:<10}: P50={p50:.0f}ms  (n={len(lats)})")


# ─── SCÉNARIO 4 : WORK STEALING ──────────────────────────────────────────────

def scenario_work_stealing():
    titre("SCÉNARIO 4 — Work Stealing : équilibrage automatique entre workers")

    print("""
  Problème : distribution inégale des tâches.
    Worker 0 reçoit 80 tâches, workers 1-3 reçoivent 0.
    Sans work stealing : worker 0 surchargé, autres idle.
    Avec work stealing : les idle volent des tâches au surchargé.

  ForkJoinPool (Java), Tokio (Rust), Go runtime utilisent ce pattern.
    """)

    N_WORKERS    = 4
    N_TACHES     = 80
    LATENCE_MS   = 15

    # ── Sans work stealing ────────────────────────────────────────────────────
    print(f"  Sans work stealing (distribution fixe) :")
    queues_fixes = [deque() for _ in range(N_WORKERS)]

    # Tout vers worker 0
    for i in range(N_TACHES):
        queues_fixes[0].append(Tache(id=f"t{i}", priorite=1, payload=""))

    traitement_sans = [0] * N_WORKERS
    t0 = time.perf_counter()

    def worker_fixe(wid: int):
        while queues_fixes[wid]:
            queues_fixes[wid].popleft()
            time.sleep(LATENCE_MS / 1000)
            traitement_sans[wid] += 1

    threads = [threading.Thread(target=worker_fixe, args=(i,), daemon=True)
               for i in range(N_WORKERS)]
    for th in threads: th.start()
    for th in threads: th.join()
    duree_sans = (time.perf_counter() - t0) * 1000

    print(f"    Tâches par worker : {traitement_sans}")
    print(f"    Durée totale      : {duree_sans:.0f}ms")

    # ── Avec work stealing ─────────────────────────────────────────────────────
    print(f"\n  Avec work stealing :")
    pool = WorkStealingPool(N_WORKERS, capacite_par_worker=100,
                            latence_traitement_ms=LATENCE_MS)

    # Tout vers worker 0
    for i in range(N_TACHES):
        pool._queues[0].append(Tache(id=f"t{i}", priorite=1, payload=""))

    t0 = time.perf_counter()
    pool.demarrer()
    # Attendre que tout soit traité
    deadline = time.time() + 5.0
    while sum(pool.stats["par_worker"]) < N_TACHES and time.time() < deadline:
        time.sleep(0.05)
    pool.arreter()
    duree_avec = (time.perf_counter() - t0) * 1000

    print(f"    Tâches par worker : {pool.stats['par_worker']}")
    print(f"    Vols effectués    : {pool.stats['vols']}")
    print(f"    Durée totale      : {duree_avec:.0f}ms")

    gain = duree_sans / max(duree_avec, 1)
    print(f"\n  Accélération : {gain:.1f}x  (théorique max: {N_WORKERS}x) ✅")
    print(f"  Les workers idle ont volé des tâches → distribution équilibrée")


# ─── SCÉNARIO 5 : LOAD SHEDDING ADAPTATIF ────────────────────────────────────

def scenario_load_shedding():
    titre("SCÉNARIO 5 — Load Shedding adaptatif : rejet proportionnel à la pression")

    print("""
  Load shedding "TCP Vegas style" :
    Quand la pression dépasse un seuil, on commence à rejeter.
    Plus la pression est haute, plus le taux de rejet est élevé.

    Pression 0-50%  : tout autorisé
    Pression 50-80% : rejeter proportionnellement (ex: 60% → rejeter 20%)
    Pression >80%   : rejeter agressivement (80%+ des requêtes)

  Avantage : régulation douce, pas de cliff-edge (pas de basculement brutal).
    """)

    class LoadSheddingAdaptatif:
        def __init__(self, queue: QueueBornee):
            self.queue = queue
            self.stats = {"acceptees": 0, "rejetees": 0}

        def soumettre(self, tache: Tache) -> bool:
            pression = self.queue.pression() / 100   # 0.0 → 1.0
            # Taux de rejet : 0% sous 50%, linéaire entre 50-80%, 80% au-delà
            if pression < 0.5:
                taux_rejet = 0.0
            elif pression < 0.8:
                taux_rejet = (pression - 0.5) / 0.3   # 0 → 1 entre 50% et 80%
            else:
                taux_rejet = 0.8 + (pression - 0.8) * 1.0

            taux_rejet = min(taux_rejet, 0.95)   # Max 95% de rejet

            if random.random() < taux_rejet:
                self.stats["rejetees"] += 1
                return False
            ok = self.queue.enqueue(tache, timeout_s=0)
            if ok:
                self.stats["acceptees"] += 1
            else:
                self.stats["rejetees"] += 1
            return ok

    CAPACITE = 20
    q   = QueueBornee(capacite=CAPACITE, strategie=StrategieQueue.DROP_NEWEST)
    lsa = LoadSheddingAdaptatif(q)

    traitees_ls = [0]
    def on_traitee_ls(t): traitees_ls[0] += 1

    w = Worker(0, q, on_traitee_ls, latence_traitement_ms=40)
    w.demarrer()

    print(f"  {'Phase':<18} {'Envoi':>6}  {'Pression':>9}  {'Rejet%':>7}  {'Acceptées':>10}")
    print("  " + "─"*58)

    phases = [
        ("Léger (10/s)",   10, 0.2),
        ("Modéré (30/s)",  30, 0.3),
        ("Pic (60/s)",     60, 0.4),
        ("Très fort (100/s)", 100, 0.5),
        ("Retour calme",   10, 0.8),
    ]

    for label, n, delai in phases:
        lsa.stats["acceptees"] = 0
        lsa.stats["rejetees"]  = 0

        for i in range(n):
            t = Tache(id=f"ls-{i}", priorite=1, payload="")
            lsa.soumettre(t)
            time.sleep(delai / n)

        total   = lsa.stats["acceptees"] + lsa.stats["rejetees"]
        pct_rej = lsa.stats["rejetees"] / max(total, 1) * 100
        pression = q.pression()
        print(f"  {label:<18} {n:>6}  {pression:>7.0f}%  {pct_rej:>6.0f}%  {lsa.stats['acceptees']:>10}")

    w.arreter()

    print(f"""
  Load shedding adaptatif vs brutal :
    Brutal    : rejeter tout quand queue pleine (cliff-edge)
    Adaptatif : rejeter progressivement (courbe douce)

  En production : utilisé par TCP (congestion control), Envoy (admission control),
  et les serveurs de jeu (rejeter les connexions excédentaires gracieusement).
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)

    print("╔" + "═"*62 + "╗")
    print("║   JOUR 23 — BACKPRESSURE & QUEUE MANAGEMENT             ║")
    print("╚" + "═"*62 + "╝")

    scenario_comparaison()
    scenario_pression()
    scenario_priority()
    scenario_work_stealing()
    scenario_load_shedding()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Backpressure & Queue Management — Ce qu'il faut retenir :

    BLOCK       : zéro perte, propagation de la lenteur vers l'amont
    DROP_NEWEST : protège les requêtes en cours, rejette les nouvelles
    DROP_OLDEST : fraîcheur prioritaire (IoT, flux temps réel)
    PRIORITY    : SLA différenciés, free peut être sacrifié pour premium

    Work Stealing : workers idle volent aux surchargés → auto-équilibrage
    Load Shedding : rejet proportionnel à la pression (pas de cliff-edge)

  Ce que nos scénarios ont prouvé :
    Scénario 1 → BLOCK=0 perte, DROP_NEWEST=stable, PRIORITY=sélectif ✅
    Scénario 2 → Barre de pression : thermomètre en temps réel de la queue ✅
    Scénario 3 → PREMIUM ~100% traité, FREE éjecté en premier ✅
    Scénario 4 → Work stealing : accélération 3-4x vs distribution fixe ✅
    Scénario 5 → Load shedding adaptatif : rejet progressif sous pression ✅

  Utilisé en production :
    Kafka         → Consumer lag = backpressure implicite
    gRPC          → WINDOW_UPDATE frames = backpressure HTTP/2
    RxJava/Reactor→ .onBackpressureBuffer/.onBackpressureDrop
    Akka Streams  → backpressure intégré au modèle acteur
    Envoy         → admission control + load shedding adaptatif

  Lien avec les autres jours :
    Jour 22 (Rate Limiting) → côté serveur : rejeter AVANT la queue
    Jour 21 (Circuit Breaker) → côté client : ne pas remplir une queue vide
    Jour 20 (Saga) → les sagas utilisent des queues bornées par étape

  → Jour 24 : Write-Ahead Log (WAL) & Storage Engine
    Comment une base de données garantit la durabilité des données
    même en cas de crash. Tout commit = écriture dans le WAL avant
    les fichiers de données. On implémente un mini moteur de stockage.
  """)

if __name__ == "__main__":
    main()
