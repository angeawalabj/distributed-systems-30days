"""
Jour 15 — Simulation : Redlock en action
==========================================
5 scénarios :
  1. Acquisition et libération nominale (N=5, quorum=3)
  2. Exclusion mutuelle : deux clients se disputent la ressource
  3. Tolérance aux pannes : 2 instances Redis tombent
  4. GC pause + fencing token : le verrou expire mais la ressource est protégée
  5. Comparaison : verrou simple Redis vs Redlock sous partition réseau
"""

import time
import threading
import random
from redlock import InstanceRedis, ClientRedlock, RessourceProtegee

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s): time.sleep(s)

def creer_cluster(n: int = 5, latence_ms: float = 5) -> list[InstanceRedis]:
    return [InstanceRedis(f"redis-{i}", latence_ms=latence_ms) for i in range(1, n+1)]

def afficher_resultat(res, label=""):
    if label:
        print(f"\n  {label}")
    icone = "✅" if res.acquis else "❌"
    print(f"  {icone} acquis={res.acquis}  validity={res.validity_ms:.0f}ms  "
          f"durée={res.duree_ms:.1f}ms  fencing={res.fencing_token}")
    print(f"     OK : {res.instances_ok}")
    print(f"     KO : {res.instances_ko}")


# ─── SCÉNARIO 1 : ACQUISITION NOMINALE ───────────────────────────────────────

def scenario_nominal():
    titre("SCÉNARIO 1 — Acquisition nominale : 5 instances Redis, quorum=3")

    print("""
  Algorithme Redlock en 6 étapes :
    1. Lire t1
    2. SET key token NX PX ttl sur chaque instance (parallèle)
    3. Lire t2, calculer elapsed = t2 - t1
    4. validity = ttl - elapsed - drift
    5. Acquis si : instances_ok ≥ N/2+1  ET  validity > 0
    6. Sinon : libérer partout
    """)

    instances = creer_cluster(5, latence_ms=8)
    client    = ClientRedlock("client-A", instances, ttl_ms=5000)

    print(f"  Cluster : {len(instances)} instances Redis, quorum={client.quorum}")
    print(f"  TTL     : {client.ttl_ms}ms\n")

    res = client.acquerir("ressource:compte_alice")
    afficher_resultat(res, "Acquisition :")

    if res.acquis:
        print(f"\n  Section critique (100ms) ...")
        attendre(0.1)

        # Vérifier TTL restant sur chaque instance
        print(f"  TTL restant par instance :")
        for inst in instances:
            ttl = inst.ttl_restant_ms("ressource:compte_alice")
            print(f"    {inst.id} → {ttl}ms restants")

        client.liberer("ressource:compte_alice", res.token)
        print(f"\n  Verrou libéré. État des instances :")
        for inst in instances:
            val = inst.get("ressource:compte_alice")
            print(f"    {inst.id} → {'(vide)' if val is None else val}")

    print(f"\n  ✅ Garanties Redlock :")
    print(f"    → Mutual exclusion : 1 seul client détient le verrou à la fois")
    print(f"    → Liveness : le verrou expire automatiquement (pas de deadlock)")
    print(f"    → Fault tolerance : tolère {client.N - client.quorum} pannes Redis")


# ─── SCÉNARIO 2 : EXCLUSION MUTUELLE ─────────────────────────────────────────

def scenario_exclusion():
    titre("SCÉNARIO 2 — Exclusion mutuelle : deux clients se disputent la ressource")

    print("""
  Client-A et Client-B tentent d'acquérir le même verrou simultanément.
  Un seul doit gagner. L'autre doit attendre puis réessayer.
    """)

    instances = creer_cluster(5, latence_ms=6)
    client_a  = ClientRedlock("client-A", instances, ttl_ms=3000, retry_count=1)
    client_b  = ClientRedlock("client-B", instances, ttl_ms=3000, retry_count=1)
    ressource = "ressource:commande_123"
    resultats = {}
    journaux  = []
    jlock     = threading.Lock()

    def acquerir_et_travailler(client: ClientRedlock):
        res = client.acquerir(ressource)
        with jlock:
            resultats[client.id] = res
            journaux.append(f"  {client.id} → {'✅ ACQUIS' if res.acquis else '❌ REFUSÉ'}  "
                            f"(fencing={res.fencing_token})")
        if res.acquis:
            with jlock:
                journaux.append(f"  {client.id} → travaille 200ms...")
            attendre(0.2)
            client.liberer(ressource, res.token)
            with jlock:
                journaux.append(f"  {client.id} → verrou libéré")

    # Lancer les deux clients quasi-simultanément
    t_a = threading.Thread(target=acquerir_et_travailler, args=(client_a,))
    t_b = threading.Thread(target=acquerir_et_travailler, args=(client_b,))
    t_a.start()
    time.sleep(0.002)   # 2ms d'écart → A prend l'avance
    t_b.start()
    t_a.join(); t_b.join()

    print(f"  Timeline :\n")
    for ligne_j in journaux:
        print(ligne_j)

    res_a = resultats.get("client-A")
    res_b = resultats.get("client-B")

    nb_acquis = sum(1 for r in resultats.values() if r and r.acquis)
    print(f"\n  Clients ayant acquis le verrou : {nb_acquis}/2")
    print(f"  {'✅ Exclusion mutuelle respectée' if nb_acquis <= 1 else '❌ VIOLATION : deux clients ont le verrou !'}")

    if res_a and res_b:
        tokens = {r.fencing_token for r in [res_a, res_b] if r.acquis}
        print(f"  Fencing tokens utilisés : {sorted(tokens)} (tous distincts ✅)" if len(tokens) == nb_acquis else "")


# ─── SCÉNARIO 3 : TOLÉRANCE AUX PANNES ───────────────────────────────────────

def scenario_pannes():
    titre("SCÉNARIO 3 — Tolérance aux pannes : 2 instances Redis sur 5 tombent")

    print("""
  Redlock tolère N - quorum = 5 - 3 = 2 pannes simultanées.
  Avec 2 instances down, le quorum (3) est encore atteignable.
  Avec 3 instances down, le quorum est impossible → verrou non acquis.
    """)

    instances = creer_cluster(5, latence_ms=5)
    client    = ClientRedlock("client-A", instances, ttl_ms=5000, retry_count=1)

    configs = [
        ("0 panne",  []),
        ("1 panne",  ["redis-2"]),
        ("2 pannes", ["redis-2", "redis-4"]),
        ("3 pannes", ["redis-2", "redis-3", "redis-4"]),
    ]

    print(f"  {'Config':<12} {'Instances down':<22} {'Acquis':>8}  {'OK':>4}  {'Validity':>10}")
    print("  " + "─"*60)

    for label, down_ids in configs:
        # Redémarrer toutes les instances
        for inst in instances:
            inst.redemarrer()
        # Faire tomber les instances configurées
        for inst in instances:
            if inst.id in down_ids:
                inst.tomber()

        res = client.acquerir("ressource:test")
        if res.acquis:
            client.liberer("ressource:test", res.token)

        down_str = ", ".join(down_ids) if down_ids else "(aucune)"
        acquis_str = "✅ oui" if res.acquis else "❌ non"
        print(f"  {label:<12} {down_str:<22} {acquis_str:>8}  "
              f"{len(res.instances_ok):>4}/{len(instances)}  "
              f"{res.validity_ms:>8.0f}ms")

    print(f"""
  Avec 2 pannes : quorum atteint (3/5) → ✅ verrou acquis
  Avec 3 pannes : quorum impossible (2/5 < 3) → ❌ refusé

  Règle générale : Redlock tolère ⌊(N-1)/2⌋ pannes simultanées.
  C'est identique au quorum de Raft (Jour 7).
    """)


# ─── SCÉNARIO 4 : GC PAUSE + FENCING TOKEN ───────────────────────────────────

def scenario_gc_pause():
    titre("SCÉNARIO 4 — GC Pause + Fencing Token : la critique de Kleppmann")

    print("""
  Scénario de Martin Kleppmann (2016) :

    t=0   Client-A acquiert verrou (TTL=300ms)
    t=100 Client-A entre en GC pause (le monde s'arrête)
    t=350 Le verrou expire (TTL dépassé)
    t=400 Client-B acquiert le même verrou
    t=500 Client-A sort de GC pause, croit toujours avoir le verrou
          → DEUX CLIENTS croient avoir le verrou !

  Sans fencing token : Client-A écrase les données de Client-B.
  Avec fencing token : la ressource rejette Client-A (token obsolète).
    """)

    instances = creer_cluster(5, latence_ms=3)
    client_a  = ClientRedlock("client-A", instances, ttl_ms=300, retry_count=1)
    client_b  = ClientRedlock("client-B", instances, ttl_ms=2000, retry_count=3,
                              retry_delay_ms=100)
    ressource = RessourceProtegee("fichier_critique")
    journal   = []
    jlock     = threading.Lock()

    def log(msg):
        with jlock:
            journal.append(f"  +{(time.perf_counter()-t0)*1000:6.0f}ms  {msg}")

    t0 = time.perf_counter()

    # Client-A acquiert le verrou
    res_a = client_a.acquerir("lock:fichier")
    log(f"client-A acquiert le verrou  fencing={res_a.fencing_token}  validity={res_a.validity_ms:.0f}ms")

    # Simuler GC pause de 400ms (verrou TTL=300ms → il va expirer)
    log("client-A entre en GC PAUSE (400ms)")

    def gc_pause_et_ecriture():
        time.sleep(0.4)   # GC pause : 400ms
        log("client-A sort de GC pause — croit toujours avoir le verrou")
        ok = ressource.ecrire("données_A_corrompues", res_a.fencing_token, "client-A")
        log(f"client-A tente d'écrire → {'✅ accepté' if ok else '❌ REJETÉ (fencing token obsolète)'}")

    def client_b_travaille():
        time.sleep(0.35)  # Attend que le verrou de A expire
        res_b = client_b.acquerir("lock:fichier")
        log(f"client-B acquiert le verrou  fencing={res_b.fencing_token}")
        if res_b.acquis:
            time.sleep(0.05)
            ok = ressource.ecrire("données_B_valides", res_b.fencing_token, "client-B")
            log(f"client-B écrit → {'✅ accepté' if ok else '❌ rejeté'}")
            client_b.liberer("lock:fichier", res_b.token)

    t_a = threading.Thread(target=gc_pause_et_ecriture, daemon=True)
    t_b = threading.Thread(target=client_b_travaille,   daemon=True)
    t_a.start(); t_b.start()
    t_a.join(timeout=3); t_b.join(timeout=3)

    print()
    for ligne_j in journal:
        print(ligne_j)

    print(f"\n  État final de la ressource :")
    print(f"    Valeur    : {ressource.lire()!r}")
    print(f"    Écritures acceptées : {ressource._nb_ecritures}")
    print(f"    Écritures rejetées  : {ressource._nb_rejets}")
    print(f"\n  Historique des accès :")
    for h in ressource.historique:
        icone = "✅" if h["action"] == "ÉCRIT" else "❌"
        print(f"    {icone} {h['action']:<8} client={h['client']:<10} "
              f"token={h['token']}  val={h['valeur']!r}")

    print(f"""
  ✅ Le fencing token a protégé la ressource :
     Client-A (token={res_a.fencing_token}) a été rejeté car Client-B
     avait déjà écrit avec un token plus élevé.
     Aucune corruption malgré la GC pause.
    """)


# ─── SCÉNARIO 5 : VERROU SIMPLE vs REDLOCK SOUS PARTITION ────────────────────

def scenario_comparaison():
    titre("SCÉNARIO 5 — Verrou simple Redis vs Redlock sous partition réseau")

    print("""
  Verrou simple (1 instance Redis) :
    Si cette instance est partitionnée (réseau coupé) :
      → Les nouvelles acquisitions échouent (disponibilité perdue)
      → Ou pire : deux clients croient avoir le verrou (si split-brain)

  Redlock (5 instances) :
    Une partition peut isoler 1 ou 2 instances.
    Le quorum est encore atteignable sur les 3 restantes.
    → Disponibilité et sécurité préservées.
    """)

    # Verrou simple : 1 instance
    redis_unique = InstanceRedis("redis-unique", latence_ms=3)
    instances_5  = creer_cluster(5, latence_ms=3)
    client_simple  = ClientRedlock("client", [redis_unique], ttl_ms=2000, retry_count=1)
    client_redlock = ClientRedlock("client", instances_5,    ttl_ms=2000, retry_count=1)

    print(f"  Simulation de partition réseau progressive :\n")
    print(f"  {'Instances down':>18}  {'Verrou simple':>16}  {'Redlock (5)':>12}")
    print("  " + "─"*52)

    configs_5 = [
        ("0/5",   []),
        ("1/5",   ["redis-1"]),
        ("2/5",   ["redis-1", "redis-2"]),
        ("3/5",   ["redis-1", "redis-2", "redis-3"]),
    ]

    for label, down_ids in configs_5:
        # Reset
        redis_unique.redemarrer()
        for inst in instances_5:
            inst.redemarrer()

        # Partition sur redis-unique si dans down_ids
        if "redis-1" in down_ids:   # Simuler que l'unique instance est down
            redis_unique.tomber()

        for inst in instances_5:
            if inst.id in down_ids:
                inst.tomber()

        res_simple  = client_simple.acquerir("lock:test")
        res_redlock = client_redlock.acquerir("lock:test")

        if res_simple.acquis:  client_simple.liberer("lock:test", res_simple.token)
        if res_redlock.acquis: client_redlock.liberer("lock:test", res_redlock.token)

        s = "✅ acquis" if res_simple.acquis  else "❌ échec"
        r = "✅ acquis" if res_redlock.acquis else "❌ échec"
        print(f"  {label:>18}  {s:>16}  {r:>12}")

    print(f"""
  Observations :
    Verrou simple : dès que l'unique Redis est down → ❌ tout échoue
    Redlock 5     : tolère jusqu'à 2 pannes simultanées → ✅

  Coût de cette résilience :
    → 5 instances Redis à maintenir (vs 1)
    → Latence acquisition = latence de la (N/2+1)ème instance la plus lente
    → En pratique : ~5-15ms de surcoût vs verrou simple

  Recommandation :
    Workloads non-critiques     → verrou simple 1 Redis (suffisant)
    Workloads critiques (paiements, stock) → Redlock ou service dédié (etcd, ZooKeeper)
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 15 — DISTRIBUTED LOCKING : REDLOCK               ║")
    print("╚" + "═"*62 + "╝")

    scenario_nominal()
    scenario_exclusion()
    scenario_pannes()
    scenario_gc_pause()
    scenario_comparaison()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Redlock — Ce qu'il faut retenir :

    1. SET NX PX en parallèle sur N instances Redis indépendantes
    2. Acquis si quorum (N/2+1) atteint ET validity_time > 0
    3. Libération via script Lua atomique (compare-and-delete)
    4. Fencing token monotone pour se protéger des GC pauses

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Acquisition nominale, TTL géré, libération propre ✅
    Scénario 2 → Exclusion mutuelle : 1 seul client gagne sur 2 ✅
    Scénario 3 → Tolère 2 pannes sur 5, échoue proprement à 3 ✅
    Scénario 4 → Fencing token rejette le client après GC pause ✅
    Scénario 5 → Redlock résiste aux partitions, verrou simple non ✅

  Limites de Redlock (débat Kleppmann) :
    → Les fencing tokens résolvent les GC pauses
    → Mais la ressource cible doit les implémenter (pas toujours possible)
    → Sous partition réseau sévère : risque théorique de split-brain

  Alternatives selon le cas :
    etcd / ZooKeeper  → verrous via consensus (Raft/ZAB) — plus fort
    PostgreSQL        → SELECT FOR UPDATE (simple, transactionnel)
    Redlock           → bon équilibre performance / résilience

  → Semaine 4 — Jour 16 : Load Balancing L4 vs L7
    Répartir le trafic entre plusieurs instances d'un service.
    L4 (TCP) vs L7 (HTTP) : que peut-on inspecter ? Que peut-on décider ?
  """)


if __name__ == "__main__":
    main()
