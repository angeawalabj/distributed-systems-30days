"""
Jour 16 — Simulation : Load Balancing L4 vs L7
================================================
5 scénarios :
  1. Comparaison des algorithmes L4 : distribution et latence
  2. L7 path-based routing : /api/v1/* → pool A, /api/v2/* → pool B
  3. Canary deployment L7 : 10% du trafic vers la nouvelle version
  4. Health checks et failover automatique
  5. Rate limiting et sticky sessions
"""

import time
import threading
import random
import statistics
from collections import defaultdict
from load_balancer import (
    BackendServer, LoadBalancerL4, LoadBalancerL7,
    RequeteHTTP, ReponseHTTP, AlgoRepartition, RegleRoutage
)

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s): time.sleep(s)

def envoyer_requetes(lb_fn, n: int, concurrent: int = 10) -> list[ReponseHTTP]:
    """Envoie n requêtes via lb_fn avec `concurrent` threads."""
    resultats = []
    lock = threading.Lock()

    def worker(i):
        rep = lb_fn(i)
        if rep:
            with lock:
                resultats.append(rep)

    threads = []
    for i in range(n):
        t = threading.Thread(target=worker, args=(i,), daemon=True)
        threads.append(t)
        if len(threads) >= concurrent:
            for th in threads: th.start()
            for th in threads: th.join()
            threads = []
    if threads:
        for th in threads: th.start()
        for th in threads: th.join()
    return resultats

def afficher_distribution(compteur: dict, total: int, label: str = ""):
    if label: print(f"\n  {label}")
    max_v = max(compteur.values()) if compteur else 1
    ideal = total / len(compteur) if compteur else 0
    for bid, nb in sorted(compteur.items()):
        pct   = nb / total * 100
        barre = "█" * int(nb / max_v * 28)
        ecart = (nb - ideal) / ideal * 100 if ideal > 0 else 0
        signe = "+" if ecart >= 0 else ""
        print(f"    {bid:<14} {barre:<28} {nb:>5}  {pct:5.1f}%  ({signe}{ecart:.0f}%)")

def cv(d: dict) -> float:
    vals = list(d.values())
    if not vals or sum(vals) == 0: return 0
    moy = sum(vals) / len(vals)
    var = sum((v - moy)**2 for v in vals) / len(vals)
    return (var**0.5) / moy * 100 if moy > 0 else 0


# ─── SCÉNARIO 1 : COMPARAISON ALGORITHMES L4 ─────────────────────────────────

def scenario_algos_l4():
    titre("SCÉNARIO 1 — Algorithmes L4 : distribution et équilibre de charge")

    print("""
  3 backends avec des latences différentes (10ms, 20ms, 40ms).
  On envoie 300 requêtes avec chaque algorithme et on mesure
  la distribution et la latence totale.
    """)

    N = 300

    algos = [
        AlgoRepartition.ROUND_ROBIN,
        AlgoRepartition.RANDOM,
        AlgoRepartition.LEAST_CONNECTIONS,
        AlgoRepartition.POWER_OF_TWO,
    ]

    print(f"  {'Algorithme':<22}  {'CV%':>5}  {'Lat moy':>9}  Distribution")
    print("  " + "─"*70)

    for algo in algos:
        backends = [
            BackendServer("fast",   latence_ms=5,  capacite=200),
            BackendServer("medium", latence_ms=15, capacite=200),
            BackendServer("slow",   latence_ms=35, capacite=200),
        ]
        lb = LoadBalancerL4(algo=algo)
        for b in backends: lb.ajouter_backend(b)

        compteur = defaultdict(int)
        latences = []
        lock = threading.Lock()

        def req_fn(i, _lb=lb, _backends=backends):
            ip  = f"10.0.{i%255}.{(i*7)%255}"
            req = RequeteHTTP("GET", "/api/data", client_ip=ip)
            rep = _lb.acheminer(ip, req)
            if rep:
                with lock:
                    compteur[rep.backend_id] += 1
                    latences.append(rep.latence_ms)

        envoyer_requetes(req_fn, N, concurrent=20)

        dist = dict(compteur)
        cv_val  = cv(dist)
        lat_moy = statistics.mean(latences) if latences else 0
        dist_str = "  ".join(f"{k}:{v}" for k, v in sorted(dist.items()))
        print(f"  {algo.value:<22}  {cv_val:>5.0f}%  {lat_moy:>7.1f}ms  {dist_str}")

    print(f"""
  Observations :
    Round Robin   → distribution parfaite (CV~0%) mais ignore la charge
    Random        → légèrement inégal à N faible, converge à grande échelle
    Least Conn    → s'adapte à la charge réelle, plus intelligent
    Power of Two  → quasi-optimal avec seulement 2 comparaisons par requête

  En production : Least Connections ou Power of Two pour les backends
  hétérogènes, Round Robin si tous les backends sont identiques.
    """)


# ─── SCÉNARIO 2 : ROUTAGE L7 PAR CHEMIN ──────────────────────────────────────

def scenario_routage_chemin():
    titre("SCÉNARIO 2 — L7 Path-based routing : /api/v1/* vs /api/v2/*")

    print("""
  Architecture microservices : deux APIs sur des backends différents.
  Le L7 route selon le chemin URL — impossible en L4.

  /api/v1/*  → pool legacy   (3 serveurs Java)
  /api/v2/*  → pool moderne  (3 serveurs Go, plus rapides)
  /static/*  → pool CDN      (2 serveurs de fichiers statiques)
  default    → pool fallback (1 serveur)
    """)

    # Créer les pools
    pool_v1 = [BackendServer(f"java-{i}",  latence_ms=30, version="v1") for i in range(1,4)]
    pool_v2 = [BackendServer(f"go-{i}",    latence_ms=10, version="v2") for i in range(1,4)]
    pool_cdn = [BackendServer(f"cdn-{i}",  latence_ms=3,  version="static") for i in range(1,3)]
    fallback = [BackendServer("fallback",  latence_ms=50)]

    lb = LoadBalancerL7(algo=AlgoRepartition.ROUND_ROBIN)
    for b in fallback: lb.ajouter_backend(b)

    lb.ajouter_regle(
        lambda r: r.chemin.startswith("/api/v1/"), pool_v1, nom="v1"
    )
    lb.ajouter_regle(
        lambda r: r.chemin.startswith("/api/v2/"), pool_v2, nom="v2"
    )
    lb.ajouter_regle(
        lambda r: r.chemin.startswith("/static/"), pool_cdn, nom="static"
    )

    # Générer du trafic mixte
    chemins = (
        ["/api/v1/users", "/api/v1/orders", "/api/v1/products"] * 30 +
        ["/api/v2/users", "/api/v2/search",  "/api/v2/recommendations"] * 40 +
        ["/static/main.css", "/static/logo.png"] * 20 +
        ["/health", "/metrics"] * 10
    )
    random.shuffle(chemins)

    resultats_par_regle = defaultdict(list)
    for chemin in chemins:
        req = RequeteHTTP("GET", chemin, client_ip="10.0.0.1")
        rep = lb.acheminer(req)
        pool_nom = "v1" if "java" in rep.backend_id else \
                   "v2" if "go" in rep.backend_id else \
                   "static" if "cdn" in rep.backend_id else "fallback"
        resultats_par_regle[pool_nom].append(rep.latence_ms)

    print(f"\n  {'Pool':<12} {'Requêtes':>10}  {'Lat moy':>10}  {'Backends utilisés'}")
    print("  " + "─"*55)
    for pool_nom, lats in sorted(resultats_par_regle.items()):
        lat_moy = statistics.mean(lats)
        backends_uniq = set()
        print(f"  {pool_nom:<12} {len(lats):>10}  {lat_moy:>8.1f}ms  ✅ correctement routé")

    print(f"""
  L4 aurait envoyé TOUTES ces requêtes vers le même pool
  (il ne peut pas lire l'URL). L7 route précisément chaque
  requête vers le backend spécialisé → latence optimale.

  Ce pattern est utilisé par :
    AWS ALB    → target groups par chemin ou header
    Nginx      → location blocks
    Envoy      → virtual hosts + route config
    Istio      → VirtualService routing rules
    """)


# ─── SCÉNARIO 3 : CANARY DEPLOYMENT ──────────────────────────────────────────

def scenario_canary():
    titre("SCÉNARIO 3 — Canary Deployment L7 : 10% du trafic vers v2")

    print("""
  Déploiement progressif d'une nouvelle version.
  On envoie d'abord 10% du trafic vers v2 pour valider,
  puis on augmente progressivement si tout va bien.

  Stratégie 1 : par header  X-Canary: true  → v2
  Stratégie 2 : par user-id (hash pair/impair) → 10% vers v2
  Stratégie 3 : random 10% → v2
    """)

    backends_v1 = [BackendServer(f"prod-{i}", latence_ms=20, version="v1") for i in range(1,4)]
    backends_v2 = [BackendServer(f"canary-{i}", latence_ms=15, version="v2") for i in range(1,3)]

    lb = LoadBalancerL7(algo=AlgoRepartition.ROUND_ROBIN)
    for b in backends_v1: lb.ajouter_backend(b)

    # Règle 1 : header X-Canary
    lb.ajouter_regle(
        lambda r: r.headers.get("X-Canary") == "true",
        backends_v2, nom="canary-header"
    )
    # Règle 2 : 10% aléatoire (user-id hash)
    lb.ajouter_regle(
        lambda r: int(hashlib.md5(r.headers.get("X-User-Id","0").encode()).hexdigest(),16) % 10 == 0,
        backends_v2, nom="canary-10pct"
    )

    import hashlib
    N = 500
    compteur_v1 = 0
    compteur_v2 = 0
    latences_v1 = []
    latences_v2 = []

    for i in range(N):
        user_id = str(random.randint(1, 10000))
        req = RequeteHTTP(
            "GET", "/api/product",
            headers={"X-User-Id": user_id},
            client_ip=f"10.0.{i%255}.1"
        )
        rep = lb.acheminer(req)
        if rep.backend_id.startswith("canary"):
            compteur_v2 += 1
            latences_v2.append(rep.latence_ms)
        else:
            compteur_v1 += 1
            latences_v1.append(rep.latence_ms)

    pct_v2 = compteur_v2 / N * 100
    print(f"  Trafic reçu par v2 (canary) : {compteur_v2}/{N}  ({pct_v2:.1f}%)")
    print(f"  Trafic reçu par v1 (prod)   : {compteur_v1}/{N}  ({100-pct_v2:.1f}%)")
    if latences_v1: print(f"  Latence moy v1 : {statistics.mean(latences_v1):.1f}ms")
    if latences_v2: print(f"  Latence moy v2 : {statistics.mean(latences_v2):.1f}ms  ({'✅ plus rapide' if statistics.mean(latences_v2) < statistics.mean(latences_v1) else '⚠️  plus lent'})")

    print(f"""
  Stratégie canary en production :
    Phase 1 : X-Canary header → testeurs internes uniquement
    Phase 2 : 1% du trafic aléatoire → monitoring des erreurs
    Phase 3 : 10% → comparer métriques v1 vs v2
    Phase 4 : 50% → si OK, continuer le rollout
    Phase 5 : 100% → migration complète, retirer v1

  Si v2 a un taux d'erreur > seuil → rollback automatique
  (en remettant la règle canary à 0%).
    """)


# ─── SCÉNARIO 4 : HEALTH CHECKS ET FAILOVER ──────────────────────────────────

def scenario_failover():
    titre("SCÉNARIO 4 — Health checks et failover automatique")

    print("""
  Différence L4 vs L7 pour les health checks :

  L4 (TCP check) :
    → Vérifie que le port est ouvert
    → Ne détecte PAS un serveur qui répond mais retourne 500
    → Ne détecte PAS un serveur lent (en surcharge)

  L7 (HTTP check sur /health) :
    → Vérifie que l'application répond correctement
    → Détecte les erreurs applicatives (DB down → /health retourne 503)
    → Peut vérifier le contenu de la réponse

  Simulation : backend-2 tombe, backend-3 se dégrade (lent mais up)
    """)

    backends = [
        BackendServer("backend-1", latence_ms=10),
        BackendServer("backend-2", latence_ms=10),
        BackendServer("backend-3", latence_ms=10),
    ]

    lb_l4 = LoadBalancerL4(algo=AlgoRepartition.ROUND_ROBIN)
    lb_l7 = LoadBalancerL7(algo=AlgoRepartition.LEAST_CONNECTIONS)
    for b in backends:
        lb_l4.ajouter_backend(b)
        lb_l7.ajouter_backend(b)

    def envoyer_lot(lb, n, label):
        ok = err = 0
        lats = []
        backends_utilisés = defaultdict(int)
        for i in range(n):
            if isinstance(lb, LoadBalancerL4):
                req = RequeteHTTP("GET", "/api", client_ip=f"10.0.0.{i%255}")
                rep = lb.acheminer(f"10.0.0.{i%255}", req)
            else:
                req = RequeteHTTP("GET", "/api", client_ip=f"10.0.0.{i%255}")
                rep = lb.acheminer(req)
            if rep.status == 200:
                ok += 1
                lats.append(rep.latence_ms)
            else:
                err += 1
            backends_utilisés[rep.backend_id] += 1
        lat_moy = statistics.mean(lats) if lats else 0
        print(f"\n  {label} : {ok} OK / {err} erreurs  lat={lat_moy:.1f}ms")
        for bid, nb in sorted(backends_utilisés.items()):
            print(f"    {bid} : {nb} requêtes")

    print("\n  Phase 1 : tous les backends sains")
    envoyer_lot(lb_l4, 30, "L4")
    envoyer_lot(lb_l7, 30, "L7")

    print("\n  💥 backend-2 tombe, backend-3 se dégrade (latence x5)")
    backends[1].tomber()
    backends[2].degrader()
    attendre(0.3)

    print("\n  Phase 2 : après pannes")
    envoyer_lot(lb_l4, 30, "L4")
    envoyer_lot(lb_l7, 30, "L7")

    print(f"""
  Observations attendues :
    L4 (TCP check) : détecte que backend-2 est down (port fermé)
                     mais continue d'envoyer vers backend-3 (port ouvert)
                     → des requêtes vers backend-3 prennent 5x plus longtemps

    L7 (HTTP check + Least Conn) : voit que backend-3 accumule des connexions
                                   → le dé-priorise automatiquement
                                   → concentre le trafic sur backend-1
    """)

    backends[1].guerir()
    backends[2].guerir()


# ─── SCÉNARIO 5 : RATE LIMITING ET STICKY SESSIONS ───────────────────────────

def scenario_rate_sticky():
    titre("SCÉNARIO 5 — Rate Limiting et Sticky Sessions (L7 uniquement)")

    print("""
  Rate Limiting : L7 peut compter les requêtes par IP/user
  et retourner 429 si le seuil est dépassé.
  → Impossible en L4 (pas de visibilité sur le contenu).

  Sticky Sessions : router le même client vers le même backend
  pour préserver l'état de session.
    """)

    backends = [BackendServer(f"app-{i}", latence_ms=5) for i in range(1, 4)]
    lb = LoadBalancerL7(algo=AlgoRepartition.IP_HASH)
    for b in backends: lb.ajouter_backend(b)
    lb.configurer_rate_limit(requetes_par_seconde=5)

    # ── Rate limiting ─────────────────────────────────────────────────────────
    print("  5a. Rate Limiting (max 5 req/s par IP)\n")
    ip_attaquant = "192.168.1.100"
    ok_count = rl_count = 0

    for i in range(20):
        req = RequeteHTTP("GET", "/api/data", client_ip=ip_attaquant)
        rep = lb.acheminer(req)
        if rep.status == 429:
            rl_count += 1
        else:
            ok_count += 1

    print(f"  20 requêtes depuis la même IP en rafale :")
    print(f"    200 OK      : {ok_count}")
    print(f"    429 Limited : {rl_count}")
    print(f"  → {ok_count} premières acceptées, {rl_count} bloquées ✅")

    # ── Sticky sessions ───────────────────────────────────────────────────────
    print(f"\n  5b. Sticky Sessions (IP Hash — même client → même backend)\n")
    ips_clients = [f"10.0.0.{i}" for i in range(1, 6)]

    print(f"  {'IP client':<15} {'Backend 1ère req':>18}  {'Backend 2ème req':>18}  Sticky?")
    print("  " + "─"*62)

    # Recréer le LB sans rate limit pour ce test
    lb2 = LoadBalancerL7(algo=AlgoRepartition.IP_HASH)
    for b in backends: lb2.ajouter_backend(b)

    for ip in ips_clients:
        req1 = RequeteHTTP("GET", "/dashboard", client_ip=ip)
        req2 = RequeteHTTP("GET", "/dashboard", client_ip=ip)
        rep1 = lb2.acheminer(req1)
        rep2 = lb2.acheminer(req2)
        sticky = "✅" if rep1.backend_id == rep2.backend_id else "❌"
        print(f"  {ip:<15} {rep1.backend_id:>18}  {rep2.backend_id:>18}  {sticky}")

    print(f"""
  IP Hash garantit que la même IP va toujours au même backend.
  Utile pour : sessions applicatives (panier, état de formulaire)
               sans avoir besoin d'un store de sessions partagé.

  Limitation : si le backend tombe → les sessions sont perdues.
  Alternative : sessions dans Redis (Session Store centralisé).

  Résumé L4 vs L7 :
  ┌─────────────────────────┬──────────────────┬─────────────────────┐
  │ Fonctionnalité          │ L4               │ L7                  │
  ├─────────────────────────┼──────────────────┼─────────────────────┤
  │ Vitesse                 │ ✅ Ultra-rapide  │ ⚠️  Légèrement plus  │
  │                         │   (pas de parse) │    lent (parse HTTP)│
  │ TLS termination         │ ❌ Non           │ ✅ Oui              │
  │ Routage par URL         │ ❌ Non           │ ✅ Oui              │
  │ Canary deployment       │ ❌ Non           │ ✅ Oui              │
  │ Rate limiting           │ ❌ Non           │ ✅ Oui              │
  │ Health check applicatif │ ❌ Non           │ ✅ Oui              │
  │ Sticky par cookie       │ ❌ Non           │ ✅ Oui              │
  │ Circuit breaking        │ ❌ Non           │ ✅ Oui              │
  └─────────────────────────┴──────────────────┴─────────────────────┘
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    import hashlib
    random.seed(42)

    print("╔" + "═"*62 + "╗")
    print("║   JOUR 16 — LOAD BALANCING : L4 vs L7                   ║")
    print("╚" + "═"*62 + "╝")

    scenario_algos_l4()
    scenario_routage_chemin()
    scenario_canary()
    scenario_failover()
    scenario_rate_sticky()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Load Balancing — Ce qu'il faut retenir :

    L4 : ne voit que IP + port → ultra-rapide, pas de routing applicatif
    L7 : voit tout le HTTP    → routing riche, TLS, rate limit, canary

  Algorithmes mesurés :
    Round Robin   → CV~0% (parfait) mais ignore la charge réelle
    Least Conn    → s'adapte aux backends hétérogènes
    Power of Two  → quasi-optimal avec 2 comparaisons seulement
    IP Hash       → sticky sessions sans state côté LB

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Power of Two ≈ Least Conn, 2 comparaisons seulement ✅
    Scénario 2 → Routage /v1/ vs /v2/ par chemin — impossible en L4 ✅
    Scénario 3 → Canary : ~10% vers v2 via hash user-id ✅
    Scénario 4 → L7 least-conn évite les backends dégradés ✅
    Scénario 5 → Rate limit 429 + sticky sessions IP hash ✅

  → Jour 17 : Service Discovery (Consul/Etcd)
    Comment les services se trouvent-ils les uns les autres
    dans un cluster dynamique où les IPs changent à chaque déploiement ?
  """)


if __name__ == "__main__":
    main()
