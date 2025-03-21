"""
Jour 21 — Simulation : Circuit Breaker en action
==================================================
5 scénarios :
  1. États du circuit : CLOSED → OPEN → HALF_OPEN → CLOSED
  2. Cascade de timeouts sans vs avec circuit breaker
  3. Fallback : répondre depuis le cache quand le circuit est ouvert
  4. Fenêtre glissante : COUNT-BASED vs TIME-BASED
  5. Multi-services : chaque appel sortant a son propre circuit
"""

import time
import threading
import random
import statistics
from collections import defaultdict
from circuit_breaker import (
    CircuitBreaker, CircuitBreakerAvecFallback,
    EtatCircuit, ResultatAppel
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def ts_rel(t0): return f"+{(time.perf_counter()-t0)*1000:6.0f}ms"


# ─── SERVICES SIMULÉS ────────────────────────────────────────────────────────

def service_nominal(latence_ms: float = 50) -> str:
    time.sleep(latence_ms / 1000)
    return f"OK ({latence_ms:.0f}ms)"

def service_en_panne() -> str:
    time.sleep(0.01)   # Rapide mais toujours en erreur
    raise RuntimeError("Service unavailable")

def service_lent(latence_ms: float = 3000) -> str:
    time.sleep(latence_ms / 1000)
    return "OK (mais très lent)"


# ─── SCÉNARIO 1 : CYCLE D'ÉTATS ──────────────────────────────────────────────

def scenario_etats():
    titre("SCÉNARIO 1 — Cycle d'états : CLOSED → OPEN → HALF_OPEN → CLOSED")

    print("""
  On simule la vie d'un circuit breaker :
    Phase 1 : service nominal (CLOSED)
    Phase 2 : service en panne → circuit s'ouvre (OPEN)
    Phase 3 : timeout écoulé → sondes (HALF_OPEN)
    Phase 4 : service rétabli → circuit se referme (CLOSED)
    """)

    cb = CircuitBreaker(
        nom              = "payment-svc",
        seuil_echec_pct  = 50.0,
        taille_fenetre   = 6,
        min_requetes     = 4,
        timeout_ouvert_s = 1.5,
        nb_sondes        = 2,
        timeout_requete_s= 0.5,
    )

    t0 = time.perf_counter()
    journal: list[tuple[str, str, str]] = []

    def req(fn, label=""):
        _, ra = cb.appeler(fn)
        etat  = cb.etat().value
        icone = {"succes":"✅", "echec":"❌", "timeout":"⏱️",
                 "court_circuité":"🚫"}.get(ra.value, "?")
        ligne_j = (ts_rel(t0), icone + " " + ra.value, etat)
        journal.append(ligne_j)
        return ra

    print(f"  Phase 1 — Service nominal (4 requêtes) :")
    for _ in range(4):
        req(lambda: service_nominal(40))
    cb.afficher_etat()

    print(f"\n  Phase 2 — Service en panne (6 requêtes) :")
    for _ in range(6):
        req(service_en_panne)
    cb.afficher_etat()

    print(f"\n  Phase 3 — Circuit OPEN : requêtes bloquées pendant {cb.timeout_ouvert_s}s :")
    for _ in range(4):
        req(service_en_panne)   # Service toujours en panne mais fail-fast
    cb.afficher_etat()

    print(f"\n  Attente timeout_ouvert ({cb.timeout_ouvert_s}s) → passage HALF_OPEN ...")
    time.sleep(cb.timeout_ouvert_s + 0.1)

    print(f"\n  Phase 4 — HALF_OPEN : sondes avec service rétabli :")
    for _ in range(cb.nb_sondes + 1):
        req(lambda: service_nominal(30))
    cb.afficher_etat()

    # Afficher le journal
    print(f"\n  Journal complet :")
    print(f"  {'Temps':>8}  {'Résultat':<20}  État circuit")
    print("  " + "─"*45)
    for ts_, res, etat in journal:
        print(f"  {ts_:>8}  {res:<20}  {etat}")

    print(f"\n  Transitions d'état :")
    cb.afficher_transitions()


# ─── SCÉNARIO 2 : CASCADE DE TIMEOUTS ────────────────────────────────────────

def scenario_cascade():
    titre("SCÉNARIO 2 — Cascade de timeouts : sans vs avec circuit breaker")

    print("""
  Scénario : payment-svc met 2s à répondre (timeout DB).
  50 requêtes arrivent simultanément.

  Sans CB : chaque requête attend 2s → 50 threads bloqués → ~2s de lat totale
  Avec CB : après 5 échecs, circuit OPEN → fail-fast → ~0ms pour les suivantes
    """)

    TIMEOUT_SERVICE = 0.5   # 500ms pour la démo (simule 2s réel)
    N = 30

    # ── Sans circuit breaker ──────────────────────────────────────────────────
    latences_sans = []
    lock = threading.Lock()

    def appel_sans_cb(idx):
        t0_ = time.perf_counter()
        try:
            service_lent(TIMEOUT_SERVICE * 1000)
        except:
            pass
        with lock:
            latences_sans.append((time.perf_counter() - t0_) * 1000)

    t0 = time.perf_counter()
    threads = [threading.Thread(target=appel_sans_cb, args=(i,)) for i in range(N)]
    for th in threads: th.start()
    for th in threads: th.join()
    duree_sans = (time.perf_counter() - t0) * 1000

    # ── Avec circuit breaker ──────────────────────────────────────────────────
    cb = CircuitBreaker(
        nom              = "payment-svc",
        seuil_echec_pct  = 50.0,
        taille_fenetre   = 6,
        min_requetes     = 4,
        timeout_ouvert_s = 10.0,
        nb_sondes        = 2,
        timeout_requete_s= TIMEOUT_SERVICE,
    )

    latences_avec = []
    resultats_avec = defaultdict(int)
    lock2 = threading.Lock()

    def appel_avec_cb(idx):
        t0_ = time.perf_counter()
        _, ra = cb.appeler(service_lent, TIMEOUT_SERVICE * 1000)
        lat = (time.perf_counter() - t0_) * 1000
        with lock2:
            latences_avec.append(lat)
            resultats_avec[ra.value] += 1

    t0 = time.perf_counter()
    threads = [threading.Thread(target=appel_avec_cb, args=(i,)) for i in range(N)]
    for th in threads: th.start()
    for th in threads: th.join()
    duree_avec = (time.perf_counter() - t0) * 1000

    # ── Résultats ─────────────────────────────────────────────────────────────
    p50_sans = statistics.median(latences_sans)
    p50_avec = statistics.median(latences_avec)
    p99_sans = sorted(latences_sans)[int(len(latences_sans)*0.99)]
    p99_avec = sorted(latences_avec)[int(len(latences_avec)*0.99)]

    print(f"  {N} requêtes vers un service qui timeout à {TIMEOUT_SERVICE*1000:.0f}ms :\n")
    print(f"  {'Métrique':<28} {'Sans CB':>12}  {'Avec CB':>12}")
    print("  " + "─"*55)
    print(f"  {'Durée totale (parallèle)':<28} {duree_sans:>10.0f}ms  {duree_avec:>10.0f}ms")
    print(f"  {'Lat médiane par requête':<28} {p50_sans:>10.0f}ms  {p50_avec:>10.0f}ms")
    print(f"  {'Lat P99 par requête':<28} {p99_sans:>10.0f}ms  {p99_avec:>10.0f}ms")
    print(f"  {'Requêtes bloquées (timeout)':<28} {N:>10}    {resultats_avec.get('timeout', 0):>10}")
    print(f"  {'Requêtes fail-fast (CB)':<28} {'0':>10}    {resultats_avec.get('court_circuité', 0):>10}")

    amelioration = p50_sans / max(p50_avec, 0.1)
    print(f"\n  Amélioration de la latence médiane : {amelioration:.0f}x ✅")
    print(f"  → Le circuit breaker protège les ressources (threads, connexions)")
    cb.afficher_etat()
    print(f"\n  Transitions :")
    cb.afficher_transitions()


# ─── SCÉNARIO 3 : FALLBACK ────────────────────────────────────────────────────

def scenario_fallback():
    titre("SCÉNARIO 3 — Fallback : répondre depuis le cache quand le circuit est ouvert")

    print("""
  Le circuit breaker seul retourne une erreur quand le circuit est ouvert.
  Avec un fallback, on peut retourner une valeur dégradée mais utile :
    → Cache local de la dernière réponse connue
    → Valeur par défaut statique
    → Réponse d'un service secondaire (replica, CDN…)

  Ex : recommendation-svc est en panne → retourner les top recommandations en cache
  Ex : pricing-svc est en panne         → retourner le dernier prix connu
    """)

    # Simuler un cache de recommandations
    cache_recommandations = {
        "user_1": ["produit-A", "produit-B", "produit-C"],
        "user_2": ["produit-X", "produit-Y"],
    }

    nb_appels_cache = [0]

    def fallback_recommendations(user_id: str) -> list:
        nb_appels_cache[0] += 1
        return cache_recommandations.get(user_id, ["produit-generique"])

    cb = CircuitBreaker(
        nom="recommendation-svc",
        seuil_echec_pct=50.0,
        taille_fenetre=6,
        min_requetes=3,
        timeout_ouvert_s=2.0,
        nb_sondes=2,
        timeout_requete_s=0.2,
    )
    cbf = CircuitBreakerAvecFallback(cb, fallback=fallback_recommendations)

    def recommander(user_id: str) -> list:
        return cbf.appeler(
            lambda uid: (time.sleep(0.02) or [f"frais-{uid}-A", f"frais-{uid}-B"]),
            user_id
        )

    print(f"  Phase 1 — Service fonctionnel :")
    for uid in ["user_1", "user_2", "user_1"]:
        res = recommander(uid)
        print(f"    {uid}: {res}  (service réel)")

    print(f"\n  Phase 2 — Service en panne → circuit s'ouvre :")
    cb_broken = CircuitBreaker(
        nom="recommendation-svc",
        seuil_echec_pct=50.0,
        taille_fenetre=6,
        min_requetes=3,
        timeout_ouvert_s=2.0,
        nb_sondes=2,
        timeout_requete_s=0.1,
    )
    cbf2 = CircuitBreakerAvecFallback(cb_broken, fallback=fallback_recommendations)

    def recommander_broken(user_id: str) -> list:
        return cbf2.appeler(service_en_panne, user_id)

    for uid in ["user_1", "user_2", "user_1", "user_1", "user_2", "user_1"]:
        res = recommander_broken(uid)
        etat = cb_broken.etat().value
        source = "🗄️ CACHE" if res in cache_recommandations.values() else "❌ erreur"
        print(f"    {uid}: {res}   état={etat}  {source}")

    print(f"\n  Appels au cache (fallback) : {nb_appels_cache[0]}")
    print(f"  → L'utilisateur reçoit des recommandations même quand le service est down ✅")
    cb_broken.afficher_transitions()

    print(f"""
  Stratégies de fallback en production :
    Cache en mémoire  : dernière réponse connue (staleness acceptée)
    Redis             : cache partagé entre instances
    Service secondaire: replica read-only, CDN
    Valeur neutre     : liste vide, 0, "", false (dépend du domaine)
    Rejet explicite   : 503 avec Retry-After header (pas de fallback possible)
    """)


# ─── SCÉNARIO 4 : FENÊTRE GLISSANTE ──────────────────────────────────────────

def scenario_fenetre():
    titre("SCÉNARIO 4 — Fenêtre glissante COUNT-BASED : sensibilité au bruit")

    print("""
  Deux configurations de fenêtre glissante pour le même trafic :
    Petite fenêtre (N=4)  : réactive mais sensible aux pics transitoires
    Grande fenêtre (N=20) : stable mais lente à détecter les pannes prolongées

  On simule une panne de 10s puis un rétablissement.
    """)

    configs = [
        ("Petite fenêtre (N=4)",  4,  3, 1.0),
        ("Grande fenêtre (N=20)", 20, 8, 1.0),
    ]

    for label, fenetre, min_req, timeout_s in configs:
        cb = CircuitBreaker(
            nom              = label,
            seuil_echec_pct  = 50.0,
            taille_fenetre   = fenetre,
            min_requetes     = min_req,
            timeout_ouvert_s = timeout_s,
            nb_sondes        = 2,
            timeout_requete_s= 0.05,
        )

        etats_par_requete = []

        # Phase 1 : nominal (10 req)
        for _ in range(10):
            cb.appeler(lambda: service_nominal(5))
            etats_par_requete.append(cb.etat().value[0])   # C/O/H

        # Phase 2 : panne (15 req)
        for _ in range(15):
            cb.appeler(service_en_panne)
            etats_par_requete.append(cb.etat().value[0])

        # Phase 3 : rétablissement (après timeout, 10 req)
        time.sleep(timeout_s + 0.1)
        for _ in range(10):
            cb.appeler(lambda: service_nominal(5))
            etats_par_requete.append(cb.etat().value[0])

        # Afficher la timeline
        timeline = "".join(etats_par_requete)
        # Rendre lisible
        timeline_vis = timeline.replace("C","🟢").replace("O","🔴").replace("H","🟡")
        nb_open = timeline.count("O")
        nb_cc   = cb.stats["court_circuits"]
        print(f"\n  {label} :")
        print(f"    Timeline : {timeline}")
        print(f"    Requêtes en OPEN (fail-fast) : {nb_open}/35")
        print(f"    Court-circuits               : {nb_cc}")
        print(f"    Transitions :")
        for t in cb.transitions:
            print(f"      {t.etat_avant.value:10} → {t.etat_apres.value:10}  ({t.raison})")

    print(f"""
  Observations :
    Petite fenêtre : s'ouvre plus vite (moins de données avant de décider)
                     mais peut aussi se fermer trop vite (faux positifs)
    Grande fenêtre : plus de stabilité, moins de bruit
                     mais plus lente à réagir à une panne franche

  En production : fenêtre de 20-50 requêtes avec seuil à 50-60%
  + minimum de requêtes (ex: 20) pour éviter l'ouverture sur 1 échec
    """)


# ─── SCÉNARIO 5 : MULTI-SERVICES ─────────────────────────────────────────────

def scenario_multi_services():
    titre("SCÉNARIO 5 — Multi-services : chaque dépendance a son propre circuit")

    print("""
  En production, un service appelle N dépendances.
  Chacune a son propre circuit breaker indépendant.
  Une dépendance en panne ne bloque pas les autres.

  API Gateway appelle :
    → inventory-svc  (CB1)
    → payment-svc    (CB2)  ← en panne
    → shipping-svc   (CB3)
    → analytics-svc  (CB4)  ← non-critique, ignoré si fail
    """)

    cbs = {
        "inventory-svc":  CircuitBreaker("inventory-svc",  seuil_echec_pct=50, taille_fenetre=8, min_requetes=4, timeout_ouvert_s=2.0, nb_sondes=2, timeout_requete_s=0.3),
        "payment-svc":    CircuitBreaker("payment-svc",    seuil_echec_pct=50, taille_fenetre=8, min_requetes=4, timeout_ouvert_s=2.0, nb_sondes=2, timeout_requete_s=0.3),
        "shipping-svc":   CircuitBreaker("shipping-svc",   seuil_echec_pct=50, taille_fenetre=8, min_requetes=4, timeout_ouvert_s=2.0, nb_sondes=2, timeout_requete_s=0.3),
        "analytics-svc":  CircuitBreaker("analytics-svc",  seuil_echec_pct=50, taille_fenetre=8, min_requetes=4, timeout_ouvert_s=2.0, nb_sondes=2, timeout_requete_s=0.3),
    }

    # Services : payment toujours en panne
    services = {
        "inventory-svc":  lambda: service_nominal(30),
        "payment-svc":    service_en_panne,         # EN PANNE
        "shipping-svc":   lambda: service_nominal(20),
        "analytics-svc":  lambda: service_nominal(15),
    }

    N = 25
    resultats_globaux = defaultdict(lambda: defaultdict(int))

    for req_i in range(N):
        for svc, fn in services.items():
            _, ra = cbs[svc].appeler(fn)
            resultats_globaux[svc][ra.value] += 1

    print(f"\n  Résultats après {N} requêtes vers chaque service :\n")
    print(f"  {'Service':<18} {'État':>10}  {'✅':>5}  {'❌':>5}  {'🚫 CB':>7}  {'Taux échec':>10}")
    print("  " + "─"*62)

    for svc, cb in sorted(cbs.items()):
        res = resultats_globaux[svc]
        etat = cb.etat().value
        icone_etat = {"CLOSED":"🟢","OPEN":"🔴","HALF_OPEN":"🟡"}.get(etat,"?")
        print(f"  {svc:<18} {icone_etat} {etat:<9}  {res.get('succes',0):>5}  "
              f"{res.get('echec',0):>5}  {res.get('court_circuité',0):>7}  "
              f"{cb.taux_echec():>8.0f}%")

    print(f"\n  Isolation des pannes :")
    print(f"  → payment-svc ❌ mais inventory, shipping, analytics TOUJOURS disponibles")
    print(f"  → Checkout partiel possible : réserver le stock, réessayer le paiement plus tard")
    print(f"  → Sans CB : payment timeout 300ms × 25 req = 7.5s perdues pour les autres")

    print(f"""
  Pattern Bulkhead (cloison de navire) :
    Chaque dépendance a son propre pool de threads + circuit breaker.
    Si payment-svc sature son pool (30 threads), il ne prend pas les threads
    des autres services. Comme les cloisons étanches d'un navire.

  Hystrix (Netflix, déprécié) vs Resilience4j (Java, actuel) :
    Hystrix     → thread pool isolation + CB, mais déprécié
    Resilience4j→ fonctionnel (sans threads), léger, configurable
    Envoy/Istio → CB au niveau réseau (service mesh), language-agnostic
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)

    print("╔" + "═"*62 + "╗")
    print("║   JOUR 21 — CIRCUIT BREAKER (HYSTRIX / RESILIENCE4J)    ║")
    print("╚" + "═"*62 + "╝")

    scenario_etats()
    scenario_cascade()
    scenario_fallback()
    scenario_fenetre()
    scenario_multi_services()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Circuit Breaker — Ce qu'il faut retenir :

    CLOSED    : normal, requêtes passent, on compte les échecs
    OPEN      : protection, fail-fast immédiat (0ms), sauvegarde les threads
    HALF_OPEN : test, quelques sondes pour vérifier le rétablissement

    Fenêtre   : les N dernières requêtes (COUNT) ou la dernière T secondes
    Seuil     : % d'échecs pour ouvrir (ex: 50% sur 10 requêtes)
    Timeout   : durée en OPEN avant de tester (ex: 5s)
    Fallback  : réponse dégradée quand le circuit est ouvert (cache, défaut)

  Ce que nos scénarios ont prouvé :
    Scénario 1 → CLOSED→OPEN→HALF_OPEN→CLOSED avec timeline précise ✅
    Scénario 2 → Cascade évitée : lat médiane /10 avec circuit breaker ✅
    Scénario 3 → Fallback cache : recommandations même service down ✅
    Scénario 4 → Petite fenêtre réactive vs grande fenêtre stable ✅
    Scénario 5 → Isolation : payment down, les autres inchangés ✅

  Utilisé en production :
    Resilience4j → Java, léger, annotations Spring
    Polly        → .NET, retry + CB composables
    Envoy/Istio  → Circuit breaker au niveau réseau (service mesh)
    Hystrix      → Netflix, déprécié mais très répandu en legacy

  Lien avec les autres jours :
    Jour 20 (Saga) → le CB protège les étapes de la saga
    Jour 16 (LB)   → le LB peut retirer un backend dont le CB est OPEN
    Jour 17 (Disc) → le CB peut déclencher un désenregistrement du service

  → Jour 22 : Rate Limiting & Throttling
    Côté client : le CB protège contre les services lents.
    Côté serveur : le Rate Limiter protège contre les clients trop voraces.
    Token Bucket, Leaky Bucket, Fixed Window, Sliding Window.
  """)

if __name__ == "__main__":
    main()
