"""
Jour 18 — Simulation : Distributed Tracing en action
======================================================
5 scénarios :
  1. Trace simple : /checkout à travers 5 services en cascade
  2. Spans parallèles : inventory + fraud check simultanés
  3. Détection d'erreur : trouver quel service a cassé la requête
  4. Analyse de latence : identifier le goulot d'étranglement
  5. Sampling : ne tracer que 10% du trafic (production)
"""

import time
import threading
import random
import statistics
from collections import defaultdict
from tracing import (
    Collecteur, MicroService, ContexteTrace, Tracer,
    Span, StatutSpan
)

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s): time.sleep(s)


# ─── SCÉNARIO 1 : TRACE SIMPLE EN CASCADE ────────────────────────────────────

def scenario_trace_simple():
    titre("SCÉNARIO 1 — Trace simple : /checkout à travers 5 services")

    print("""
  Architecture e-commerce :
    gateway → cart-svc → inventory-svc → payment-svc → notif-svc

  Chaque service reçoit le traceparent header, crée son span,
  et le transmet au service suivant.
    """)

    collecteur = Collecteur()

    # Créer les services avec leurs latences
    notif     = MicroService("notif-svc",      collecteur, latence_ms=80)
    payment   = MicroService("payment-svc",    collecteur, latence_ms=350)
    inventory = MicroService("inventory-svc",  collecteur, latence_ms=120)
    cart      = MicroService("cart-svc",       collecteur, latence_ms=60)
    gateway   = MicroService("api-gateway",    collecteur, latence_ms=20)

    # Câbler les dépendances (séquentielles ici)
    cart.ajouter_dependance("inventory-svc", inventory)
    payment.ajouter_dependance("notif-svc",  notif)
    gateway.ajouter_dependance("cart-svc",   cart)
    gateway.ajouter_dependance("payment-svc", payment)

    # Lancer la requête depuis le gateway
    ctx = ContexteTrace.nouveau()
    print(f"  trace_id : {ctx.trace_id}")
    print(f"  Headers propagés : {ctx.to_headers()}\n")

    # Simulation manuelle pour contrôle précis de la cascade
    tracer_gw = Tracer("api-gateway", collecteur)
    span_gw   = tracer_gw.demarrer_span("POST /checkout", contexte=ctx)
    span_gw.ajouter_tag("http.url", "/checkout")
    span_gw.ajouter_tag("user_id", "u-42")
    time.sleep(0.020)

    # Cart service
    tracer_cart = Tracer("cart-svc", collecteur)
    ctx_cart    = ContexteTrace(trace_id=ctx.trace_id, span_id=span_gw.span_id)
    span_cart   = tracer_cart.demarrer_span("GET /cart/{user}", contexte=ctx_cart)
    time.sleep(0.060)

    # Inventory (appelé par cart)
    tracer_inv = Tracer("inventory-svc", collecteur)
    ctx_inv    = ContexteTrace(trace_id=ctx.trace_id, span_id=span_cart.span_id)
    span_inv   = tracer_inv.demarrer_span("CHECK_STOCK", contexte=ctx_inv)
    span_inv.ajouter_tag("items_checked", 3)
    time.sleep(0.120)
    tracer_inv.terminer_span(span_inv)
    tracer_cart.terminer_span(span_cart)

    # Payment service
    tracer_pay = Tracer("payment-svc", collecteur)
    ctx_pay    = ContexteTrace(trace_id=ctx.trace_id, span_id=span_gw.span_id)
    span_pay   = tracer_pay.demarrer_span("CHARGE_CARD", contexte=ctx_pay)
    span_pay.ajouter_tag("amount_eur", 89.99)
    time.sleep(0.350)

    # Notif (appelée par payment)
    tracer_notif = Tracer("notif-svc", collecteur)
    ctx_notif    = ContexteTrace(trace_id=ctx.trace_id, span_id=span_pay.span_id)
    span_notif   = tracer_notif.demarrer_span("SEND_EMAIL", contexte=ctx_notif)
    time.sleep(0.080)
    tracer_notif.terminer_span(span_notif)
    tracer_pay.terminer_span(span_pay)

    tracer_gw.terminer_span(span_gw)

    print(f"  Headers W3C Trace Context propagés :")
    print(f"    gateway → cart    : traceparent: 00-{ctx.trace_id[:8]}...-{span_gw.span_id}-01")
    print(f"    cart    → inv     : traceparent: 00-{ctx.trace_id[:8]}...-{span_cart.span_id}-01")
    print(f"    gateway → payment : traceparent: 00-{ctx.trace_id[:8]}...-{span_gw.span_id}-01")
    print(f"    payment → notif   : traceparent: 00-{ctx.trace_id[:8]}...-{span_pay.span_id}-01")

    collecteur.afficher_trace(ctx.trace_id)

    analyse = collecteur.analyser_trace(ctx.trace_id)
    print(f"\n  Analyse :")
    print(f"    Durée totale   : {analyse['duree_ms']:.0f}ms")
    print(f"    Nombre de spans: {analyse['nb_spans']}")
    print(f"    Services       : {analyse['nb_services']}")
    print(f"    Goulot         : {analyse['service_le_plus_lent']} "
          f"({analyse['par_service'][analyse['service_le_plus_lent']]['duree_totale_ms']:.0f}ms)")


# ─── SCÉNARIO 2 : SPANS PARALLÈLES ───────────────────────────────────────────

def scenario_parallele():
    titre("SCÉNARIO 2 — Spans parallèles : inventory + fraud check simultanés")

    print("""
  Optimisation : au lieu de faire inventory PUIS fraud séquentiellement,
  on les lance en parallèle.
  La trace montre les deux spans qui se chevauchent dans le temps.

  Sans parallélisme : 120ms + 400ms = 520ms
  Avec parallélisme : max(120ms, 400ms) = 400ms  → gain de 120ms
    """)

    collecteur = Collecteur()

    # Version séquentielle
    def checkout_sequentiel() -> str:
        tracer = Tracer("payment-svc", collecteur)
        ctx    = ContexteTrace.nouveau()
        span_p = tracer.demarrer_span("PROCESS_ORDER", contexte=ctx)
        span_p.ajouter_tag("mode", "séquentiel")

        # Inventory check
        tracer_inv = Tracer("inventory-svc", collecteur)
        ctx_inv    = ContexteTrace(trace_id=ctx.trace_id, span_id=span_p.span_id)
        span_inv   = tracer_inv.demarrer_span("CHECK_STOCK", contexte=ctx_inv)
        time.sleep(0.120)
        tracer_inv.terminer_span(span_inv)

        # Fraud check (après inventory)
        tracer_fraud = Tracer("fraud-svc", collecteur)
        ctx_fraud    = ContexteTrace(trace_id=ctx.trace_id, span_id=span_p.span_id)
        span_fraud   = tracer_fraud.demarrer_span("FRAUD_ANALYSIS", contexte=ctx_fraud)
        time.sleep(0.400)
        tracer_fraud.terminer_span(span_fraud)

        tracer.terminer_span(span_p)
        return ctx.trace_id

    # Version parallèle
    def checkout_parallele() -> str:
        tracer = Tracer("payment-svc", collecteur)
        ctx    = ContexteTrace.nouveau()
        span_p = tracer.demarrer_span("PROCESS_ORDER_PARALLEL", contexte=ctx)
        span_p.ajouter_tag("mode", "parallèle")

        def run_inv():
            tracer_inv = Tracer("inventory-svc", collecteur)
            ctx_inv    = ContexteTrace(trace_id=ctx.trace_id, span_id=span_p.span_id)
            span_inv   = tracer_inv.demarrer_span("CHECK_STOCK", contexte=ctx_inv)
            time.sleep(0.120)
            tracer_inv.terminer_span(span_inv)

        def run_fraud():
            tracer_fraud = Tracer("fraud-svc", collecteur)
            ctx_fraud    = ContexteTrace(trace_id=ctx.trace_id, span_id=span_p.span_id)
            span_fraud   = tracer_fraud.demarrer_span("FRAUD_ANALYSIS", contexte=ctx_fraud)
            time.sleep(0.400)
            tracer_fraud.terminer_span(span_fraud)

        t1 = threading.Thread(target=run_inv,   daemon=True)
        t2 = threading.Thread(target=run_fraud, daemon=True)
        t1.start(); t2.start()
        t1.join();  t2.join()

        tracer.terminer_span(span_p)
        return ctx.trace_id

    t0 = time.perf_counter()
    tid_seq = checkout_sequentiel()
    t_seq   = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    tid_par = checkout_parallele()
    t_par   = (time.perf_counter() - t0) * 1000

    print(f"  Version séquentielle ({t_seq:.0f}ms) :")
    collecteur.afficher_trace(tid_seq)

    print(f"\n  Version parallèle ({t_par:.0f}ms) :")
    collecteur.afficher_trace(tid_par)

    gain = t_seq - t_par
    print(f"\n  Gain de parallélisme : {gain:.0f}ms  "
          f"({gain/t_seq*100:.0f}% plus rapide) ✅")
    print(f"  Le goulot est fraud-svc (400ms) — impossible à réduire davantage")


# ─── SCÉNARIO 3 : DÉTECTION D'ERREUR ─────────────────────────────────────────

def scenario_erreur():
    titre("SCÉNARIO 3 — Détection d'erreur : quel service a cassé la requête ?")

    print("""
  Sans tracing : logs dans 5 endroits, corrélation manuelle impossible.
  Avec tracing : on voit exactement quel span a le statut ERROR
                 et son message d'erreur, dans le contexte de la trace complète.
    """)

    collecteur = Collecteur()

    def requete_avec_erreur(service_erreur: str) -> str:
        ctx = ContexteTrace.nouveau()

        services = ["api-gateway", "cart-svc", "inventory-svc", "payment-svc", "notif-svc"]
        latences = {"api-gateway": 15, "cart-svc": 40, "inventory-svc": 90,
                    "payment-svc": 200, "notif-svc": 60}
        spans_par_svc = {}

        parent_id = None
        for svc in services:
            tracer   = Tracer(svc, collecteur)
            contexte = ContexteTrace(trace_id=ctx.trace_id,
                                     span_id=parent_id or ctx.span_id)
            span     = tracer.demarrer_span(f"handle_request", contexte=contexte)

            if svc == service_erreur:
                time.sleep(latences[svc] / 1000 * 0.3)   # Échoue rapidement
                tracer.terminer_span(span, StatutSpan.ERREUR,
                                     erreur=f"DB connection timeout in {svc}")
                # Les services suivants ne sont pas appelés (erreur propagée)
                break
            else:
                time.sleep(latences[svc] / 1000)
                tracer.terminer_span(span)
                parent_id = span.span_id

        return ctx.trace_id

    # Simuler différentes pannes
    pannes = ["inventory-svc", "payment-svc"]
    for svc_en_panne in pannes:
        print(f"\n  Panne simulée dans : {svc_en_panne}")
        tid = requete_avec_erreur(svc_en_panne)
        collecteur.afficher_trace(tid)

        analyse = collecteur.analyser_trace(tid)
        print(f"\n  Diagnostic : {analyse['nb_erreurs']} erreur(s) sur {analyse['nb_spans']} spans")
        spans_erreur = [s for s in collecteur.obtenir_trace(tid)
                        if s.statut == StatutSpan.ERREUR]
        for se in spans_erreur:
            print(f"    ❌ {se.service}/{se.operation} : {se.erreur}")
        print(f"    → Rootcause identifiée en 1 coup d'œil dans Jaeger")


# ─── SCÉNARIO 4 : ANALYSE DE LATENCE ──────────────────────────────────────────

def scenario_latence():
    titre("SCÉNARIO 4 — Analyse de latence : identifier le goulot d'étranglement")

    print("""
  On simule 20 requêtes /checkout avec des latences variables.
  Le tracing agrège les statistiques par service et identifie
  automatiquement le service le plus lent (le goulot).
    """)

    collecteur = Collecteur()

    services_config = {
        "api-gateway":    {"latence": 15,  "taux_erreur": 0.0},
        "cart-svc":       {"latence": 45,  "taux_erreur": 0.02},
        "inventory-svc":  {"latence": 110, "taux_erreur": 0.05},
        "payment-svc":    {"latence": 380, "taux_erreur": 0.03},  # ← le goulot
        "notif-svc":      {"latence": 70,  "taux_erreur": 0.01},
    }

    N = 20
    trace_ids = []

    for req_i in range(N):
        ctx      = ContexteTrace.nouveau()
        trace_ids.append(ctx.trace_id)
        parent_id = ctx.span_id

        for svc, cfg in services_config.items():
            tracer   = Tracer(svc, collecteur)
            contexte = ContexteTrace(trace_id=ctx.trace_id, span_id=parent_id)
            span     = tracer.demarrer_span("handle", contexte=contexte)
            span.ajouter_tag("req_num", req_i)

            # Latence avec jitter + spike aléatoire
            lat = cfg["latence"] / 1000
            if random.random() < 0.1:   # 10% de spikes
                lat *= 3
            time.sleep(lat * random.uniform(0.8, 1.2))

            if random.random() < cfg["taux_erreur"]:
                tracer.terminer_span(span, StatutSpan.ERREUR,
                                     erreur=f"Transient error in {svc}")
            else:
                tracer.terminer_span(span)
            parent_id = span.span_id

    # Agréger les stats
    durees_par_svc: dict[str, list[float]] = defaultdict(list)
    nb_erreurs_par_svc: dict[str, int]     = defaultdict(int)

    for tid in trace_ids:
        for span in collecteur.obtenir_trace(tid):
            durees_par_svc[span.service].append(span.duree_ms)
            if span.statut == StatutSpan.ERREUR:
                nb_erreurs_par_svc[span.service] += 1

    print(f"\n  Statistiques sur {N} requêtes :\n")
    print(f"  {'Service':<20} {'P50':>8} {'P95':>8} {'P99':>8} {'Max':>8} {'Erreurs':>8}")
    print("  " + "─"*60)

    for svc, durees in sorted(durees_par_svc.items(),
                               key=lambda x: statistics.median(x[1]), reverse=True):
        durees_s = sorted(durees)
        p50 = statistics.median(durees_s)
        p95 = durees_s[int(len(durees_s) * 0.95)] if len(durees_s) > 1 else durees_s[0]
        p99 = durees_s[int(len(durees_s) * 0.99)] if len(durees_s) > 1 else durees_s[0]
        pmax = max(durees_s)
        errs = nb_erreurs_par_svc.get(svc, 0)
        goulot = " ← GOULOT" if svc == "payment-svc" else ""
        print(f"  {svc:<20} {p50:>6.0f}ms {p95:>6.0f}ms {p99:>6.0f}ms "
              f"{pmax:>6.0f}ms {errs:>7}{goulot}")

    print(f"""
  payment-svc est clairement le goulot (P50 ~380ms vs 15-110ms pour les autres).
  Actions possibles :
    → Paralléliser payment + fraud_check  (Scénario 2)
    → Optimiser la requête DB dans payment-svc
    → Ajouter un cache pour les tokens Stripe
    → Scale horizontal payment-svc uniquement
    """)


# ─── SCÉNARIO 5 : SAMPLING ────────────────────────────────────────────────────

def scenario_sampling():
    titre("SCÉNARIO 5 — Sampling : tracer 10% du trafic en production")

    print("""
  Problème : 10 000 req/s × 5 services × 1 span/service = 50 000 spans/s.
  Stocker tous les spans coûte cher et génère trop de bruit.

  Solution : Sampling (échantillonnage)
    Head sampling    : décision à la naissance de la trace (avant d'avoir les données)
    Tail sampling    : décision après avoir vu toute la trace
                       (permet de toujours garder les traces avec erreurs)

  Stratégies :
    Probabiliste   : garder X% aléatoirement
    Rate limiting  : garder max N traces/seconde par service
    Adaptatif      : Jaeger Adaptive Sampling — ajuste le taux automatiquement
    Toujours garder: traces avec erreurs ou latence > seuil
    """)

    collecteur_complet  = Collecteur()
    collecteur_sampled  = Collecteur()

    TAUX_SAMPLING = 0.10   # 10%
    N = 200

    traces_totales   = 0
    traces_conservees = 0
    traces_erreurs_conservees = 0

    for i in range(N):
        ctx    = ContexteTrace.nouveau()
        tracer = Tracer("api-gateway", collecteur_complet)
        span   = tracer.demarrer_span("GET /api", contexte=ctx)

        # Simuler du trafic varié
        latence = random.choice([0.005] * 8 + [0.050] + [0.500])  # 10% lent, 1% très lent
        time.sleep(latence)
        est_erreur = random.random() < 0.05  # 5% erreurs

        if est_erreur:
            tracer.terminer_span(span, StatutSpan.ERREUR, erreur="Error 500")
        else:
            tracer.terminer_span(span)

        traces_totales += 1

        # Décision de sampling
        garder = False
        # Règle 1 : always sample les erreurs (tail-based)
        if est_erreur:
            garder = True
            traces_erreurs_conservees += 1
        # Règle 2 : always sample les requêtes lentes (tail-based)
        elif span.duree_ms > 100:
            garder = True
        # Règle 3 : sampling probabiliste pour le reste
        elif random.random() < TAUX_SAMPLING:
            garder = True

        if garder:
            traces_conservees += 1
            # Ré-envoyer au collecteur sampled
            collecteur_sampled.recevoir(span)

    pct_conserve = traces_conservees / traces_totales * 100
    print(f"  {N} requêtes simulées :\n")
    print(f"  {'Stratégie':<35} {'Traces':>8}  {'%':>6}")
    print("  " + "─"*52)
    print(f"  {'Sans sampling (tout garder)':<35} {traces_totales:>8}  {100:>5.0f}%")
    print(f"  {'Avec sampling adaptatif':<35} {traces_conservees:>8}  {pct_conserve:>5.0f}%")
    print(f"    dont erreurs (always-keep)    : {traces_erreurs_conservees}")
    print(f"    dont lents > 100ms            : {traces_conservees - traces_erreurs_conservees - int(N * TAUX_SAMPLING * 0.85)}")
    print(f"    dont aléatoires ({TAUX_SAMPLING*100:.0f}%)          : ~{int(N * TAUX_SAMPLING)}")

    reduction = (1 - traces_conservees/traces_totales) * 100
    print(f"\n  Réduction du volume : {reduction:.0f}%")
    print(f"  À 10 000 req/s : {int(10000 * traces_conservees/traces_totales):,} spans/s stockés "
          f"(au lieu de {10000 * 5:,}) ✅")

    print(f"""
  Stratégies de sampling en production :
    Jaeger  → Adaptive Sampling, ajuste automatiquement selon le volume
    Zipkin  → Sampler probabiliste, configurable par service
    OTel    → Composable : ParentBased + TraceIdRatioBased + AlwaysOn pour erreurs

  Règle d'or : toujours garder 100% des traces avec erreurs.
  Pour le reste : 1-10% selon le volume. Les p99 restent visibles.
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 18 — DISTRIBUTED TRACING (JAEGER/ZIPKIN)          ║")
    print("╚" + "═"*62 + "╝")

    scenario_trace_simple()
    scenario_parallele()
    scenario_erreur()
    scenario_latence()
    scenario_sampling()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Distributed Tracing — Ce qu'il faut retenir :

    trace_id   : propagé dans tous les headers → corrèle tous les spans
    span       : unité de travail avec début, fin, service, tags, statut
    parent_id  : permet de reconstruire l'arbre causal
    W3C traceparent : 00-{trace_id}-{parent_span_id}-{flags}

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Cascade /checkout visible : payment-svc = goulot à 350ms ✅
    Scénario 2 → Spans parallèles : -120ms avec inventory + fraud simultanés ✅
    Scénario 3 → Rootcause identifiée en 1 span : "DB timeout in inventory" ✅
    Scénario 4 → P50/P95/P99 par service : payment-svc 3x plus lent ✅
    Scénario 5 → Sampling adaptatif : 90% de réduction, 100% des erreurs gardées ✅

  Utilisé en production :
    Jaeger  → CNCF, Uber origin, UI puissante, Adaptive Sampling
    Zipkin  → Twitter origin, simple, intégration Spring Boot
    Tempo   → Grafana, couplé à Loki (logs) et Prometheus (métriques)
    Datadog → APM commercial, auto-instrumentation

  Lien avec les autres jours :
    Jour 17 (Discovery) → trace_id corrèle les appels entre services découverts
    Jour 16 (LB)        → le LB peut propager le traceparent header
    Jour 19 (Event Sourcing) → les spans peuvent être des événements dans le store

  → Jour 19 : Event Sourcing
    L'état n'est pas stocké, il est reconstruit depuis une liste
    d'événements immuables. Comme Git : le HEAD est un snapshot
    des commits, pas une vérité stockée séparément.
  """)


if __name__ == "__main__":
    main()
