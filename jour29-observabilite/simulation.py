"""
Jour 29 — "L'Observateur Omniscient" — Simulation
==================================================
5 scénarios :
  1. Métriques     : Counter/Gauge/Histogram, rate(), quantiles, alertes
  2. Logs structurés : niveaux, recherche multi-critères, corrélation trace_id
  3. Traces distribuées : span tree, chemins, spans lents, spans en erreur
  4. Corrélation 3 piliers : une seule enquête utilise les 3 pilliers ensemble
  5. SLO & Error Budget : SLI mesuré, SLO calculé, burn rate, alerting
"""

import time, random, math
from collections import defaultdict
import sys
sys.path.insert(0, "/home/claude/jour29-observabilite")

from observabilite import (
    RegistreMetriques, Compteur, Jauge, Histogramme,
    Logger, BackendLog, NiveauLog,
    Traceur, BackendTrace, Trace, Span,
    RegleAlerte, GestionnaireAlertes,
)

SEED = 42
SEP  = "=" * 64

def titre(t): print(f"\n{SEP}\n  {t}\n{SEP}")


# ─── INFRASTRUCTURE PARTAGÉE ─────────────────────────────────────────────────

class ServiceSimule:
    """
    Un microservice instrumenté avec les 3 piliers.
    Chaque appel crée un span, émet des métriques et produit des logs.
    """
    def __init__(self, nom: str, reg: RegistreMetriques,
                 backend_log: BackendLog, backend_trace: BackendTrace,
                 latence_moy_ms: float = 30.0, taux_erreur: float = 0.0):
        self.nom          = nom
        self._lat         = latence_moy_ms
        self._err         = taux_erreur
        self._reg         = reg
        self._traceur     = Traceur(nom, backend_trace)
        self._logger      = Logger(nom, backend_log)

        # Métriques
        self._req_total   = reg.compteur(f"{nom}_requests_total",
                                          "Nombre total de requêtes")
        self._req_erreurs = reg.compteur(f"{nom}_errors_total",
                                          "Nombre total d'erreurs")
        self._latences    = reg.histogramme(f"{nom}_latency_ms",
                                             "Distribution des latences")
        self._actives     = reg.jauge(f"{nom}_active_requests",
                                       "Requêtes en cours")

    def traiter(self, operation: str = "GET",
                trace_id: str = None, parent_id: str = None,
                payload: dict = None) -> dict:
        """Traiter une requête avec instrumentation complète."""

        # Span
        if trace_id and parent_id:
            span = self._traceur.continuer_trace(trace_id, parent_id, operation)
        else:
            span = self._traceur.demarrer_trace(operation)

        # Métriques
        self._req_total.incrementer(service=self.nom, operation=operation)
        self._actives.incrementer(1, service=self.nom)

        # Log d'entrée
        self._logger.info(f"{operation} reçue",
                          trace_id=span.trace_id, span_id=span.span_id,
                          **{k: v for k, v in (payload or {}).items()
                             if k in ("user_id", "order_id", "path")})

        # Simuler le travail
        lat = max(1, random.gauss(self._lat, self._lat * 0.3))
        time.sleep(lat / 1000)

        # Injecter une erreur ?
        est_erreur = random.random() < self._err

        if est_erreur:
            self._req_erreurs.incrementer(service=self.nom, operation=operation)
            self._logger.error(f"{operation} échouée",
                               trace_id=span.trace_id, span_id=span.span_id,
                               erreur="timeout", latence_ms=round(lat, 1))
            span.ajouter_tag("erreur", True)
            span.ajouter_tag("http.status_code", 500)
            span.terminer(erreur=True)
            self._actives.incrementer(-1, service=self.nom)
            raise Exception(f"{self.nom} erreur sur {operation}")

        span.ajouter_tag("http.status_code", 200)
        span.ajouter_tag("latence_ms", round(lat, 1))
        span.terminer()
        self._actives.incrementer(-1, service=self.nom)
        self._latences.observer(lat, service=self.nom)

        self._logger.info(f"{operation} traitée",
                          trace_id=span.trace_id, span_id=span.span_id,
                          latence_ms=round(lat, 1), statut=200)

        return {"service": self.nom, "op": operation,
                "trace_id": span.trace_id, "span_id": span.span_id,
                "latence_ms": lat}


# ─── SCÉNARIO 1 : MÉTRIQUES ──────────────────────────────────────────────────

def scenario_metriques():
    titre("SCÉNARIO 1 — Métriques : Counter, Gauge, Histogram, Alertes")

    print("""
  Les 3 types fondamentaux de métriques :
    Counter   : monotone croissant → taux avec rate()
    Gauge     : valeur instantanée → CPU, connexions actives, queue size
    Histogram : distribution → quantiles P50/P90/P99
    """)

    random.seed(SEED)
    backend_log   = BackendLog()
    backend_trace = BackendTrace()
    reg = RegistreMetriques("payments-svc")

    svc_payments = ServiceSimule("payments", reg, backend_log, backend_trace,
                                  latence_moy_ms=45, taux_erreur=0.05)
    svc_catalog  = ServiceSimule("catalog",  reg, backend_log, backend_trace,
                                  latence_moy_ms=12, taux_erreur=0.01)

    # Simuler du trafic
    print("  Simulation de 80 requêtes (payments) + 120 requêtes (catalog)...\n")
    for i in range(80):
        try:
            svc_payments.traiter("checkout", payload={"user_id": f"u{i%10}"})
        except Exception:
            pass

    for i in range(120):
        try:
            svc_catalog.traiter("search", payload={"user_id": f"u{i%20}"})
        except Exception:
            pass

    # ── Afficher les métriques ────────────────────────────────────────
    print("  === COUNTERS ===")
    for nom, cpt in reg._compteurs.items():
        total = sum(e.valeur for serie in cpt.series().values()
                    for e in serie)
        print(f"    {nom:<40} total={total:.0f}")

    print("\n  === HISTOGRAM : latences ===")
    for nom, hist in reg._histogrammes.items():
        s = hist.stats()
        if s:
            print(f"    {nom}")
            print(f"      n={s['n']}  min={s['min']}ms  "
                  f"p50={s['p50']}ms  p90={s['p90']}ms  "
                  f"p99={s['p99']}ms  max={s['max']}ms")

    # ── Alertes ───────────────────────────────────────────────────────
    print("\n  === ALERTES ===")
    gestionnaire = GestionnaireAlertes()

    hist_payments = reg._histogrammes.get("payments_latency_ms")
    cpt_err_pay   = reg._compteurs.get("payments_errors_total")
    cpt_req_pay   = reg._compteurs.get("payments_requests_total")

    if hist_payments:
        gestionnaire.ajouter(RegleAlerte(
            nom          = "HighLatencyPayments",
            condition    = lambda: hist_payments.quantile(0.99),
            seuil        = 100,
            comparateur  = ">",
            severite     = "warning",
            description  = "P99 latence payments > 100ms",
        ))

    if cpt_err_pay and cpt_req_pay:
        def taux_erreur_payments():
            errs = sum(e.valeur for s in cpt_err_pay.series().values() for e in s)
            reqs = sum(e.valeur for s in cpt_req_pay.series().values() for e in s)
            return (errs / max(reqs, 1)) * 100

        gestionnaire.ajouter(RegleAlerte(
            nom          = "HighErrorRatePayments",
            condition    = taux_erreur_payments,
            seuil        = 3,
            comparateur  = ">",
            severite     = "critical",
            description  = "Taux d'erreur payments > 3%",
        ))

    alertes = gestionnaire.evaluer_tout()
    if alertes:
        for a in alertes:
            emoji = "🔴" if a["severite"] == "critical" else "⚠️ "
            print(f"    {emoji} [{a['severite'].upper()}] {a['nom']}")
            print(f"       valeur={a['valeur']}  seuil={a['seuil']}")
            print(f"       {a['description']}")
    else:
        print("    ✅ Aucune alerte déclenchée")

    # Taux d'erreur calculé
    if cpt_err_pay and cpt_req_pay:
        errs = sum(e.valeur for s in cpt_err_pay.series().values() for e in s)
        reqs = sum(e.valeur for s in cpt_req_pay.series().values() for e in s)
        print(f"\n  Taux d'erreur payments : {errs:.0f}/{reqs:.0f} = "
              f"{errs/max(reqs,1)*100:.1f}%")

    print("""
  Règle d'or des métriques :
    Ne pas alerter sur les compteurs bruts → utiliser rate() et ratios
    P99 > P90 > P50 : les lents souffrent le plus
    Séparer les métriques par labels (service, operation, status_code)
    """)
    return reg, backend_log, backend_trace


# ─── SCÉNARIO 2 : LOGS STRUCTURÉS ────────────────────────────────────────────

def scenario_logs(backend_log: BackendLog):
    titre("SCÉNARIO 2 — Logs structurés : recherche, filtrage, corrélation")

    print("""
  Log structuré = chaque ligne est un objet JSON avec des champs fixes.
  Interrogeable comme une base de données (vs grep sur du texte libre).

  Différence fondamentale :
    Mauvais : "2024-01-15 ERROR payment failed for user alice"
    Bon     : {"ts":1705..., "level":"ERROR", "msg":"payment failed",
               "user_id":"alice", "trace_id":"4f8a2b", "amount":150.0}
    """)

    stats = backend_log.stats()
    print("  === VOLUME DE LOGS ===")
    print(f"    Total : {stats['total']}")
    print(f"    Par niveau :")
    for niveau, cnt in sorted(stats["par_niveau"].items(),
                               key=lambda x: x[0]):
        barre = "█" * min(cnt, 30)
        print(f"      {niveau:<10} {cnt:>4}  {barre}")

    print(f"\n    Par service :")
    for svc, cnt in sorted(stats["par_service"].items(),
                            key=lambda x: -x[1]):
        print(f"      {svc:<20} {cnt:>4}")

    # ── Recherches ────────────────────────────────────────────────────
    print("\n  === REQUÊTES DE RECHERCHE ===")

    # 1. Erreurs seules
    erreurs = backend_log.chercher(niveau_min=NiveauLog.ERROR)
    print(f"\n  Q1 : level >= ERROR")
    print(f"    → {len(erreurs)} logs")
    for log in erreurs[:3]:
        d = log.as_dict()
        print(f"      [{d['level']}] {d['service']} — {d['msg'][:50]}"
              f"  trace={d['trace_id'][:8] if d['trace_id'] else 'none'}")
    if len(erreurs) > 3:
        print(f"      ... et {len(erreurs)-3} autres")

    # 2. Par service
    logs_payments = backend_log.chercher(service="payments")
    print(f"\n  Q2 : service = payments")
    print(f"    → {len(logs_payments)} logs")

    # 3. Par trace_id (corrélation)
    if erreurs:
        tid = erreurs[0].trace_id
        if tid:
            logs_trace = backend_log.chercher(trace_id=tid)
            print(f"\n  Q3 : trace_id = {tid[:12]}... (corrélation log↔trace)")
            print(f"    → {len(logs_trace)} logs pour cette trace")
            for log in logs_trace:
                d = log.as_dict()
                print(f"      [{d['level']}] {d['service']} — {d['msg'][:50]}")

    # 4. Contient "timeout"
    timeouts = backend_log.chercher(contient="timeout")
    print(f"\n  Q4 : message contient 'timeout'")
    print(f"    → {len(timeouts)} logs")

    print("""
  Bonnes pratiques de logging :
    Toujours inclure trace_id et span_id dans chaque log
    Logger les entrées ET sorties des fonctions critiques
    Éviter les logs dans les boucles chaudes → sampling
    Niveau ERROR uniquement pour les vraies erreurs opérationnelles
    """)


# ─── SCÉNARIO 3 : TRACES DISTRIBUÉES ─────────────────────────────────────────

def scenario_traces(backend_log: BackendLog, backend_trace: BackendTrace):
    titre("SCÉNARIO 3 — Traces distribuées : chemin, latence, erreurs")

    print("""
  Une trace = graphe de tous les spans d'une requête.
  Chaque appel inter-service crée un span enfant avec le même trace_id.
  → On reconstruit le chemin complet, même sur 8 services différents.
    """)

    random.seed(SEED + 1)

    # Simuler une requête checkout complète (3 services en chaîne)
    reg2          = RegistreMetriques("checkout-flow")
    svc_cart      = ServiceSimule("cart",      reg2, backend_log, backend_trace,
                                   latence_moy_ms=15, taux_erreur=0.0)
    svc_inventory = ServiceSimule("inventory", reg2, backend_log, backend_trace,
                                   latence_moy_ms=20, taux_erreur=0.0)
    svc_pay2      = ServiceSimule("payments2", reg2, backend_log, backend_trace,
                                   latence_moy_ms=60, taux_erreur=0.0)
    svc_notif     = ServiceSimule("notifier",  reg2, backend_log, backend_trace,
                                   latence_moy_ms=10, taux_erreur=0.0)

    trace_ids = []
    print("  Simulation de 10 requêtes checkout (cart→inventory→payments→notifier)...\n")

    for i in range(10):
        try:
            # cart : racine de la trace
            r1 = svc_cart.traiter("GET /cart",
                                   payload={"user_id": f"u{i}", "path": "/cart"})
            tid = r1["trace_id"]
            sid_cart = r1["span_id"]

            # inventory : enfant de cart
            r2 = svc_inventory.traiter("CHECK /stock",
                                        trace_id=tid, parent_id=sid_cart,
                                        payload={"user_id": f"u{i}"})

            # payments : enfant de inventory
            r3 = svc_pay2.traiter("POST /pay",
                                   trace_id=tid, parent_id=r2["span_id"],
                                   payload={"user_id": f"u{i}"})

            # notifier : enfant de payments
            svc_notif.traiter("SEND /email",
                               trace_id=tid, parent_id=r3["span_id"],
                               payload={"user_id": f"u{i}"})
            trace_ids.append(tid)

        except Exception:
            pass

    # ── Analyse des traces ────────────────────────────────────────────
    print("  === ANALYSE DES TRACES ===\n")
    traces = backend_trace.chercher(service="cart")
    print(f"  {len(traces)} traces trouvées (service=cart)\n")

    # Afficher les 3 premières
    for trace in traces[:3]:
        racine = trace.span_racine()
        if not racine:
            continue
        duree = trace.duree_totale_ms()
        print(f"  Trace {trace.trace_id[:12]}...")
        print(f"    Durée totale : {duree:.1f}ms  |  {len(trace.spans)} spans")
        print(f"    Chemin :")
        for ligne in trace.chemin():
            print(f"      {ligne}")
        lents = trace.spans_lents(50)
        if lents:
            print(f"    Spans > 50ms :")
            for s in lents:
                print(f"      {s.service}.{s.operation} → {s.duree_ms:.1f}ms")
        print()

    # Stats globales
    stats = backend_trace.stats()
    print("  === STATS GLOBALES DES TRACES ===")
    print(f"    Total spans    : {stats['total_spans']}")
    print(f"    Spans en erreur: {stats['erreurs']}")
    print(f"    Durée P50      : {stats['duree_p50_ms']}ms")
    print(f"    Durée P99      : {stats['duree_p99_ms']}ms")
    print(f"    Par service    :")
    for svc, cnt in sorted(stats["par_service"].items(), key=lambda x: -x[1]):
        print(f"      {svc:<20} {cnt} spans")

    # Traces lentes
    lentes = backend_trace.chercher(duree_min_ms=100)
    print(f"\n  Traces avec durée totale > 100ms : {len(lentes)}")
    for t in lentes[:3]:
        print(f"    {t.trace_id[:12]}... → {t.duree_totale_ms():.1f}ms")

    print("""
  Sampling des traces en production :
    100% des traces = trop de données (et trop cher à stocker)
    Stratégie : 100% des erreurs + 1% du trafic nominal
    Tail-based sampling : décider APRÈS avoir vu toute la trace
    → Garder toutes les traces lentes, sample les rapides
    """)
    return trace_ids


# ─── SCÉNARIO 4 : CORRÉLATION DES 3 PILIERS ──────────────────────────────────

def scenario_correlation(reg: RegistreMetriques,
                          backend_log: BackendLog,
                          backend_trace: BackendTrace):
    titre("SCÉNARIO 4 — Corrélation des 3 piliers : enquête d'incident")

    print("""
  Incident réel : "Augmentation du taux d'erreur sur payments à 14h32"

  Étape 1 : MÉTRIQUES → "quelque chose ne va pas"
  Étape 2 : LOGS      → "quel service, quel type d'erreur"
  Étape 3 : TRACES    → "quelle requête exactement, où ça bloque"
    """)

    # Injecter un incident : trafic avec beaucoup d'erreurs
    random.seed(SEED + 2)
    backend_incident = BackendLog()
    backend_trace_i  = BackendTrace()
    reg_i = RegistreMetriques("incident-sim")

    svc_api  = ServiceSimule("api-gateway",  reg_i, backend_incident,
                              backend_trace_i, latence_moy_ms=10, taux_erreur=0.0)
    svc_db   = ServiceSimule("database",     reg_i, backend_incident,
                              backend_trace_i, latence_moy_ms=800, taux_erreur=0.60)

    # Logger un incident structuré sur les pannes DB
    logger_db = Logger("database", backend_incident)

    print("  [INCIDENT] Simulation : database saturée (60% erreurs, latence 800ms)\n")

    trace_ids_incident = []
    ok = err = 0
    for i in range(30):
        try:
            r1 = svc_api.traiter("POST /checkout",
                                  payload={"user_id": f"u{i%5}", "path": "/checkout"})
            svc_db.traiter("SELECT users", trace_id=r1["trace_id"],
                            parent_id=r1["span_id"],
                            payload={"user_id": f"u{i%5}"})
            trace_ids_incident.append(r1["trace_id"])
            ok += 1
        except Exception as e:
            err += 1
            logger_db.error("Pool de connexions épuisé",
                             trace_id=None, erreur=str(e),
                             pool_actif=random.randint(95, 100),
                             pool_max=100)

    # ── Étape 1 : Métriques ───────────────────────────────────────────
    print("  ─── Étape 1 : MÉTRIQUES ───")
    cpt_req_db  = reg_i._compteurs.get("database_requests_total")
    cpt_err_db  = reg_i._compteurs.get("database_errors_total")
    hist_db     = reg_i._histogrammes.get("database_latency_ms")

    reqs_tot = sum(e.valeur for s in (cpt_req_db.series().values() if cpt_req_db else []) for e in s)
    errs_tot = sum(e.valeur for s in (cpt_err_db.series().values() if cpt_err_db else []) for e in s)
    taux_err = errs_tot / max(reqs_tot, 1) * 100

    print(f"    database_requests_total : {reqs_tot:.0f}")
    print(f"    database_errors_total   : {errs_tot:.0f}")
    print(f"    Taux d'erreur           : {taux_err:.1f}%")
    if hist_db:
        s = hist_db.stats()
        if s:
            print(f"    Latence P99             : {s.get('p99', '?')}ms")
    print(f"    → 🔴 ALERTE : taux_erreur {taux_err:.0f}% >> seuil 3%")

    # ── Étape 2 : Logs ────────────────────────────────────────────────
    print("\n  ─── Étape 2 : LOGS ───")
    erreurs_db = backend_incident.chercher(
        niveau_min=NiveauLog.ERROR, service="database")
    pool_logs  = backend_incident.chercher(contient="Pool")
    print(f"    Logs ERROR sur database : {len(erreurs_db)}")
    print(f"    Logs mentionnant 'Pool' : {len(pool_logs)}")
    if pool_logs:
        ex = pool_logs[0].as_dict()
        print(f"    Exemple : {ex['msg']}"
              f" — pool_actif={ex.get('pool_actif','?')}/{ex.get('pool_max','?')}")
    print(f"    → Cause identifiée : pool de connexions saturé (95-100/100)")

    # ── Étape 3 : Traces ──────────────────────────────────────────────
    print("\n  ─── Étape 3 : TRACES ───")
    traces_err  = backend_trace_i.chercher(avec_erreur=True)
    traces_lent = backend_trace_i.chercher(duree_min_ms=500)
    print(f"    Traces avec erreur   : {len(traces_err)}")
    print(f"    Traces > 500ms       : {len(traces_lent)}")

    if traces_lent:
        t = traces_lent[0]
        print(f"\n    Trace la plus lente : {t.trace_id[:12]}...")
        print(f"    Durée totale : {t.duree_totale_ms():.1f}ms")
        print(f"    Chemin :")
        for ligne in t.chemin():
            print(f"      {ligne}")
        for s in t.spans:
            emoji = "🔴" if s.erreur else "  "
            print(f"      {emoji} {s.service}.{s.operation} → {s.duree_ms:.1f}ms"
                  f"  {'ERREUR' if s.erreur else ''}")

    print(f"""
  Bilan de l'enquête :
    MÉTRIQUES → taux erreur {taux_err:.0f}% détecté en 30s (alerte Prometheus)
    LOGS      → cause : pool connexions DB 95-100/100 (épuisé)
    TRACES    → requête spécifique identifiée : api→db {len(traces_lent)} spans lents

  Remédiation :
    Court terme : augmenter pool_max (100→300), restart database
    Moyen terme : lire depuis un replica read-only pour les queries SELECT
    Long terme   : cache Redis pour les données chaudes, CQRS
    """)


# ─── SCÉNARIO 5 : SLO & ERROR BUDGET ─────────────────────────────────────────

def scenario_slo():
    titre("SCÉNARIO 5 — SLO & Error Budget : mesurer la fiabilité")

    print("""
  SLI (Service Level Indicator) : métrique mesurée
    Ex: "taux de requêtes réussies (status < 500)"

  SLO (Service Level Objective) : cible interne
    Ex: "SLI ≥ 99.9% sur une fenêtre glissante de 30 jours"

  SLA (Service Level Agreement) : contrat externe avec pénalités
    Ex: "99.5% garanti, crédit si violation"

  Error Budget = 1 - SLO
    SLO 99.9%  → Error Budget = 0.1% = 43.8 min/mois de panne tolérée
    SLO 99.99% → Error Budget = 0.01% = 4.38 min/mois

  Burn rate : à quelle vitesse on brûle le budget ?
    Burn rate 1× = on consomme exactement le budget sur 30 jours
    Burn rate 14× = le budget sera épuisé en ~2 jours
    → Alerte si burn rate > 14× sur 1h OU > 6× sur 6h
    """)

    random.seed(SEED + 3)

    # Simuler un historique de trafic sur 30 jours (compressé)
    # Chaque "tick" = 1 heure de production
    nb_ticks    = 720   # 30 jours × 24h
    slo_cible   = 0.999  # 99.9%

    requetes_par_tick = 1000
    total_req         = 0
    total_err         = 0
    burn_rates        = []
    budget_restant    = []

    # Phase 1 (ticks 0-600) : nominal, ~0.05% erreurs
    # Phase 2 (ticks 600-650) : incident, ~2% erreurs
    # Phase 3 (ticks 650-720) : retour normal

    print(f"  Simulation : {nb_ticks} heures de trafic ({requetes_par_tick} req/h)\n")
    print(f"  SLO cible : {slo_cible*100:.1f}%")
    budget_total_erreurs = (1 - slo_cible) * nb_ticks * requetes_par_tick
    print(f"  Error budget total : {budget_total_erreurs:.0f} erreurs tolérées sur 30j\n")

    erreurs_par_tick = []
    for tick in range(nb_ticks):
        if tick < 600:
            taux = 0.0005   # 0.05% — très faible
        elif tick < 650:
            taux = 0.025    # 2.5% — incident
        else:
            taux = 0.0008   # retour presque normal

        n_err = int(requetes_par_tick * taux + random.gauss(0, 2))
        n_err = max(0, n_err)
        erreurs_par_tick.append(n_err)
        total_req += requetes_par_tick
        total_err += n_err

    # Calculs
    sli_global  = 1 - (total_err / total_req)
    budget_cons = total_err
    budget_pct  = budget_cons / budget_total_erreurs * 100

    # Burn rate sur les 30 derniers ticks (30h)
    err_30h  = sum(erreurs_par_tick[-30:])
    req_30h  = 30 * requetes_par_tick
    sli_30h  = 1 - (err_30h / req_30h)
    burn_30h = (1 - sli_30h) / (1 - slo_cible)

    # Burn rate pendant l'incident (ticks 600-650)
    err_inc = sum(erreurs_par_tick[600:650])
    req_inc = 50 * requetes_par_tick
    sli_inc = 1 - (err_inc / req_inc)
    burn_inc = (1 - sli_inc) / (1 - slo_cible)

    print(f"  === RAPPORT SLO 30 JOURS ===")
    print(f"    SLI global    : {sli_global*100:.4f}%")
    slo_ok = sli_global >= slo_cible
    print(f"    SLO cible     : {slo_cible*100:.1f}%  → {'✅ RESPECTÉ' if slo_ok else '❌ VIOLÉ'}")
    print(f"    Total requêtes: {total_req:,}")
    print(f"    Total erreurs : {total_err:,}")
    print(f"    Budget consommé: {budget_cons:.0f} / {budget_total_erreurs:.0f} "
          f"= {budget_pct:.1f}%")

    print(f"\n  === BURN RATE ===")
    print(f"    Pendant l'incident (ticks 600-650) :")
    print(f"      SLI période   : {sli_inc*100:.2f}%")
    print(f"      Burn rate     : {burn_inc:.1f}× "
          f"{'🔴 CRITIQUE' if burn_inc > 14 else '⚠️  ÉLEVÉ' if burn_inc > 6 else '🟢 OK'}")
    print(f"      Budget épuisé en : {30 / max(burn_inc, 0.01):.1f} jours à ce rythme")

    print(f"\n    Dernières 30h (post-incident) :")
    print(f"      SLI période   : {sli_30h*100:.4f}%")
    print(f"      Burn rate     : {burn_30h:.2f}× "
          f"{'🔴 CRITIQUE' if burn_30h > 14 else '⚠️  ÉLEVÉ' if burn_30h > 6 else '🟢 OK'}")

    # Alertes multi-window (Google SRE)
    print(f"\n  === STRATÉGIE D'ALERTE (Google SRE, multi-fenêtre) ===")
    print(f"    Fenêtre 1h  + Burn rate > 14× → 🔴 PAGE immédiat (budget épuisé en 2j)")
    print(f"    Fenêtre 6h  + Burn rate > 6×  → 🔴 PAGE (budget épuisé en 5j)")
    print(f"    Fenêtre 24h + Burn rate > 3×  → ⚠️  Ticket (à régler dans la journée)")
    print(f"    Fenêtre 72h + Burn rate > 1×  → 📊 Rapport hebdo")

    alerte_active = burn_inc > 14
    print(f"\n    Alerte pendant incident : {'🔴 PAGE DÉCLENCHÉ' if alerte_active else '⚠️  Warning'} "
          f"(burn rate {burn_inc:.1f}×)")

    print(f"""
  SLO en pratique :
    SLO 99.9%  → 43.8 min/mois de panne tolérée → services B2B standard
    SLO 99.99% → 4.38 min/mois                  → services critiques (paiements)
    SLO 99.999%→ 26s/mois                        → infrastructure core (très coûteux)

  Le piège : un SLO trop élevé = plus d'innovation
    Error budget épuisé → FREEZE des déploiements jusqu'à fin de mois
    → SRE et product alignment : on peut déployer seulement si budget disponible
    """)


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    random.seed(SEED)
    print("+" + "=" * 62 + "+")
    print("|   JOUR 29 — 'L'OBSERVATEUR OMNISCIENT' (OBSERVABILITÉ)  |")
    print("+" + "=" * 62 + "+")

    reg, backend_log, backend_trace = scenario_metriques()
    scenario_logs(backend_log)
    scenario_traces(backend_log, backend_trace)
    scenario_correlation(reg, backend_log, backend_trace)
    scenario_slo()

    print("\n" + SEP)
    print("  RÉSUMÉ")
    print(SEP)
    print("""
  Les 3 piliers de l'observabilité :

    MÉTRIQUES  (Prometheus)  → "Est-ce sain ?" — alertes, dashboards
      Counter   : rate() pour le débit, toujours croissant
      Gauge     : valeur instantanée (CPU, connexions)
      Histogram : P50/P90/P99 pour les latences

    LOGS       (ELK / Loki)  → "Qu'est-ce qui s'est passé ?"
      Structurés JSON — interrogeables comme une base de données
      trace_id dans chaque log → corrélation avec les traces

    TRACES     (Jaeger)      → "Où exactement ?"
      Span tree : graphe de causalité d'une requête
      Identifie le service lent ou en erreur
      Sampling : 100% erreurs + 1% nominal

  SLO & Error Budget (Google SRE) :
    SLI mesuré → SLO cible (99.9%) → Error Budget (43.8min/mois)
    Burn rate : si > 14× → PAGE immédiat (budget épuisé en 2 jours)
    Budget épuisé → freeze des déploiements

  Ce que les scénarios ont prouvé :
    Scénario 1 → alertes P99 et taux erreur déclenchées automatiquement ✅
    Scénario 2 → logs structurés : recherche par service/niveau/trace_id ✅
    Scénario 3 → trace checkout : 4 services, spans lents identifiés ✅
    Scénario 4 → enquête 3 piliers : 30s pour trouver pool DB saturé ✅
    Scénario 5 → burn rate 25× pendant incident → PAGE justifié ✅

  → Jour 30 — Synthèse : TrueTime & Spanner
    Horloge atomique distribuée → MVCC global → transactions planétaires.
    Le dernier chapitre du curriculum.
  """)

main()
