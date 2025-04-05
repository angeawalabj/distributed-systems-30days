"""
Jour 29 — "L'Observateur Omniscient" — Observabilité
======================================================
Le problème :
  Un système distribué tombe. Les logs sont sur 47 machines différentes.
  La requête qui a échoué a traversé 8 services.
  Personne ne sait où ça a pêché ni pourquoi.

Les 3 piliers de l'observabilité (CNCF) :
  MÉTRIQUES  : "est-ce que le système est en bonne santé ?"
               Valeurs numériques agrégées dans le temps.
               → Prometheus, StatsD, OpenMetrics

  LOGS       : "qu'est-ce qui s'est passé exactement ?"
               Événements textuels avec contexte.
               → Structured logging (JSON), ELK Stack, Loki

  TRACES     : "comment une requête a traversé le système ?"
               Graphe de causalité d'une requête end-to-end.
               → OpenTelemetry, Jaeger, Zipkin

Ces 3 piliers sont COMPLÉMENTAIRES, pas substituables :
  Métrique 500/s → alerte → QUAND ?
  Log "payment failed" → QUOI ? → MAIS PAS POURQUOI ?
  Trace → "cart→payments→db : timeout à 2.3s" → POURQUOI ✅

La 4e dimension (émergente) :
  ÉVÉNEMENTS STRUCTURÉS (Honeycomb)
  Un log avec 50 champs → interrogeable comme une base de données
  "Donne-moi toutes les requêtes > 500ms de l'utilisateur alice"

Corrélation des 3 piliers :
  TraceID dans les logs → naviguer log↔trace
  Métriques avec labels → drill-down trace→métrique
  → "La métrique d'erreur a augmenté à 14h32,
     les logs montrent des timeouts DB,
     la trace 4f8a montre exactement quelle requête a saturé le pool"
"""

from __future__ import annotations
import time, math, random, threading
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from collections import defaultdict
from enum import Enum


# ─── 1. MÉTRIQUES ────────────────────────────────────────────────────────────

class TypeMetrique(Enum):
    COUNTER   = "counter"    # Toujours croissant (nb requêtes total)
    GAUGE     = "gauge"      # Valeur instantanée (CPU %, connexions actives)
    HISTOGRAM = "histogram"  # Distribution (latences)
    SUMMARY   = "summary"    # Quantiles pré-calculés


@dataclass
class EchantillonMetrique:
    valeur:    float
    timestamp: float
    labels:    dict


class Compteur:
    """Counter : monotone croissant. rate() pour le débit."""
    def __init__(self, nom: str, description: str = "", labels: list = None):
        self.nom         = nom
        self.description = description
        self._labels_def = labels or []
        self._series: dict[tuple, list] = defaultdict(list)
        self._lock = threading.Lock()

    def incrementer(self, valeur: float = 1.0, **labels):
        cle = tuple(sorted(labels.items()))
        with self._lock:
            self._series[cle].append(EchantillonMetrique(valeur, time.time(), labels))

    def valeur_totale(self, **labels) -> float:
        cle = tuple(sorted(labels.items()))
        with self._lock:
            return sum(e.valeur for e in self._series[cle])

    def rate(self, fenetre_s: float = 60, **labels) -> float:
        """Débit par seconde sur la fenêtre."""
        cle   = tuple(sorted(labels.items()))
        now   = time.time()
        debut = now - fenetre_s
        with self._lock:
            recents = [e for e in self._series[cle] if e.timestamp >= debut]
        return sum(e.valeur for e in recents) / max(fenetre_s, 1)

    def series(self) -> dict:
        with self._lock:
            return {k: list(v) for k, v in self._series.items()}


class Jauge:
    """Gauge : valeur instantanée, peut monter ou descendre."""
    def __init__(self, nom: str, description: str = ""):
        self.nom         = nom
        self.description = description
        self._valeurs: dict[tuple, float] = {}
        self._historique: dict[tuple, list] = defaultdict(list)
        self._lock = threading.Lock()

    def definir(self, valeur: float, **labels):
        cle = tuple(sorted(labels.items()))
        with self._lock:
            self._valeurs[cle] = valeur
            self._historique[cle].append(EchantillonMetrique(valeur, time.time(), labels))

    def incrementer(self, delta: float, **labels):
        cle = tuple(sorted(labels.items()))
        with self._lock:
            self._valeurs[cle] = self._valeurs.get(cle, 0) + delta
            self._historique[cle].append(
                EchantillonMetrique(self._valeurs[cle], time.time(), labels))

    def valeur(self, **labels) -> float:
        cle = tuple(sorted(labels.items()))
        with self._lock:
            return self._valeurs.get(cle, 0.0)

    def historique(self, **labels) -> list:
        cle = tuple(sorted(labels.items()))
        with self._lock:
            return list(self._historique[cle])


class Histogramme:
    """
    Histogram : distribution des valeurs.
    Buckets prédéfinis → permet de calculer les quantiles approximatifs.
    En production : Prometheus histogram_quantile() fonction.
    """
    BUCKETS_LATENCE = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]  # ms

    def __init__(self, nom: str, description: str = "",
                 buckets: list = None):
        self.nom         = nom
        self.description = description
        self._buckets    = sorted(buckets or self.BUCKETS_LATENCE)
        self._series: dict[tuple, list] = defaultdict(list)
        self._lock = threading.Lock()

    def observer(self, valeur: float, **labels):
        cle = tuple(sorted(labels.items()))
        with self._lock:
            self._series[cle].append(EchantillonMetrique(valeur, time.time(), labels))

    def quantile(self, q: float, **labels) -> float:
        """Calculer le quantile q (0.0-1.0) depuis les données brutes."""
        cle = tuple(sorted(labels.items()))
        with self._lock:
            vals = sorted(e.valeur for e in self._series[cle])
        if not vals:
            return 0.0
        idx = int(q * len(vals))
        return vals[min(idx, len(vals) - 1)]

    def buckets_cumules(self, **labels) -> dict:
        """Compter les observations par bucket (≤ seuil)."""
        cle = tuple(sorted(labels.items()))
        with self._lock:
            vals = [e.valeur for e in self._series[cle]]
        return {b: sum(1 for v in vals if v <= b)
                for b in self._buckets}

    def stats(self, **labels) -> dict:
        cle = tuple(sorted(labels.items()))
        with self._lock:
            vals = sorted(e.valeur for e in self._series[cle])
        if not vals:
            return {}
        return {
            "n":   len(vals),
            "min": round(vals[0], 2),
            "p50": round(vals[int(len(vals)*0.50)], 2),
            "p90": round(vals[int(len(vals)*0.90)], 2),
            "p99": round(vals[int(len(vals)*0.99)], 2),
            "max": round(vals[-1], 2),
            "moy": round(sum(vals)/len(vals), 2),
        }


class RegistreMetriques:
    """
    Registre central de toutes les métriques.
    Equivalent de prometheus.Registry.
    Expose /metrics en format text.
    """
    def __init__(self, service: str):
        self.service    = service
        self._compteurs:   dict[str, Compteur]    = {}
        self._jauges:      dict[str, Jauge]       = {}
        self._histogrammes:dict[str, Histogramme] = {}

    def compteur(self, nom: str, desc: str = "") -> Compteur:
        if nom not in self._compteurs:
            self._compteurs[nom] = Compteur(nom, desc)
        return self._compteurs[nom]

    def jauge(self, nom: str, desc: str = "") -> Jauge:
        if nom not in self._jauges:
            self._jauges[nom] = Jauge(nom, desc)
        return self._jauges[nom]

    def histogramme(self, nom: str, desc: str = "",
                    buckets: list = None) -> Histogramme:
        if nom not in self._histogrammes:
            self._histogrammes[nom] = Histogramme(nom, desc, buckets)
        return self._histogrammes[nom]

    def snapshot(self) -> dict:
        """Exporter toutes les métriques (format interne)."""
        return {
            "service":     self.service,
            "timestamp":   time.time(),
            "compteurs":   {n: c.series() for n, c in self._compteurs.items()},
            "jauges":      {n: j._valeurs for n, j in self._jauges.items()},
            "histogrammes":{n: h.stats() for n, h in self._histogrammes.items()},
        }


# ─── 2. LOGS STRUCTURÉS ──────────────────────────────────────────────────────

class NiveauLog(Enum):
    DEBUG   = 10
    INFO    = 20
    WARNING = 30
    ERROR   = 40
    CRITICAL= 50


@dataclass
class EntreeLog:
    timestamp:  float
    niveau:     NiveauLog
    message:    str
    service:    str
    trace_id:   Optional[str]
    span_id:    Optional[str]
    champs:     dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "ts":       round(self.timestamp, 3),
            "level":    self.niveau.name,
            "msg":      self.message,
            "service":  self.service,
            "trace_id": self.trace_id or "",
            "span_id":  self.span_id  or "",
            **self.champs,
        }


class Logger:
    """
    Logger structuré — chaque log est un objet JSON interrogeable.
    Corrélé avec les traces via trace_id et span_id.
    """
    def __init__(self, service: str,
                 backend: Optional["BackendLog"] = None):
        self.service = service
        self._backend = backend or BackendLog()
        self._contexte: dict = {}   # Champs toujours ajoutés (user_id, request_id...)

    def avec_contexte(self, **kwargs) -> "Logger":
        """Retourner un logger enrichi avec des champs supplémentaires."""
        l = Logger(self.service, self._backend)
        l._contexte = {**self._contexte, **kwargs}
        return l

    def _log(self, niveau: NiveauLog, message: str,
             trace_id: str = None, span_id: str = None, **kwargs):
        entree = EntreeLog(
            timestamp = time.time(),
            niveau    = niveau,
            message   = message,
            service   = self.service,
            trace_id  = trace_id or self._contexte.get("trace_id"),
            span_id   = span_id  or self._contexte.get("span_id"),
            champs    = {**self._contexte, **kwargs},
        )
        self._backend.ecrire(entree)

    def debug(self, msg: str, **kw):    self._log(NiveauLog.DEBUG,    msg, **kw)
    def info(self, msg: str, **kw):     self._log(NiveauLog.INFO,     msg, **kw)
    def warning(self, msg: str, **kw):  self._log(NiveauLog.WARNING,  msg, **kw)
    def error(self, msg: str, **kw):    self._log(NiveauLog.ERROR,    msg, **kw)
    def critical(self, msg: str, **kw): self._log(NiveauLog.CRITICAL, msg, **kw)


class BackendLog:
    """Stockage et recherche des logs structurés (ELK/Loki simplifié)."""
    def __init__(self):
        self._logs: list[EntreeLog] = []
        self._lock = threading.Lock()

    def ecrire(self, entree: EntreeLog):
        with self._lock:
            self._logs.append(entree)

    def chercher(self, niveau_min: NiveauLog = NiveauLog.DEBUG,
                 service: str = None,
                 trace_id: str = None,
                 contient: str = None,
                 champ: tuple = None,   # (nom, valeur)
                 depuis_s: float = None) -> list[EntreeLog]:
        """Recherche multi-critères — comme Kibana ou Grafana Loki."""
        maintenant = time.time()
        with self._lock:
            resultats = []
            for log in self._logs:
                if log.niveau.value < niveau_min.value:
                    continue
                if service and log.service != service:
                    continue
                if trace_id and log.trace_id != trace_id:
                    continue
                if contient and contient.lower() not in log.message.lower():
                    continue
                if champ:
                    nom, val = champ
                    if log.champs.get(nom) != val:
                        continue
                if depuis_s and log.timestamp < maintenant - depuis_s:
                    continue
                resultats.append(log)
        return resultats

    def stats(self) -> dict:
        with self._lock:
            par_niveau   = defaultdict(int)
            par_service  = defaultdict(int)
            for log in self._logs:
                par_niveau[log.niveau.name] += 1
                par_service[log.service]    += 1
        return {"total": len(self._logs),
                "par_niveau":  dict(par_niveau),
                "par_service": dict(par_service)}


# ─── 3. TRACES DISTRIBUÉES ───────────────────────────────────────────────────

@dataclass
class Span:
    """
    Un span = une opération dans la trace.
    Contient : service, opération, durée, parent, tags, événements.
    """
    trace_id:   str
    span_id:    str
    parent_id:  Optional[str]
    service:    str
    operation:  str
    debut:      float
    fin:        Optional[float] = None
    tags:       dict = field(default_factory=dict)
    evenements: list = field(default_factory=list)
    erreur:     bool = False

    @property
    def duree_ms(self) -> float:
        if self.fin is None:
            return (time.time() - self.debut) * 1000
        return (self.fin - self.debut) * 1000

    def terminer(self, erreur: bool = False):
        self.fin    = time.time()
        self.erreur = erreur

    def ajouter_evenement(self, nom: str, **attrs):
        self.evenements.append({"nom": nom, "ts": time.time(), **attrs})

    def ajouter_tag(self, cle: str, valeur: Any):
        self.tags[cle] = valeur


@dataclass
class Trace:
    """Ensemble de spans reliés par un trace_id."""
    trace_id: str
    spans:    list[Span] = field(default_factory=list)

    def span_racine(self) -> Optional[Span]:
        return next((s for s in self.spans if s.parent_id is None), None)

    def duree_totale_ms(self) -> float:
        racine = self.span_racine()
        if not racine:
            return 0.0
        return racine.duree_ms

    def chemin(self) -> list[str]:
        """Reconstruire le chemin service→service."""
        racine = self.span_racine()
        if not racine:
            return []
        chemin = [racine.service]
        by_parent = defaultdict(list)
        for s in self.spans:
            if s.parent_id:
                by_parent[s.parent_id].append(s)
        def parcourir(span_id, depth=0):
            for enfant in sorted(by_parent[span_id],
                                  key=lambda s: s.debut):
                chemin.append("  " * depth + "→ " + enfant.service)
                parcourir(enfant.span_id, depth + 1)
        parcourir(racine.span_id)
        return chemin

    def spans_lents(self, seuil_ms: float = 100) -> list[Span]:
        return [s for s in self.spans if s.duree_ms > seuil_ms]

    def spans_erreur(self) -> list[Span]:
        return [s for s in self.spans if s.erreur]


class Traceur:
    """
    Traceur OpenTelemetry-compatible.
    Crée des spans, les propager via les headers HTTP (W3C Trace Context).
    """
    def __init__(self, service: str, backend: "BackendTrace"):
        self.service = service
        self._backend = backend

    def demarrer_trace(self, operation: str, **tags) -> Span:
        """Créer une nouvelle trace (requête entrante sans contexte)."""
        import hashlib, uuid
        trace_id = hashlib.sha1(str(uuid.uuid4()).encode()).hexdigest()[:16]
        span_id  = hashlib.sha1((trace_id + operation).encode()).hexdigest()[:8]
        span = Span(trace_id=trace_id, span_id=span_id, parent_id=None,
                    service=self.service, operation=operation,
                    debut=time.time(), tags=tags)
        self._backend.enregistrer(span)
        return span

    def continuer_trace(self, trace_id: str, parent_id: str,
                        operation: str, **tags) -> Span:
        """Continuer une trace existante (requête inter-service)."""
        import hashlib
        span_id = hashlib.sha1(
            (trace_id + parent_id + operation).encode()
        ).hexdigest()[:8]
        span = Span(trace_id=trace_id, span_id=span_id, parent_id=parent_id,
                    service=self.service, operation=operation,
                    debut=time.time(), tags=tags)
        self._backend.enregistrer(span)
        return span


class BackendTrace:
    """Stockage et analyse des traces (Jaeger/Zipkin simplifié)."""
    def __init__(self):
        self._spans: list[Span] = []
        self._lock = threading.Lock()

    def enregistrer(self, span: Span):
        with self._lock:
            self._spans.append(span)

    def trace(self, trace_id: str) -> Optional[Trace]:
        with self._lock:
            spans = [s for s in self._spans if s.trace_id == trace_id]
        return Trace(trace_id=trace_id, spans=spans) if spans else None

    def chercher(self, service: str = None,
                 operation: str = None,
                 duree_min_ms: float = None,
                 avec_erreur: bool = None,
                 depuis_s: float = None) -> list[Trace]:
        """Recherche de traces par critères."""
        maintenant = time.time()
        with self._lock:
            tous_spans = list(self._spans)

        # Grouper par trace_id
        par_trace: dict[str, list] = defaultdict(list)
        for span in tous_spans:
            par_trace[span.trace_id].append(span)

        traces = []
        for tid, spans in par_trace.items():
            t = Trace(trace_id=tid, spans=spans)
            if service and not any(s.service == service for s in spans):
                continue
            if operation and not any(s.operation == operation for s in spans):
                continue
            if duree_min_ms and t.duree_totale_ms() < duree_min_ms:
                continue
            if avec_erreur is not None:
                has_err = any(s.erreur for s in spans)
                if avec_erreur != has_err:
                    continue
            if depuis_s:
                if not any(s.debut >= maintenant - depuis_s for s in spans):
                    continue
            traces.append(t)
        return traces

    def stats(self) -> dict:
        with self._lock:
            spans = list(self._spans)
        par_service = defaultdict(int)
        erreurs     = 0
        durees      = []
        for s in spans:
            par_service[s.service] += 1
            if s.erreur: erreurs += 1
            if s.fin: durees.append(s.duree_ms)
        durees.sort()
        return {
            "total_spans":  len(spans),
            "erreurs":      erreurs,
            "par_service":  dict(par_service),
            "duree_p50_ms": round(durees[len(durees)//2], 1) if durees else 0,
            "duree_p99_ms": round(durees[int(len(durees)*0.99)], 1) if durees else 0,
        }


# ─── 4. RÈGLES D'ALERTE ──────────────────────────────────────────────────────

@dataclass
class RegleAlerte:
    nom:          str
    condition:    Callable[[], float]   # Retourne la valeur à évaluer
    seuil:        float
    comparateur:  str                  # ">", "<", ">=", "<="
    severite:     str                  # "warning", "critical"
    description:  str = ""

    def evaluer(self) -> tuple[bool, float]:
        valeur = self.condition()
        if self.comparateur == ">":  declenche = valeur > self.seuil
        elif self.comparateur == "<": declenche = valeur < self.seuil
        elif self.comparateur == ">=": declenche = valeur >= self.seuil
        else: declenche = valeur <= self.seuil
        return declenche, valeur


class GestionnaireAlertes:
    def __init__(self):
        self._regles: list[RegleAlerte] = []
        self._historique: list[dict] = []

    def ajouter(self, regle: RegleAlerte):
        self._regles.append(regle)

    def evaluer_tout(self) -> list[dict]:
        alertes = []
        for regle in self._regles:
            try:
                declenche, valeur = regle.evaluer()
                if declenche:
                    alerte = {
                        "nom":        regle.nom,
                        "severite":   regle.severite,
                        "valeur":     round(valeur, 2),
                        "seuil":      regle.seuil,
                        "description":regle.description,
                        "ts":         time.time(),
                    }
                    alertes.append(alerte)
                    self._historique.append(alerte)
            except Exception:
                pass
        return alertes

    def historique(self, n: int = 20) -> list[dict]:
        return self._historique[-n:]
