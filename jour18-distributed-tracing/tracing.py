"""
Jour 18 — Distributed Tracing (Jaeger/Zipkin)
===============================================
Problème : une requête /checkout prend 2.3 secondes.
Elle touche 5 microservices. Lequel est lent ?
Sans tracing : on ne sait pas. Logs dans 5 endroits différents,
aucune corrélation entre eux.

Solution : Distributed Tracing
  Chaque requête reçoit un trace_id unique à sa naissance.
  Ce trace_id est propagé dans les headers HTTP de tous les appels suivants.
  Chaque service crée un "span" : unité de travail horodatée avec :
    - trace_id : identifie la trace globale
    - span_id  : identifie ce span unique
    - parent_id: identifie le span parent (pour reconstruire l'arbre)
    - timestamps de début et fin
    - tags, logs, erreurs

  Un collecteur (Jaeger, Zipkin) agrège tous les spans et reconstruit
  la cascade complète. On voit exactement où le temps est perdu.

Standards :
  OpenTracing (déprécié) → OpenTelemetry (OTel, le standard actuel)
  W3C Trace Context : headers standardisés
    traceparent: 00-{trace_id}-{span_id}-{flags}
    tracestate : vendor-specific data

Propagation des headers :
  Client → [traceparent: 00-abc123-span1-01] → Service A
  Service A → [traceparent: 00-abc123-span2-01] → Service B
  Service A → [traceparent: 00-abc123-span3-01] → Service C (parallèle)
  Chacun crée son span avec parent_id = le span de l'appelant.

Visualisation (Jaeger UI) :
  /checkout  ←────────────────────────────── 2.3s ──────────────────────────────
    cart-svc     ←── 0.2s ──
    inventory    ←─────── 0.5s ──────
    payment      ←───────────────────────────── 1.8s ──────────────────────────
      payment-db ←──── 0.3s ────
      fraud-svc  ←─────────────────── 1.4s ────────────────────
    notif-svc    ←── 0.1s
"""

from __future__ import annotations
import time
import uuid
import threading
import random
import json
from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum
from collections import defaultdict


# ─── STATUT D'UN SPAN ────────────────────────────────────────────────────────

class StatutSpan(Enum):
    OK      = "ok"
    ERREUR  = "error"
    TIMEOUT = "timeout"


# ─── SPAN ────────────────────────────────────────────────────────────────────

@dataclass
class Span:
    """
    Unité de travail dans une trace distribuée.
    Équivalent d'un Span OpenTelemetry / Jaeger.
    """
    trace_id:    str          # ID global de la trace (partagé par tous les spans)
    span_id:     str          # ID unique de ce span
    parent_id:   Optional[str]  # ID du span parent (None = span racine)
    operation:   str          # Nom de l'opération (ex: "GET /checkout")
    service:     str          # Nom du service (ex: "api-gateway")
    debut:       float        # timestamp UNIX en secondes
    fin:         Optional[float] = None
    statut:      StatutSpan   = StatutSpan.OK
    tags:        dict         = field(default_factory=dict)
    logs:        list[dict]   = field(default_factory=list)
    erreur:      Optional[str] = None

    @property
    def duree_ms(self) -> float:
        if self.fin is None:
            return (time.time() - self.debut) * 1000
        return (self.fin - self.debut) * 1000

    def terminer(self, statut: StatutSpan = StatutSpan.OK,
                 erreur: str = None):
        self.fin    = time.time()
        self.statut = statut
        if erreur:
            self.erreur = erreur
            self.statut = StatutSpan.ERREUR

    def ajouter_tag(self, cle: str, valeur: Any):
        self.tags[cle] = valeur

    def ajouter_log(self, message: str, **kwargs):
        self.logs.append({"ts": time.time(), "msg": message, **kwargs})

    def to_dict(self) -> dict:
        return {
            "trace_id":  self.trace_id,
            "span_id":   self.span_id,
            "parent_id": self.parent_id,
            "operation": self.operation,
            "service":   self.service,
            "duree_ms":  round(self.duree_ms, 2),
            "statut":    self.statut.value,
            "tags":      self.tags,
            "erreur":    self.erreur,
        }


# ─── CONTEXTE DE PROPAGATION ─────────────────────────────────────────────────

@dataclass
class ContexteTrace:
    """
    Contexte de trace propagé dans les headers HTTP.
    Standard W3C Trace Context :
      traceparent: 00-{trace_id}-{span_id}-{flags}
    """
    trace_id:  str
    span_id:   str    # span_id du span PARENT (celui qui fait l'appel sortant)
    sampled:   bool = True

    def to_headers(self) -> dict[str, str]:
        flags = "01" if self.sampled else "00"
        return {
            "traceparent": f"00-{self.trace_id}-{self.span_id}-{flags}",
        }

    @staticmethod
    def from_headers(headers: dict) -> Optional["ContexteTrace"]:
        header = headers.get("traceparent", "")
        if not header:
            return None
        parts = header.split("-")
        if len(parts) < 4:
            return None
        return ContexteTrace(
            trace_id=parts[1],
            span_id=parts[2],
            sampled=(parts[3] == "01"),
        )

    @staticmethod
    def nouveau() -> "ContexteTrace":
        return ContexteTrace(
            trace_id=uuid.uuid4().hex,
            span_id=uuid.uuid4().hex[:16],
        )


# ─── TRACER LOCAL ────────────────────────────────────────────────────────────

class Tracer:
    """
    Tracer local à un service.
    Crée des spans, les enrichit, les envoie au collecteur.
    En prod : SDK OpenTelemetry avec export vers Jaeger/Zipkin/OTLP.
    """

    def __init__(self, service_nom: str, collecteur: "Collecteur"):
        self.service    = service_nom
        self.collecteur = collecteur
        self._local     = threading.local()  # Stockage du contexte actif par thread

    def demarrer_span(
        self,
        operation:  str,
        contexte:   Optional[ContexteTrace] = None,
        parent_span: Optional[Span] = None,
    ) -> Span:
        """
        Démarre un nouveau span.
        Si un contexte est fourni (appel entrant), utilise son trace_id.
        Sinon, crée une nouvelle trace (span racine).
        """
        if parent_span:
            trace_id  = parent_span.trace_id
            parent_id = parent_span.span_id
        elif contexte:
            trace_id  = contexte.trace_id
            parent_id = contexte.span_id
        else:
            trace_id  = uuid.uuid4().hex
            parent_id = None

        span = Span(
            trace_id  = trace_id,
            span_id   = uuid.uuid4().hex[:16],
            parent_id = parent_id,
            operation = operation,
            service   = self.service,
            debut     = time.time(),
        )
        return span

    def terminer_span(self, span: Span, statut: StatutSpan = StatutSpan.OK,
                      erreur: str = None):
        span.terminer(statut, erreur)
        self.collecteur.recevoir(span)

    def contexte_sortant(self, span: Span) -> ContexteTrace:
        """Contexte à injecter dans les headers des appels sortants."""
        return ContexteTrace(trace_id=span.trace_id, span_id=span.span_id)


# ─── COLLECTEUR ──────────────────────────────────────────────────────────────

class Collecteur:
    """
    Agrège les spans de tous les services et reconstruit les traces.
    En prod : Jaeger Collector, Zipkin Server, ou OTLP Collector.
    """

    def __init__(self):
        self._spans: dict[str, list[Span]] = defaultdict(list)  # trace_id → spans
        self._lock  = threading.RLock()
        self.stats  = {"spans_recus": 0, "traces": 0, "erreurs": 0}

    def recevoir(self, span: Span):
        with self._lock:
            self._spans[span.trace_id].append(span)
            self.stats["spans_recus"] += 1
            if span.statut == StatutSpan.ERREUR:
                self.stats["erreurs"] += 1
            if len(self._spans[span.trace_id]) == 1:
                self.stats["traces"] += 1

    def obtenir_trace(self, trace_id: str) -> list[Span]:
        with self._lock:
            return sorted(self._spans.get(trace_id, []), key=lambda s: s.debut)

    def toutes_traces(self) -> dict[str, list[Span]]:
        with self._lock:
            return {tid: sorted(spans, key=lambda s: s.debut)
                    for tid, spans in self._spans.items()}

    def afficher_trace(self, trace_id: str, largeur: int = 60):
        """
        Affiche la trace en cascade style Jaeger UI.
        Chaque span est représenté par une barre proportionnelle à sa durée.
        """
        spans = self.obtenir_trace(trace_id)
        if not spans:
            print(f"  Trace {trace_id} introuvable")
            return

        t_debut_global = min(s.debut for s in spans)
        t_fin_global   = max(s.fin or time.time() for s in spans)
        duree_totale   = (t_fin_global - t_debut_global) * 1000
        if duree_totale == 0:
            duree_totale = 1

        # Construire l'arbre des spans
        par_parent: dict[Optional[str], list[Span]] = defaultdict(list)
        par_id: dict[str, Span] = {}
        for s in spans:
            par_parent[s.parent_id].append(s)
            par_id[s.span_id] = s

        print(f"\n  Trace {trace_id[:16]}...  duree_totale={duree_totale:.0f}ms")
        print(f"  {'Service + Opération':<35} {'Durée':>8}  Cascade")
        print("  " + "─"*70)

        def afficher_recursif(span_id: Optional[str], profondeur: int = 0):
            enfants = sorted(
                par_parent.get(span_id, []),
                key=lambda s: s.debut
            )
            for span in enfants:
                debut_rel   = (span.debut - t_debut_global) * 1000
                duree_span  = span.duree_ms
                debut_pct   = debut_rel / duree_totale
                largeur_pct = duree_span / duree_totale
                offset   = max(0, int(debut_pct * largeur))
                nb_chars = max(1, int(largeur_pct * largeur))

                icone  = "✅" if span.statut == StatutSpan.OK else "❌"
                indent = "  " * profondeur
                label  = f"{indent}{icone} {span.service}/{span.operation}"
                barre  = " " * offset + "█" * nb_chars

                print(f"  {label:<35} {duree_span:>6.0f}ms  {barre}")
                afficher_recursif(span.span_id, profondeur + 1)

        # Les spans racines sont ceux dont le parent_id n'est PAS
        # l'ID d'un autre span de cette trace (parent externe = contexte entrant)
        ids_internes = set(par_id.keys())
        racines = [s for s in spans if s.parent_id not in ids_internes]
        racines.sort(key=lambda s: s.debut)
        for racine in racines:
            debut_rel   = (racine.debut - t_debut_global) * 1000
            duree_span  = racine.duree_ms
            debut_pct   = debut_rel / duree_totale
            largeur_pct = duree_span / duree_totale
            offset   = max(0, int(debut_pct * largeur))
            nb_chars = max(1, int(largeur_pct * largeur))
            icone  = "✅" if racine.statut == StatutSpan.OK else "❌"
            label  = f"{icone} {racine.service}/{racine.operation}"
            barre  = " " * offset + "█" * nb_chars
            print(f"  {label:<35} {duree_span:>6.0f}ms  {barre}")
            afficher_recursif(racine.span_id, profondeur=1)

    def analyser_trace(self, trace_id: str) -> dict:
        """Analyse une trace et retourne les métriques clés."""
        spans = self.obtenir_trace(trace_id)
        if not spans:
            return {}

        racines   = [s for s in spans if s.parent_id is None]
        erreurs   = [s for s in spans if s.statut == StatutSpan.ERREUR]
        par_svc   = defaultdict(list)
        for s in spans:
            par_svc[s.service].append(s)

        t_debut_global = min(s.debut for s in spans)
        t_fin_global   = max(s.fin or time.time() for s in spans)

        return {
            "trace_id":     trace_id[:16],
            "duree_ms":     (t_fin_global - t_debut_global) * 1000,
            "nb_spans":     len(spans),
            "nb_services":  len(par_svc),
            "nb_erreurs":   len(erreurs),
            "service_le_plus_lent": max(
                par_svc.items(),
                key=lambda kv: sum(s.duree_ms for s in kv[1])
            )[0] if par_svc else None,
            "par_service":  {
                svc: {
                    "nb_spans":  len(sps),
                    "duree_totale_ms": sum(s.duree_ms for s in sps),
                    "duree_max_ms": max(s.duree_ms for s in sps),
                }
                for svc, sps in par_svc.items()
            },
        }


# ─── SERVICE SIMULÉ ──────────────────────────────────────────────────────────

class MicroService:
    """
    Simule un microservice qui instrumente ses opérations avec des spans.
    """

    def __init__(self, nom: str, collecteur: Collecteur,
                 latence_ms: float = 20.0, taux_erreur: float = 0.0):
        self.nom          = nom
        self.tracer       = Tracer(nom, collecteur)
        self.latence      = latence_ms / 1000
        self.taux_erreur  = taux_erreur
        self._dependances: dict[str, "MicroService"] = {}

    def ajouter_dependance(self, nom: str, service: "MicroService"):
        self._dependances[nom] = service

    def traiter(
        self,
        operation:   str,
        ctx_entrant: Optional[ContexteTrace] = None,
        appels:      list[tuple[str, str, bool]] = None,
        latence_override: float = None,
    ) -> tuple[Span, Any]:
        """
        Traite une requête :
          1. Crée un span enfant (ou racine si pas de contexte)
          2. Fait les appels aux dépendances (séquentiels ou parallèles)
          3. Termine le span

        appels = [(nom_svc, operation, parallele), ...]
        """
        span = self.tracer.demarrer_span(operation, contexte=ctx_entrant)
        span.ajouter_tag("http.method", "GET")
        span.ajouter_tag("service.version", "v1")

        # Latence propre du service
        latence = latence_override if latence_override is not None else self.latence
        time.sleep(latence * random.uniform(0.8, 1.2))

        # Appels aux dépendances
        if appels:
            ctx_sortant = self.tracer.contexte_sortant(span)
            parallel_appels = [(n, o) for n, o, p in appels if p]
            seq_appels      = [(n, o) for n, o, p in appels if not p]

            # Appels parallèles
            if parallel_appels:
                resultats = {}
                lock = threading.Lock()

                def appel_parallele(nom_dep, op_dep):
                    svc = self._dependances.get(nom_dep)
                    if svc:
                        ctx_child = ContexteTrace(
                            trace_id=ctx_sortant.trace_id,
                            span_id=span.span_id
                        )
                        child_span, _ = svc.traiter(op_dep, ctx_entrant=ctx_child)
                        with lock:
                            resultats[nom_dep] = child_span

                threads = [
                    threading.Thread(target=appel_parallele, args=(n, o), daemon=True)
                    for n, o in parallel_appels
                ]
                for t in threads: t.start()
                for t in threads: t.join()

            # Appels séquentiels
            for nom_dep, op_dep in seq_appels:
                svc = self._dependances.get(nom_dep)
                if svc:
                    ctx_child = ContexteTrace(
                        trace_id=ctx_sortant.trace_id,
                        span_id=span.span_id
                    )
                    svc.traiter(op_dep, ctx_entrant=ctx_child)

        # Simuler une erreur aléatoire
        if random.random() < self.taux_erreur:
            self.tracer.terminer_span(span, StatutSpan.ERREUR,
                                      erreur=f"Internal Server Error in {self.nom}")
        else:
            self.tracer.terminer_span(span)

        return span, None
