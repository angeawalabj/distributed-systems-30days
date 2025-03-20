"""
Jour 20 — Saga Pattern (Choreography + Orchestration)
======================================================
Problème : une commande e-commerce touche 4 services indépendants.
  1. inventory-svc  : réserver le stock
  2. payment-svc    : débiter la carte
  3. shipment-svc   : créer l'expédition
  4. notif-svc      : envoyer l'email de confirmation

Si shipment-svc échoue APRÈS que payment-svc a débité :
  → Chaque service a sa propre DB (pas de transaction ACID globale)
  → ROLLBACK distribué impossible (2PC trop lent et fragile)
  → Il faut des ACTIONS COMPENSATOIRES explicitement codées

Solution : Saga
  Une séquence de transactions locales.
  Chaque étape produit un événement → déclenche la suivante.
  En cas d'échec → compensations dans l'ordre inverse.

Deux patterns :

  1. Choreography : pas de coordinateur. Chaque service écoute les
     événements et sait quoi faire (+ compensation si besoin).
     ✅ Découplé  ❌ Logique dispersée, difficile à tracer

  2. Orchestration : un Saga Orchestrator central envoie des commandes,
     attend les réponses, décide des compensations.
     ✅ Logique centralisée, facile à comprendre
     ❌ Coordinateur = SPOF potentiel (mais idempotent + persistent)
"""

from __future__ import annotations
import time
import uuid
import threading
import random
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
from enum import Enum
from collections import defaultdict


# ─── ÉTATS D'UNE SAGA ────────────────────────────────────────────────────────

class EtatSaga(Enum):
    EN_COURS     = "en_cours"
    COMPLETEE    = "completee"
    EN_COMPENSATION = "en_compensation"
    COMPENSEE    = "compensee"      # Rollback logique complet
    ECHOUEE      = "echouee"        # Échec sans compensation possible


# ─── BUS D'ÉVÉNEMENTS (simplifié) ────────────────────────────────────────────

class BusEvenements:
    """
    Bus publish/subscribe en mémoire.
    En production : Kafka, RabbitMQ, AWS SNS/SQS.
    """
    def __init__(self):
        self._handlers: dict[str, list[Callable]] = defaultdict(list)
        self._journal:  list[dict] = []
        self._lock      = threading.Lock()

    def publier(self, type_ev: str, payload: dict, source: str = ""):
        ev = {"id": uuid.uuid4().hex[:8], "type": type_ev,
              "payload": payload, "source": source, "ts": time.time()}
        with self._lock:
            self._journal.append(ev)
            handlers = list(self._handlers.get(type_ev, []))
        for h in handlers:
            threading.Thread(target=h, args=(ev,), daemon=True).start()

    def s_abonner(self, type_ev: str, handler: Callable):
        with self._lock:
            self._handlers[type_ev].append(handler)

    def journal(self) -> list[dict]:
        with self._lock:
            return list(self._journal)


# ─── SERVICE SIMULÉ ──────────────────────────────────────────────────────────

@dataclass
class ResultatService:
    succes:    bool
    data:      dict = field(default_factory=dict)
    erreur:    str  = ""


class ServiceSimule:
    """
    Simule un microservice avec latence, taux d'erreur et compensation.
    """
    def __init__(self, nom: str, latence_ms: float = 50.0, taux_erreur: float = 0.0):
        self.nom         = nom
        self.latence     = latence_ms / 1000
        self.taux_erreur = taux_erreur
        self._etat:  dict[str, Any] = {}   # état local du service (sa propre DB)
        self._lock   = threading.Lock()
        self.stats   = defaultdict(int)

    def executer(self, commande: str, params: dict,
                 forcer_erreur: bool = False) -> ResultatService:
        """Exécute une action locale (transaction locale)."""
        time.sleep(self.latence * random.uniform(0.8, 1.2))
        if forcer_erreur or random.random() < self.taux_erreur:
            self.stats["echecs"] += 1
            return ResultatService(False, erreur=f"{self.nom}: {commande} échoué")
        with self._lock:
            self._etat[commande] = params
        self.stats["succes"] += 1
        return ResultatService(True, data={"ref": uuid.uuid4().hex[:8], **params})

    def compenser(self, commande: str, params: dict) -> ResultatService:
        """Annule l'action (transaction compensatoire)."""
        time.sleep(self.latence * 0.5)
        with self._lock:
            self._etat.pop(commande, None)
        self.stats["compensations"] += 1
        return ResultatService(True, data={"compensé": commande})


# ════════════════════════════════════════════════════════════════
# PATTERN 1 : CHOREOGRAPHY
# ════════════════════════════════════════════════════════════════

class SagaChoreography:
    """
    Saga chorégraphiée : pas de coordinateur central.
    Chaque service réagit aux événements et publie le suivant.

    Flux nominal :
      COMMANDE_CREEE
        → inventory: STOCK_RESERVE
          → payment: PAIEMENT_EFFECTUE
            → shipment: EXPEDITION_CREEE
              → notif: CONFIRMATION_ENVOYEE → FIN ✅

    Flux compensatoire (ex: échec payment) :
      COMMANDE_CREEE → STOCK_RESERVE → PAIEMENT_ECHOUE
        → inventory: STOCK_LIBERE → COMMANDE_ANNULEE
    """

    def __init__(self, bus: BusEvenements,
                 inventory: ServiceSimule,
                 payment:   ServiceSimule,
                 shipment:  ServiceSimule,
                 notif:     ServiceSimule):
        self.bus      = bus
        self.services = {"inventory": inventory, "payment": payment,
                         "shipment": shipment, "notif": notif}
        self._sagas:  dict[str, dict] = {}   # saga_id → état
        self._lock    = threading.Lock()

        # Câblage des handlers
        bus.s_abonner("COMMANDE_CREEE",       self._on_commande_creee)
        bus.s_abonner("STOCK_RESERVE",         self._on_stock_reserve)
        bus.s_abonner("STOCK_RESERVE_ECHEC",   self._on_stock_echec)
        bus.s_abonner("PAIEMENT_EFFECTUE",     self._on_paiement_ok)
        bus.s_abonner("PAIEMENT_ECHOUE",       self._on_paiement_echec)
        bus.s_abonner("EXPEDITION_CREEE",      self._on_expedition_ok)
        bus.s_abonner("EXPEDITION_ECHOUEE",    self._on_expedition_echec)

    def demarrer(self, commande_id: str, articles: list, montant: float,
                 forcer_echec_a: str = None) -> str:
        with self._lock:
            self._sagas[commande_id] = {
                "etat": EtatSaga.EN_COURS, "etapes": [],
                "forcer_echec": forcer_echec_a, "montant": montant
            }
        self.bus.publier("COMMANDE_CREEE",
                         {"commande_id": commande_id, "articles": articles,
                          "montant": montant, "forcer_echec": forcer_echec_a},
                         source="order-svc")
        return commande_id

    def _on_commande_creee(self, ev):
        p = ev["payload"]
        forcer = p.get("forcer_echec") == "inventory"
        res = self.services["inventory"].executer(
            "RESERVER_STOCK", {"articles": p["articles"]}, forcer_erreur=forcer)
        self._log(p["commande_id"], "inventory", res.succes)
        if res.succes:
            self.bus.publier("STOCK_RESERVE",
                             {**p, "ref_stock": res.data["ref"]}, source="inventory-svc")
        else:
            self.bus.publier("STOCK_RESERVE_ECHEC",
                             {**p, "erreur": res.erreur}, source="inventory-svc")

    def _on_stock_reserve(self, ev):
        p = ev["payload"]
        forcer = p.get("forcer_echec") == "payment"
        res = self.services["payment"].executer(
            "DEBITER_CARTE", {"montant": p["montant"]}, forcer_erreur=forcer)
        self._log(p["commande_id"], "payment", res.succes)
        if res.succes:
            self.bus.publier("PAIEMENT_EFFECTUE",
                             {**p, "ref_paiement": res.data["ref"]}, source="payment-svc")
        else:
            self.bus.publier("PAIEMENT_ECHOUE",
                             {**p, "erreur": res.erreur}, source="payment-svc")

    def _on_paiement_ok(self, ev):
        p = ev["payload"]
        forcer = p.get("forcer_echec") == "shipment"
        res = self.services["shipment"].executer(
            "CREER_EXPEDITION", {"commande_id": p["commande_id"]}, forcer_erreur=forcer)
        self._log(p["commande_id"], "shipment", res.succes)
        if res.succes:
            self.bus.publier("EXPEDITION_CREEE",
                             {**p, "ref_expedition": res.data["ref"]}, source="shipment-svc")
        else:
            self.bus.publier("EXPEDITION_ECHOUEE",
                             {**p, "erreur": res.erreur}, source="shipment-svc")

    def _on_expedition_ok(self, ev):
        p = ev["payload"]
        self.services["notif"].executer("ENVOYER_EMAIL", {"commande_id": p["commande_id"]})
        self._log(p["commande_id"], "notif", True)
        with self._lock:
            if p["commande_id"] in self._sagas:
                self._sagas[p["commande_id"]]["etat"] = EtatSaga.COMPLETEE

    # ── Compensations ─────────────────────────────────────────────────────────

    def _on_stock_echec(self, ev):
        p = ev["payload"]
        with self._lock:
            if p["commande_id"] in self._sagas:
                self._sagas[p["commande_id"]]["etat"] = EtatSaga.COMPENSEE

    def _on_paiement_echec(self, ev):
        """Paiement échoué → libérer le stock (compensation)."""
        p = ev["payload"]
        with self._lock:
            if p["commande_id"] in self._sagas:
                self._sagas[p["commande_id"]]["etat"] = EtatSaga.EN_COMPENSATION
        self.services["inventory"].compenser("RESERVER_STOCK", {"articles": p.get("articles", [])})
        with self._lock:
            if p["commande_id"] in self._sagas:
                self._sagas[p["commande_id"]]["etat"] = EtatSaga.COMPENSEE

    def _on_expedition_echec(self, ev):
        """Expédition échouée → rembourser + libérer stock."""
        p = ev["payload"]
        with self._lock:
            if p["commande_id"] in self._sagas:
                self._sagas[p["commande_id"]]["etat"] = EtatSaga.EN_COMPENSATION
        self.services["payment"].compenser("DEBITER_CARTE", {"montant": p.get("montant", 0)})
        self.services["inventory"].compenser("RESERVER_STOCK", {"articles": p.get("articles", [])})
        with self._lock:
            if p["commande_id"] in self._sagas:
                self._sagas[p["commande_id"]]["etat"] = EtatSaga.COMPENSEE

    def _log(self, commande_id: str, service: str, ok: bool):
        with self._lock:
            if commande_id in self._sagas:
                self._sagas[commande_id]["etapes"].append(
                    {"service": service, "ok": ok, "ts": time.time()})

    def etat(self, commande_id: str) -> dict:
        with self._lock:
            return dict(self._sagas.get(commande_id, {}))


# ════════════════════════════════════════════════════════════════
# PATTERN 2 : ORCHESTRATION
# ════════════════════════════════════════════════════════════════

@dataclass
class EtapeSaga:
    """Une étape dans la séquence orchestrée."""
    nom:         str
    service:     str
    commande:    str
    params_fn:   Callable[[dict], dict]   # construit les params depuis le contexte
    compenser_fn: Optional[Callable[[dict], tuple[str, dict]]] = None


class SagaOrchestrator:
    """
    Orchestrateur central : exécute les étapes en séquence,
    gère les compensations en cas d'échec.

    Avantage vs Choreography : la logique complète est ici,
    visible en un seul endroit. Chaque étape est explicitement
    définie avec sa compensation.

    En production : l'état de la saga est persistent (base de données)
    pour survivre aux redémarrages. Les étapes sont idempotentes.
    """

    def __init__(self, nom: str, etapes: list[EtapeSaga],
                 services: dict[str, ServiceSimule]):
        self.nom      = nom
        self.etapes   = etapes
        self.services = services

    def executer(self, contexte: dict,
                 forcer_echec_a: str = None) -> dict:
        """
        Exécute la saga séquentiellement.
        Retourne le résultat avec l'état final et les compensations effectuées.
        """
        saga_id      = uuid.uuid4().hex[:8]
        ctx          = dict(contexte)
        etapes_ok:   list[EtapeSaga] = []
        journal      = []

        print(f"\n  ── Saga {self.nom} [{saga_id}] démarré ──")

        for etape in self.etapes:
            params      = etape.params_fn(ctx)
            forcer      = (forcer_echec_a == etape.service)
            svc         = self.services[etape.service]

            t0  = time.perf_counter()
            res = svc.executer(etape.commande, params, forcer_erreur=forcer)
            dt  = (time.perf_counter() - t0) * 1000

            icone = "✅" if res.succes else "❌"
            print(f"  {icone} {etape.service:<14} {etape.commande:<22} {dt:>6.0f}ms"
                  + (f"  ref={res.data.get('ref','')[:6]}" if res.succes else f"  {res.erreur}"))

            journal.append({"etape": etape.nom, "service": etape.service,
                             "ok": res.succes, "duree_ms": dt})

            if res.succes:
                ctx.update(res.data)
                etapes_ok.append(etape)
            else:
                # Compensation dans l'ordre inverse
                print(f"\n  ⟳ Compensation (ordre inverse) :")
                compensations = []
                for etape_a_comp in reversed(etapes_ok):
                    if etape_a_comp.compenser_fn is None:
                        continue
                    svc_comp, params_comp = etape_a_comp.compenser_fn(ctx)
                    t0c = time.perf_counter()
                    self.services[svc_comp].compenser(etape_a_comp.commande, params_comp)
                    dtc = (time.perf_counter() - t0c) * 1000
                    print(f"  ↩  {svc_comp:<14} COMPENSER_{etape_a_comp.commande:<16} {dtc:>5.0f}ms")
                    compensations.append(svc_comp)

                return {"saga_id": saga_id, "etat": EtatSaga.COMPENSEE,
                        "journal": journal, "compensations": compensations,
                        "echec_a": etape.service}

        print(f"  ✅ Saga terminée avec succès")
        return {"saga_id": saga_id, "etat": EtatSaga.COMPLETEE,
                "journal": journal, "compensations": [], "ctx": ctx}
