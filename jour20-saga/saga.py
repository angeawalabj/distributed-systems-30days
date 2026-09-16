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
    EN_COURS        = "en_cours"
    SUCCES          = "succes"
    ECHOUEE         = "echouee"         # Échec sans compensation nécessaire (rien n'avait réussi)
    EN_COMPENSATION = "en_compensation"
    COMPENSEE       = "compensee"       # Échec avec compensations exécutées (rollback logique)


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
        self._derniere_activite = time.time()

    def publier(self, type_ev: str, payload: dict, source: str = ""):
        ev = {"id": uuid.uuid4().hex[:8], "type": type_ev,
              "payload": payload, "emetteur": source, "ts": time.time()}
        with self._lock:
            self._journal.append(ev)
            self._derniere_activite = time.time()
            handlers = list(self._handlers.get(type_ev, []))
        for h in handlers:
            threading.Thread(target=h, args=(ev,), daemon=True).start()

    def s_abonner(self, type_ev: str, handler: Callable):
        with self._lock:
            self._handlers[type_ev].append(handler)

    def attendre_silence(self, timeout: float = 2.0, silence: float = 0.15):
        """Bloque jusqu'à ce qu'aucun événement n'ait été publié depuis `silence` secondes."""
        fin = time.time() + timeout
        while time.time() < fin:
            with self._lock:
                derniere = self._derniere_activite
            if time.time() - derniere > silence:
                return
            time.sleep(0.02)

    @property
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
    def __init__(self, nom: str, latence_ms: float = 50.0, taux_echec: float = 0.0):
        self.nom         = nom
        self.latence     = latence_ms / 1000
        self.taux_echec  = taux_echec
        self._etat:  dict[str, Any] = {}   # état local du service (sa propre DB)
        self._lock   = threading.Lock()
        self.stats   = defaultdict(int)

    def executer(self, commande: str, params: dict,
                 forcer_erreur: bool = False) -> ResultatService:
        """Exécute une action locale (transaction locale)."""
        time.sleep(self.latence * random.uniform(0.8, 1.2))
        if forcer_erreur or random.random() < self.taux_echec:
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

class SagaCommandeChoreography:
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

    L'échec est injecté au niveau du ServiceSimule lui-même
    (taux_echec=1.0), pas par un flag externe : chaque service ne
    connaît que sa propre fiabilité, exactement comme en production.
    """

    def __init__(self, bus: BusEvenements,
                 inventory: ServiceSimule,
                 payment:   ServiceSimule,
                 shipment:  ServiceSimule,
                 notif:     ServiceSimule):
        self.bus      = bus
        self.services = {"inventory": inventory, "payment": payment,
                         "shipment": shipment, "notif": notif}
        self._sagas:  dict[str, dict] = {}   # commande_id → état
        self._lock    = threading.Lock()

        # Câblage des handlers
        bus.s_abonner("COMMANDE_CREEE",       self._on_commande_creee)
        bus.s_abonner("STOCK_RESERVE",         self._on_stock_reserve)
        bus.s_abonner("STOCK_RESERVE_ECHEC",   self._on_stock_echec)
        bus.s_abonner("PAIEMENT_EFFECTUE",     self._on_paiement_ok)
        bus.s_abonner("PAIEMENT_ECHOUE",       self._on_paiement_echec)
        bus.s_abonner("EXPEDITION_CREEE",      self._on_expedition_ok)
        bus.s_abonner("EXPEDITION_ECHOUEE",    self._on_expedition_echec)

    def lancer(self, commande_id: str, produit: str, montant: float, user_id: str) -> str:
        with self._lock:
            self._sagas[commande_id] = {
                "etat": EtatSaga.EN_COURS, "etapes": [],
                "debut": time.time(), "produit": produit,
                "montant": montant, "user_id": user_id,
            }
        self.bus.publier("COMMANDE_CREEE",
                         {"commande_id": commande_id, "produit": produit,
                          "montant": montant, "user_id": user_id},
                         source="order-svc")
        return commande_id

    def _appartient(self, commande_id: str) -> bool:
        """Un bus peut être partagé par plusieurs managers (plusieurs saga
        « pipelines » en parallèle) : chaque manager ne doit réagir qu'aux
        commandes qu'il a lui-même lancées via `lancer()`."""
        with self._lock:
            return commande_id in self._sagas

    def _on_commande_creee(self, ev):
        p = ev["payload"]
        if not self._appartient(p["commande_id"]):
            return
        res = self.services["inventory"].executer(
            "RESERVER_STOCK", {"produit": p["produit"]})
        self._log(p["commande_id"], "inventory", res.succes)
        if res.succes:
            self.bus.publier("STOCK_RESERVE",
                             {**p, "ref_stock": res.data["ref"]}, source="inventory-svc")
        else:
            self._finir(p["commande_id"], EtatSaga.ECHOUEE)
            self.bus.publier("STOCK_RESERVE_ECHEC",
                             {**p, "erreur": res.erreur}, source="inventory-svc")

    def _on_stock_reserve(self, ev):
        p = ev["payload"]
        if not self._appartient(p["commande_id"]):
            return
        res = self.services["payment"].executer(
            "DEBITER_CARTE", {"montant": p["montant"]})
        self._log(p["commande_id"], "payment", res.succes)
        if res.succes:
            self.bus.publier("PAIEMENT_EFFECTUE",
                             {**p, "ref_paiement": res.data["ref"]}, source="payment-svc")
        else:
            self.bus.publier("PAIEMENT_ECHOUE",
                             {**p, "erreur": res.erreur}, source="payment-svc")

    def _on_paiement_ok(self, ev):
        p = ev["payload"]
        if not self._appartient(p["commande_id"]):
            return
        res = self.services["shipment"].executer(
            "CREER_EXPEDITION", {"commande_id": p["commande_id"]})
        self._log(p["commande_id"], "shipment", res.succes)
        if res.succes:
            self.bus.publier("EXPEDITION_CREEE",
                             {**p, "ref_expedition": res.data["ref"]}, source="shipment-svc")
        else:
            self.bus.publier("EXPEDITION_ECHOUEE",
                             {**p, "erreur": res.erreur}, source="shipment-svc")

    def _on_expedition_ok(self, ev):
        p = ev["payload"]
        if not self._appartient(p["commande_id"]):
            return
        self.services["notif"].executer("ENVOYER_EMAIL", {"commande_id": p["commande_id"]})
        self._log(p["commande_id"], "notif", True)
        self._finir(p["commande_id"], EtatSaga.SUCCES)

    # ── Compensations ─────────────────────────────────────────────────────────

    def _on_stock_echec(self, ev):
        pass  # rien à compenser : c'était la première étape, déjà finalisé ci-dessus

    def _on_paiement_echec(self, ev):
        """Paiement échoué → libérer le stock (compensation)."""
        p = ev["payload"]
        if not self._appartient(p["commande_id"]):
            return
        with self._lock:
            if p["commande_id"] in self._sagas:
                self._sagas[p["commande_id"]]["etat"] = EtatSaga.EN_COMPENSATION
        self.services["inventory"].compenser("RESERVER_STOCK", {"produit": p.get("produit", "")})
        self._finir(p["commande_id"], EtatSaga.COMPENSEE)

    def _on_expedition_echec(self, ev):
        """Expédition échouée → rembourser + libérer stock."""
        p = ev["payload"]
        if not self._appartient(p["commande_id"]):
            return
        with self._lock:
            if p["commande_id"] in self._sagas:
                self._sagas[p["commande_id"]]["etat"] = EtatSaga.EN_COMPENSATION
        self.services["payment"].compenser("DEBITER_CARTE", {"montant": p.get("montant", 0)})
        self.services["inventory"].compenser("RESERVER_STOCK", {"produit": p.get("produit", "")})
        self._finir(p["commande_id"], EtatSaga.COMPENSEE)

    def _log(self, commande_id: str, service: str, succes: bool):
        with self._lock:
            if commande_id in self._sagas:
                self._sagas[commande_id]["etapes"].append(
                    {"etape": service, "succes": succes})

    def _finir(self, commande_id: str, etat_final: EtatSaga):
        with self._lock:
            if commande_id in self._sagas:
                self._sagas[commande_id]["etat"] = etat_final
                self._sagas[commande_id]["fin"] = time.time()

    def etat(self, commande_id: str) -> dict:
        with self._lock:
            return dict(self._sagas.get(commande_id, {}))


# ════════════════════════════════════════════════════════════════
# PATTERN 2 : ORCHESTRATION
# ════════════════════════════════════════════════════════════════

@dataclass
class EtapeSaga:
    """Une étape dans la séquence orchestrée, avec sa compensation."""
    nom:          str
    service:      ServiceSimule
    commande:     str
    compensation: Optional[str] = None   # nom de l'action compensatoire, ou None


class SagaOrchestrator:
    """
    Orchestrateur central : exécute les étapes en séquence,
    gère les compensations en cas d'échec.

    Avantage vs Choreography : la logique complète est ici,
    visible en un seul endroit. Chaque étape est explicitement
    définie avec sa compensation.

    En production : l'état de la saga est persistant (base de données)
    pour survivre aux redémarrages. Les étapes sont idempotentes.
    """

    def __init__(self, saga_id: str, etapes: list[EtapeSaga]):
        self.saga_id = saga_id
        self.etapes  = etapes
        self.journal: list[dict] = []
        self.debut:   Optional[float] = None
        self.fin:     Optional[float] = None

    def executer(self, payload: dict) -> bool:
        """
        Exécute la saga séquentiellement.
        Retourne True si toutes les étapes ont réussi, False sinon
        (auquel cas les compensations ont déjà été exécutées).
        """
        self.debut = time.time()
        ctx = dict(payload)
        etapes_ok: list[EtapeSaga] = []

        print(f"\n  ── Saga [{self.saga_id}] démarrée ──")

        for etape in self.etapes:
            t0  = time.perf_counter()
            res = etape.service.executer(etape.commande, ctx)
            dt  = (time.perf_counter() - t0) * 1000

            icone = "✅" if res.succes else "❌"
            print(f"  {icone} {etape.service.nom:<14} {etape.commande:<22} {dt:>6.0f}ms"
                  + (f"  ref={res.data.get('ref', '')[:6]}" if res.succes else f"  {res.erreur}"))

            self.journal.append({"etape": etape.nom, "service": etape.service.nom,
                                  "ok": res.succes, "duree_ms": dt})

            if res.succes:
                ctx.update(res.data)
                etapes_ok.append(etape)
            else:
                if etapes_ok:
                    print(f"\n  ⟳ Compensation (ordre inverse) :")
                for etape_a_comp in reversed(etapes_ok):
                    if etape_a_comp.compensation is None:
                        continue
                    t0c = time.perf_counter()
                    etape_a_comp.service.compenser(etape_a_comp.commande, ctx)
                    dtc = (time.perf_counter() - t0c) * 1000
                    print(f"  ↩  {etape_a_comp.service.nom:<14} {etape_a_comp.compensation:<22} {dtc:>5.0f}ms")
                    self.journal.append({"etape": f"{etape_a_comp.compensation} (compensation)",
                                          "service": etape_a_comp.service.nom,
                                          "ok": True, "duree_ms": dtc})
                self.fin = time.time()
                return False

        print(f"  ✅ Saga terminée avec succès")
        self.fin = time.time()
        return True

    def afficher(self):
        print(f"\n  ── Journal saga {self.saga_id} ({len(self.journal)} entrées) ──")
        for e in self.journal:
            icone = "✅" if e["ok"] and "(compensation)" not in e["etape"] else \
                    "↩️ " if "(compensation)" in e["etape"] else "❌"
            print(f"    {icone} {e['etape']}  ({e['duree_ms']:.0f}ms)")
