"""
Jour 19 — Event Sourcing
==========================
Approche traditionnelle (CRUD) :
  UPDATE accounts SET balance = 900 WHERE id = 42
  → On sait l'état ACTUEL, mais pas comment on y est arrivé.
  → Impossible de savoir : "qui a débité 100€ et quand ?"
  → Impossible de reconstruire l'état à un instant T passé.

Event Sourcing :
  Au lieu de stocker l'état, on stocke les ÉVÉNEMENTS qui l'ont produit.
  L'état est une PROJECTION des événements.

  append(AccountDebited { account_id=42, amount=100, ts=... })
  append(AccountCredited { account_id=42, amount=50,  ts=... })
  → balance = 0 + (-100) + 50 = -50

  Avantages :
    ✅ Audit trail complet et immuable
    ✅ Time-travel : état à n'importe quel instant passé
    ✅ Replay : reconstruire des projections différentes depuis les mêmes événements
    ✅ Debug : rejouer exactement ce qui s'est passé
    ✅ CQRS naturel : écriture = append, lecture = projection

  Inconvénients :
    ❌ L'état courant nécessite de rejouer tous les événements → lent
    ❌ Snapshots nécessaires pour grandes séquences
    ❌ Évolution du schéma des événements (upcasting)
    ❌ Complexité accrue

Terminologie :
  Event      : fait immuable qui s'est produit ("CompteDebite", "CommandePassee")
  Aggregate  : entité métier qui produit et consomme des événements
  Projection : vue calculée depuis les événements (balance, liste des commandes)
  Snapshot   : état agrégé à un instant T pour accélérer la reconstruction
  Stream     : séquence d'événements d'un aggregate (tous les events de account-42)

Exemples en prod :
  Martin Fowler → a popularisé le pattern (2005)
  Greg Young    → Event Sourcing + CQRS
  EventStoreDB  → base de données dédiée
  Axon Framework → Java Event Sourcing/CQRS
  Kafka         → souvent utilisé comme event log (mais pas un event store pur)
"""

from __future__ import annotations
import time
import uuid
import json
import threading
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Type, Callable
from enum import Enum
from collections import defaultdict
import copy


# ─── ÉVÉNEMENT DE BASE ───────────────────────────────────────────────────────

@dataclass
class Evenement:
    """
    Fait immuable qui s'est produit dans le domaine métier.
    Un événement est TOUJOURS au passé : "CompteDebite", "CommandePassee".
    Il ne doit JAMAIS être modifié une fois persisté.
    """
    event_id:     str   = field(default_factory=lambda: uuid.uuid4().hex[:16])
    aggregate_id: str   = ""
    aggregate_type: str = ""
    type_event:   str   = ""
    version:      int   = 0       # Numéro de séquence dans le stream
    ts:           float = field(default_factory=time.time)
    metadata:     dict  = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "event_id":      self.event_id,
            "aggregate_id":  self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "type_event":    self.type_event,
            "version":       self.version,
            "ts":            self.ts,
            "metadata":      self.metadata,
            "payload":       self._payload(),
        }

    def _payload(self) -> dict:
        """Sous-classes surchargent pour exposer leur payload métier."""
        return {}


# ─── ÉVÉNEMENTS MÉTIER : COMPTE BANCAIRE ─────────────────────────────────────

@dataclass
class CompteOuvert(Evenement):
    proprietaire: str  = ""
    solde_initial: float = 0.0

    def __post_init__(self):
        self.type_event     = "CompteOuvert"
        self.aggregate_type = "Compte"

    def _payload(self): return {"proprietaire": self.proprietaire,
                                "solde_initial": self.solde_initial}

@dataclass
class CompteDebite(Evenement):
    montant:    float = 0.0
    motif:      str   = ""
    destinataire: str = ""

    def __post_init__(self):
        self.type_event     = "CompteDebite"
        self.aggregate_type = "Compte"

    def _payload(self): return {"montant": self.montant, "motif": self.motif,
                                "destinataire": self.destinataire}

@dataclass
class CompteCredite(Evenement):
    montant: float = 0.0
    motif:   str   = ""
    source:  str   = ""

    def __post_init__(self):
        self.type_event     = "CompteCredite"
        self.aggregate_type = "Compte"

    def _payload(self): return {"montant": self.montant, "motif": self.motif,
                                "source": self.source}

@dataclass
class CompteSuspendu(Evenement):
    raison: str = ""

    def __post_init__(self):
        self.type_event     = "CompteSuspendu"
        self.aggregate_type = "Compte"

    def _payload(self): return {"raison": self.raison}

@dataclass
class CompteReactive(Evenement):
    def __post_init__(self):
        self.type_event     = "CompteReactive"
        self.aggregate_type = "Compte"


# ─── SNAPSHOT ────────────────────────────────────────────────────────────────

@dataclass
class Snapshot:
    """
    État agrégé d'un aggregate à un instant T.
    Évite de rejouer tous les événements depuis le début.
    Stratégie commune : créer un snapshot tous les 50-100 événements.
    """
    snapshot_id:  str   = field(default_factory=lambda: uuid.uuid4().hex[:12])
    aggregate_id: str   = ""
    version:      int   = 0      # Version de l'événement qui a déclenché le snapshot
    etat:         dict  = field(default_factory=dict)
    ts:           float = field(default_factory=time.time)


# ─── EVENT STORE ─────────────────────────────────────────────────────────────

class EventStore:
    """
    Persistance des événements. Append-only — jamais de UPDATE ou DELETE.
    En prod : EventStoreDB, PostgreSQL (table events), Kafka, DynamoDB Streams.

    Structure :
      streams[aggregate_id] = [event1, event2, event3, ...]
      snapshots[aggregate_id] = snapshot_le_plus_recent

    Concurrence optimiste : chaque append vérifie la version attendue.
    Si la version ne correspond pas → ConcurrencyException.
    Empêche les Lost Update sans verrou distribué.
    """

    def __init__(self):
        self._streams:   dict[str, list[Evenement]] = defaultdict(list)
        self._snapshots: dict[str, Snapshot]         = {}
        self._abonnes:   dict[str, list[Callable]]   = defaultdict(list)
        self._lock       = threading.RLock()
        self.stats       = {"appends": 0, "lectures": 0, "snapshots_crees": 0}

    def appendre(self, aggregate_id: str, evenement: Evenement,
                 version_attendue: int = -1) -> int:
        """
        Appends an event to a stream.
        version_attendue = -1 → pas de vérification de concurrence
        version_attendue = N  → l'event sera rejeté si le stream a déjà N+1 events
        Retourne la nouvelle version.
        """
        with self._lock:
            stream = self._streams[aggregate_id]
            version_actuelle = len(stream)

            if version_attendue >= 0 and version_actuelle != version_attendue:
                raise ConcurrencyException(
                    f"Conflit de version : attendu {version_attendue}, "
                    f"actuel {version_actuelle} sur {aggregate_id}"
                )

            evenement.aggregate_id = aggregate_id
            evenement.version      = version_actuelle
            stream.append(copy.deepcopy(evenement))
            self.stats["appends"] += 1

        # Notifier les abonnés hors du verrou
        self._notifier(aggregate_id, evenement)
        return version_actuelle + 1

    def lire_stream(self, aggregate_id: str,
                    depuis_version: int = 0,
                    jusqu_a_version: int = None) -> list[Evenement]:
        """
        Lit les événements d'un stream, optionnellement sur une plage.
        Pour time-travel : jusqu_a_version = version à la date voulue.
        """
        with self._lock:
            self.stats["lectures"] += 1
            stream = self._streams.get(aggregate_id, [])
            jusqu_a = jusqu_a_version if jusqu_a_version is not None else len(stream)
            return list(stream[depuis_version:jusqu_a])

    def lire_tous_types(self, type_event: str) -> list[Evenement]:
        """Lit tous les événements d'un type donné (pour les projections globales)."""
        with self._lock:
            resultat = []
            for stream in self._streams.values():
                resultat.extend(e for e in stream if e.type_event == type_event)
            return sorted(resultat, key=lambda e: e.ts)

    def sauvegarder_snapshot(self, snapshot: Snapshot):
        with self._lock:
            self._snapshots[snapshot.aggregate_id] = snapshot
            self.stats["snapshots_crees"] += 1

    def charger_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        with self._lock:
            return self._snapshots.get(aggregate_id)

    def nb_evenements(self, aggregate_id: str) -> int:
        with self._lock:
            return len(self._streams.get(aggregate_id, []))

    def souscrire(self, type_event: str, callback: Callable[[Evenement], None]):
        """Souscrit aux événements d'un type. Utilisé pour mettre à jour les projections."""
        with self._lock:
            self._abonnes[type_event].append(callback)

    def _notifier(self, aggregate_id: str, evenement: Evenement):
        abonnes = self._abonnes.get(evenement.type_event, [])
        for cb in abonnes:
            threading.Thread(target=cb, args=(evenement,), daemon=True).start()


class ConcurrencyException(Exception):
    pass


# ─── AGGREGATE : COMPTE BANCAIRE ─────────────────────────────────────────────

class Compte:
    """
    Aggregate qui encapsule la logique métier d'un compte bancaire.
    État reconstruit uniquement depuis les événements.

    Principe clé : les méthodes métier (debiter, crediter) NE MODIFIENT PAS
    l'état directement. Elles produisent des événements qui, eux, modifient l'état
    via apply().
    """

    SEUIL_SNAPSHOT = 5    # Créer un snapshot tous les 5 événements (démo)

    def __init__(self, account_id: str, store: EventStore):
        self.id      = account_id
        self._store  = store

        # État interne — reconstruit depuis les événements
        self.solde:       float = 0.0
        self.proprietaire: str  = ""
        self.suspendu:    bool  = False
        self.version:     int   = 0    # Numéro du dernier événement appliqué

        self._recharger()

    def _recharger(self):
        """Reconstruit l'état depuis le snapshot (si dispo) + événements suivants."""
        snapshot = self._store.charger_snapshot(self.id)
        debut = 0
        if snapshot:
            self.solde        = snapshot.etat["solde"]
            self.proprietaire = snapshot.etat["proprietaire"]
            self.suspendu     = snapshot.etat["suspendu"]
            self.version      = snapshot.version  # already = nb events at snapshot time
            debut             = snapshot.version

        evenements = self._store.lire_stream(self.id, depuis_version=debut)
        for e in evenements:
            self._apply(e)

    def _apply(self, evenement: Evenement):
        """Applique un événement à l'état interne."""
        if isinstance(evenement, CompteOuvert):
            self.proprietaire = evenement.proprietaire
            self.solde        = evenement.solde_initial
        elif isinstance(evenement, CompteDebite):
            self.solde -= evenement.montant
        elif isinstance(evenement, CompteCredite):
            self.solde += evenement.montant
        elif isinstance(evenement, CompteSuspendu):
            self.suspendu = True
        elif isinstance(evenement, CompteReactive):
            self.suspendu = False
        self.version = evenement.version + 1

    def _enregistrer(self, evenement: Evenement):
        """Persiste l'événement et l'applique localement."""
        self._store.appendre(self.id, evenement, version_attendue=self.version)
        self._apply(evenement)
        self._snapshot_si_necessaire()

    def _snapshot_si_necessaire(self):
        if self.version > 0 and self.version % self.SEUIL_SNAPSHOT == 0:
            snap = Snapshot(
                aggregate_id = self.id,
                version      = self.version,
                etat         = {
                    "solde":        self.solde,
                    "proprietaire": self.proprietaire,
                    "suspendu":     self.suspendu,
                }
            )
            self._store.sauvegarder_snapshot(snap)

    # ── API métier ────────────────────────────────────────────────────────────

    def ouvrir(self, proprietaire: str, solde_initial: float = 0.0):
        if self._store.nb_evenements(self.id) > 0:
            raise ValueError("Compte déjà ouvert")
        self._enregistrer(CompteOuvert(
            proprietaire=proprietaire, solde_initial=solde_initial
        ))

    def debiter(self, montant: float, motif: str = "", destinataire: str = ""):
        if self.suspendu:
            raise ValueError("Compte suspendu")
        if montant <= 0:
            raise ValueError("Montant invalide")
        if self.solde < montant:
            raise ValueError(f"Solde insuffisant : {self.solde:.2f}€ < {montant:.2f}€")
        self._enregistrer(CompteDebite(montant=montant, motif=motif,
                                       destinataire=destinataire))

    def crediter(self, montant: float, motif: str = "", source: str = ""):
        if self.suspendu:
            raise ValueError("Compte suspendu")
        if montant <= 0:
            raise ValueError("Montant invalide")
        self._enregistrer(CompteCredite(montant=montant, motif=motif, source=source))

    def suspendre(self, raison: str = ""):
        if self.suspendu:
            raise ValueError("Déjà suspendu")
        self._enregistrer(CompteSuspendu(raison=raison))

    def reactiver(self):
        if not self.suspendu:
            raise ValueError("Pas suspendu")
        self._enregistrer(CompteReactive())


# ─── PROJECTION ──────────────────────────────────────────────────────────────

class ProjectionSoldes:
    """
    Vue dénormalisée reconstruite depuis les événements.
    Mise à jour au fil de l'eau (event-driven) ou reconstituée à la demande.
    En CQRS : c'est le "Query Side" (lecture) vs "Command Side" (écriture).
    """

    def __init__(self, store: EventStore):
        self._soldes: dict[str, float] = {}
        self._nb_transactions: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

        # Souscription aux événements pour mise à jour en temps réel
        store.souscrire("CompteOuvert",  self._on_ouvert)
        store.souscrire("CompteDebite",  self._on_debite)
        store.souscrire("CompteCredite", self._on_credite)

    def _on_ouvert(self, e: CompteOuvert):
        with self._lock:
            self._soldes[e.aggregate_id] = e.solde_initial

    def _on_debite(self, e: CompteDebite):
        with self._lock:
            self._soldes[e.aggregate_id] = self._soldes.get(e.aggregate_id, 0) - e.montant
            self._nb_transactions[e.aggregate_id] += 1

    def _on_credite(self, e: CompteCredite):
        with self._lock:
            self._soldes[e.aggregate_id] = self._soldes.get(e.aggregate_id, 0) + e.montant
            self._nb_transactions[e.aggregate_id] += 1

    def solde(self, account_id: str) -> float:
        with self._lock:
            return self._soldes.get(account_id, 0.0)

    def tous_soldes(self) -> dict[str, float]:
        with self._lock:
            return dict(self._soldes)

    def reconstruire_depuis(self, store: EventStore):
        """Reconstruit entièrement la projection depuis le store (cold start)."""
        with self._lock:
            self._soldes.clear()
            self._nb_transactions.clear()
        for e in store.lire_tous_types("CompteOuvert"):
            self._on_ouvert(e)
        for e in store.lire_tous_types("CompteDebite"):
            self._on_debite(e)
        for e in store.lire_tous_types("CompteCredite"):
            self._on_credite(e)
