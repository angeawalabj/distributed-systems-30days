"""
Jour 23 — "La Fenêtre du Temps" — Flink
=========================================
Apache Flink : moteur de traitement de flux (stream processing).

Problème : les événements arrivent en continu, un par un.
  Comment calculer "le chiffre d'affaires des 5 dernières minutes" 
  sans attendre la fin du monde ?

  BATCH (Spark, MapReduce) :
    Attendre que toutes les données arrivent → traiter → résultat
    Latence : minutes à heures
    Adapté : rapports quotidiens, ML training

  STREAM (Flink, Kafka Streams) :
    Traiter chaque événement à son arrivée → résultat continu
    Latence : millisecondes à secondes
    Adapté : dashboards temps réel, détection de fraude, alertes

Le concept central : les FENÊTRES TEMPORELLES

  1. TUMBLING WINDOW (fenêtre fixe non-chevauchante)
     ├──10min──┤├──10min──┤├──10min──┤
     [0-10min] [10-20min] [20-30min]
     → Chaque événement appartient exactement à 1 fenêtre
     → Agrégats périodiques : CA par heure, requêtes par minute

  2. SLIDING WINDOW (fenêtre glissante)
     ├──10min──┤
         ├──10min──┤
             ├──10min──┤
     Taille=10min, pas=5min → fenêtres se chevauchent
     → Chaque événement appartient à plusieurs fenêtres
     → Détection d'anomalies, moyennes mobiles

  3. SESSION WINDOW (fenêtre de session)
     [event event event]  gap  [event event]  gap  [event]
     ←── session 1 ────→       ←─ session 2 →      ← s3 →
     → Fenêtre qui se ferme après N secondes d'inactivité
     → Analyse du comportement utilisateur, durée de session

Event Time vs Processing Time :
  Processing Time : l'heure de la machine qui reçoit l'événement
  Event Time      : l'heure à laquelle l'événement s'est produit
  → Un événement peut arriver TARD (réseau lent, mobile hors-ligne)
  → Flink utilise des WATERMARKS pour gérer les événements tardifs

Watermark :
  Signal qui dit "je suis sûr que tous les événements avant T sont arrivés"
  → Flink peut alors fermer et émettre les fenêtres avant T
  → Événements arrivant après le watermark = "late events" → ignorés ou retraités
"""

from __future__ import annotations
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Iterator
from collections import defaultdict


# ─── ÉVÉNEMENT ───────────────────────────────────────────────────────────────

@dataclass
class Evenement:
    """Un événement dans le flux."""
    cle:          str     # Clef de partitionnement (user_id, session_id...)
    valeur:       Any
    event_time:   float   # Timestamp de l'événement (peut être dans le passé)
    processing_time: float = field(default_factory=time.time)

    def retard(self) -> float:
        """Retard entre l'event time et le processing time."""
        return self.processing_time - self.event_time


# ─── FENÊTRES ────────────────────────────────────────────────────────────────

@dataclass
class FenetreTumbling:
    """Fenêtre fixe non-chevauchante."""
    debut:  float
    fin:    float
    cle:    str
    events: list[Evenement] = field(default_factory=list)

    @property
    def duree(self) -> float:
        return self.fin - self.debut

    def appartient(self, event_time: float) -> bool:
        return self.debut <= event_time < self.fin


@dataclass
class FenetreSliding:
    """Fenêtre glissante avec chevauchement."""
    debut:  float
    fin:    float
    cle:    str
    events: list[Evenement] = field(default_factory=list)

    def appartient(self, event_time: float) -> bool:
        return self.debut <= event_time < self.fin


@dataclass
class FenetreSession:
    """Fenêtre de session : se ferme après un gap d'inactivité."""
    debut:    float
    fin:      float      # Dernier événement + gap_timeout
    cle:      str
    events:   list[Evenement] = field(default_factory=list)
    fermee:   bool = False

    def etendre(self, event_time: float, gap: float):
        """Étendre la session si l'événement arrive dans le gap."""
        self.fin = event_time + gap
        self.fermee = False


# ─── WATERMARK ───────────────────────────────────────────────────────────────

class GestionnaireWatermark:
    """
    Gère les watermarks pour l'event-time processing.
    Watermark = max(event_times_vus) - max_retard_tolere
    → Garantit que tous les événements avant le watermark sont arrivés.
    """

    def __init__(self, max_retard_s: float = 5.0):
        self.max_retard_s  = max_retard_s
        self._max_event_t  = 0.0
        self._watermark    = 0.0

    def mettre_a_jour(self, event_time: float) -> float:
        if event_time > self._max_event_t:
            self._max_event_t = event_time
            self._watermark   = self._max_event_t - self.max_retard_s
        return self._watermark

    @property
    def watermark(self) -> float:
        return self._watermark


# ─── OPÉRATEURS DE FLUX ──────────────────────────────────────────────────────

class OperateurTumbling:
    """
    Fenêtres tumbling : intervalles fixes non-chevauchants.
    Chaque événement appartient à exactement une fenêtre.
    """

    def __init__(self, taille_s: float,
                 fn_agregat: Callable[[list[Evenement]], Any],
                 max_retard_s: float = 0.0):
        self.taille_s     = taille_s
        self.fn_agregat   = fn_agregat
        self.max_retard_s = max_retard_s
        self._fenêtres: dict[tuple, FenetreTumbling] = {}
        self._resultats: list[dict] = []
        self._watermark  = GestionnaireWatermark(max_retard_s)
        self._lock = threading.Lock()

    def _cle_fenetre(self, cle: str, event_time: float) -> tuple:
        debut = int(event_time / self.taille_s) * self.taille_s
        return (cle, debut)

    def traiter(self, event: Evenement):
        with self._lock:
            wm  = self._watermark.mettre_a_jour(event.event_time)
            k   = self._cle_fenetre(event.cle, event.event_time)
            if k not in self._fenêtres:
                debut = k[1]
                self._fenêtres[k] = FenetreTumbling(
                    debut=debut, fin=debut + self.taille_s, cle=event.cle
                )
            self._fenêtres[k].events.append(event)
            # Émettre les fenêtres dont la fin est avant le watermark
            self._emettre_pretes(wm)

    def _emettre_pretes(self, watermark: float):
        a_emettre = [k for k, f in self._fenêtres.items()
                     if f.fin <= watermark + self.max_retard_s]
        for k in a_emettre:
            f = self._fenêtres.pop(k)
            if f.events:
                self._resultats.append({
                    "type":    "tumbling",
                    "cle":     f.cle,
                    "debut":   f.debut,
                    "fin":     f.fin,
                    "nb":      len(f.events),
                    "resultat": self.fn_agregat(f.events),
                })

    def vider(self) -> list[dict]:
        """Forcer l'émission de toutes les fenêtres restantes."""
        with self._lock:
            for k, f in list(self._fenêtres.items()):
                if f.events:
                    self._resultats.append({
                        "type":     "tumbling",
                        "cle":      f.cle,
                        "debut":    f.debut,
                        "fin":      f.fin,
                        "nb":       len(f.events),
                        "resultat": self.fn_agregat(f.events),
                    })
            self._fenêtres.clear()
        return list(self._resultats)


class OperateurSliding:
    """
    Fenêtres glissantes : taille fixe, pas configurable.
    Un événement appartient à ceil(taille/pas) fenêtres.
    """

    def __init__(self, taille_s: float, pas_s: float,
                 fn_agregat: Callable[[list[Evenement]], Any]):
        self.taille_s   = taille_s
        self.pas_s      = pas_s
        self.fn_agregat = fn_agregat
        self._tous_events: list[Evenement] = []
        self._lock = threading.Lock()

    def traiter(self, event: Evenement):
        with self._lock:
            self._tous_events.append(event)

    def calculer(self, t_debut: float, t_fin: float,
                 cle: Optional[str] = None) -> list[dict]:
        """Calculer toutes les fenêtres glissantes dans [t_debut, t_fin]."""
        resultats = []
        t = t_debut
        while t + self.taille_s <= t_fin + self.pas_s:
            debut_f = t
            fin_f   = t + self.taille_s
            with self._lock:
                events = [e for e in self._tous_events
                          if debut_f <= e.event_time < fin_f
                          and (cle is None or e.cle == cle)]
            if events:
                resultats.append({
                    "type":     "sliding",
                    "cle":      cle or "*",
                    "debut":    debut_f,
                    "fin":      fin_f,
                    "nb":       len(events),
                    "resultat": self.fn_agregat(events),
                })
            t += self.pas_s
        return resultats


class OperateurSession:
    """
    Fenêtres de session : se ferment après gap_s secondes d'inactivité.
    Parfait pour analyser les sessions utilisateur.
    """

    def __init__(self, gap_s: float,
                 fn_agregat: Callable[[list[Evenement]], Any]):
        self.gap_s      = gap_s
        self.fn_agregat = fn_agregat
        self._sessions: dict[str, FenetreSession] = {}
        self._fermees:  list[FenetreSession] = []
        self._lock      = threading.Lock()

    def traiter(self, event: Evenement):
        with self._lock:
            cle = event.cle
            if cle in self._sessions:
                s = self._sessions[cle]
                if event.event_time <= s.fin:
                    # Dans le gap → étendre la session
                    s.events.append(event)
                    s.etendre(event.event_time, self.gap_s)
                else:
                    # Hors gap → fermer la session et en ouvrir une nouvelle
                    self._fermer(cle)
                    self._ouvrir(cle, event)
            else:
                self._ouvrir(cle, event)

    def _ouvrir(self, cle: str, event: Evenement):
        self._sessions[cle] = FenetreSession(
            debut=event.event_time,
            fin=event.event_time + self.gap_s,
            cle=cle,
            events=[event],
        )

    def _fermer(self, cle: str):
        if cle in self._sessions:
            s = self._sessions.pop(cle)
            s.fermee = True
            self._fermees.append(s)

    def vider(self) -> list[dict]:
        with self._lock:
            for cle in list(self._sessions.keys()):
                self._fermer(cle)
        return [
            {
                "type":     "session",
                "cle":      s.cle,
                "debut":    s.debut,
                "fin":      s.fin,
                "duree":    s.fin - s.debut - self.gap_s,
                "nb":       len(s.events),
                "resultat": self.fn_agregat(s.events),
            }
            for s in self._fermees
        ]
