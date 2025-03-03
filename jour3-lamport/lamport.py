"""
Jour 3 — Lamport Timestamps
============================
Problème : Dans un système distribué, chaque machine a sa propre
horloge qui dérive. Si le Serveur A enregistre un événement à
10:00:00.200 et le Serveur B à 10:00:00.100, est-ce que A s'est
vraiment passé APRÈS B ? Pas forcément — les horloges divergent.

Solution de Lamport (1978) : utiliser une horloge LOGIQUE, pas physique.

Règles :
  1. Chaque processus maintient un compteur local (commence à 0)
  2. À chaque événement interne : compteur += 1
  3. À l'envoi d'un message   : compteur += 1, envoyer le compteur
  4. À la réception           : compteur = max(local, reçu) + 1

Propriété garantie :
  Si A → B (A cause B), alors lamport(A) < lamport(B)
  MAIS : lamport(A) < lamport(B) ne prouve PAS que A cause B
  (les horloges de Lamport détectent la causalité, pas l'inverse)
"""

import threading
import time
import random
from dataclasses import dataclass, field
from typing import Optional


# ─── HORLOGE DE LAMPORT ───────────────────────────────────────────────────────

class HorlogeLamport:
    """
    Horloge logique thread-safe.
    Le verrou garantit que deux événements simultanés sur le même
    processus reçoivent des timestamps distincts.
    """

    def __init__(self, nom_processus: str):
        self.processus = nom_processus
        self._compteur = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Événement interne : incrémenter et retourner le timestamp."""
        with self._lock:
            self._compteur += 1
            return self._compteur

    def envoyer(self) -> int:
        """Avant d'envoyer un message : incrémenter et retourner le timestamp à inclure."""
        return self.tick()

    def recevoir(self, timestamp_recu: int) -> int:
        """
        À la réception d'un message avec timestamp_recu :
        Règle de Lamport : compteur = max(local, reçu) + 1
        
        Le +1 garantit que l'événement "réception" est APRÈS
        l'événement "envoi" du message.
        """
        with self._lock:
            self._compteur = max(self._compteur, timestamp_recu) + 1
            return self._compteur

    @property
    def valeur(self) -> int:
        with self._lock:
            return self._compteur

    def __repr__(self):
        return f"Horloge({self.processus}, t={self._compteur})"


# ─── ÉVÉNEMENT ────────────────────────────────────────────────────────────────

@dataclass(order=True)
class Evenement:
    """
    Un événement dans le système distribué.
    order=True permet de trier par (timestamp, processus) automatiquement.
    """
    timestamp:  int
    processus:  str
    type_evt:   str = field(compare=False)
    description: str = field(compare=False)
    horloge_physique: float = field(compare=False, default=0.0)

    def __str__(self):
        return (
            f"[t={self.timestamp:>3}] {self.processus:<12} "
            f"{self.type_evt:<8} → {self.description}"
        )


# ─── PROCESSUS DISTRIBUÉ ─────────────────────────────────────────────────────

class Processus:
    """
    Simule un nœud dans le système distribué.
    Chaque processus a sa propre horloge de Lamport et un journal d'événements.
    """

    def __init__(self, nom: str, derive_ms: float = 0):
        self.nom = nom
        self.horloge = HorlogeLamport(nom)
        self.evenements: list[Evenement] = []
        self.derive_ms = derive_ms  # Dérive de l'horloge physique (pour illustrer le problème)
        self._lock = threading.Lock()

    def _enregistrer(self, type_evt: str, desc: str, timestamp: int):
        evt = Evenement(
            timestamp=timestamp,
            processus=self.nom,
            type_evt=type_evt,
            description=desc,
            horloge_physique=time.time() * 1000 + self.derive_ms
        )
        with self._lock:
            self.evenements.append(evt)
        return evt

    def evenement_interne(self, description: str) -> Evenement:
        """Quelque chose se passe localement (calcul, écriture DB, etc.)"""
        t = self.horloge.tick()
        return self._enregistrer("INTERNE", description, t)

    def preparer_message(self, contenu: str) -> tuple[str, int]:
        """Prépare un message à envoyer, retourne (contenu, timestamp)."""
        t = self.horloge.envoyer()
        self._enregistrer("ENVOI", f"→ '{contenu}'", t)
        return contenu, t

    def recevoir_message(self, contenu: str, timestamp_expediteur: int) -> Evenement:
        """Reçoit un message avec le timestamp de l'expéditeur."""
        t = self.horloge.recevoir(timestamp_expediteur)
        return self._enregistrer("RECEPTION", f"← '{contenu}' (exp. t={timestamp_expediteur})", t)

    def __repr__(self):
        return f"Processus({self.nom}, {self.horloge})"
