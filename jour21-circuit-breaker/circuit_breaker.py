"""
Jour 21 — Circuit Breaker (Hystrix/Resilience4j)
=================================================
Problème : payment-svc est en panne et met 30s à répondre (timeout).
  → 100 requêtes simultanées = 100 threads bloqués 30s chacun
  → Pool de threads saturé → TOUTE l'application est figée
  → Les services appelants cascadent en timeout aussi
  → Un seul service lent détruit tout le système

Sans Circuit Breaker : chaque requête attend le timeout complet.
Avec Circuit Breaker  : après N échecs, on COUPE le circuit.
  Les requêtes suivantes échouent IMMÉDIATEMENT (fail-fast, 0ms).
  Le système reste réactif. Les appelants peuvent utiliser un fallback.

Analogie : le disjoncteur électrique.
  Quand il y a un court-circuit, le disjoncteur s'ouvre immédiatement.
  Il ne laisse pas le courant circuler jusqu'à ce que la maison brûle.
  Après un délai de refroidissement, on peut le réenclencher (test).

États du Circuit Breaker :

  CLOSED (fermé = circuit passant, état normal) :
    Toutes les requêtes passent.
    On compte les échecs sur une fenêtre glissante.
    Si taux_echec > seuil → passer à OPEN.

  OPEN (ouvert = circuit coupé, état protection) :
    Les requêtes échouent IMMÉDIATEMENT sans appel au service.
    → Fail-fast : réponse en 0ms au lieu de 30s de timeout
    Après timeout_ouverture secondes → passer à HALF-OPEN.

  HALF-OPEN (semi-ouvert = test de rétablissement) :
    On laisse passer N requêtes de test (ex: 1 ou 3).
    Si elles réussissent → revenir à CLOSED (service rétabli).
    Si elles échouent  → revenir à OPEN (service toujours en panne).

Deux types de fenêtres pour compter les échecs :
  COUNT_BASED  : compter sur les N dernières requêtes
                 Ex: si 5/10 dernières requêtes échouent → OPEN
  TIME_BASED   : compter sur une fenêtre de T secondes
                 Ex: si 50% des requêtes échouent dans la dernière seconde → OPEN
"""

from __future__ import annotations
import time
import threading
import random
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
from enum import Enum
from collections import deque


# ─── ÉTAT DU CIRCUIT BREAKER ─────────────────────────────────────────────────

class EtatCircuit(Enum):
    CLOSED    = "CLOSED"      # Normal — requêtes passent
    OPEN      = "OPEN"        # Coupé — fail-fast immédiat
    HALF_OPEN = "HALF_OPEN"   # Test — quelques requêtes de sonde


# ─── RÉSULTAT D'UN APPEL ─────────────────────────────────────────────────────

class ResultatAppel(Enum):
    SUCCES  = "succes"
    ECHEC   = "echec"
    TIMEOUT = "timeout"
    COURT_CIRCUITE = "court_circuité"   # Circuit ouvert, fail-fast


# ─── ÉVÉNEMENT DE TRANSITION ─────────────────────────────────────────────────

@dataclass
class TransitionEtat:
    ts:          float
    etat_avant:  EtatCircuit
    etat_apres:  EtatCircuit
    raison:      str


# ─── CIRCUIT BREAKER ─────────────────────────────────────────────────────────

class CircuitBreaker:
    """
    Implémentation du pattern Circuit Breaker.

    Paramètres :
      seuil_echec_pct   : % d'échecs pour ouvrir le circuit (ex: 50%)
      taille_fenetre    : nb de requêtes dans la fenêtre COUNT-BASED
      min_requetes      : nb minimum de requêtes avant d'évaluer le seuil
      timeout_ouvert_s  : secondes en OPEN avant de passer à HALF_OPEN
      nb_sondes         : nb de requêtes en HALF_OPEN pour tester
      timeout_requete_s : timeout par requête (déclanche ResultatAppel.TIMEOUT)
    """

    def __init__(
        self,
        nom:               str,
        seuil_echec_pct:   float = 50.0,
        taille_fenetre:    int   = 10,
        min_requetes:      int   = 5,
        timeout_ouvert_s:  float = 5.0,
        nb_sondes:         int   = 3,
        timeout_requete_s: float = 2.0,
    ):
        self.nom               = nom
        self.seuil_echec_pct   = seuil_echec_pct
        self.taille_fenetre    = taille_fenetre
        self.min_requetes      = min_requetes
        self.timeout_ouvert_s  = timeout_ouvert_s
        self.nb_sondes         = nb_sondes
        self.timeout_requete_s = timeout_requete_s

        self._etat            = EtatCircuit.CLOSED
        self._ts_ouverture    = 0.0
        self._sondes_reussies = 0
        self._sondes_envoyees = 0

        # Fenêtre glissante : True = succès, False = échec
        self._fenetre: deque[bool] = deque(maxlen=taille_fenetre)

        self._lock        = threading.RLock()
        self.transitions: list[TransitionEtat] = []
        self.stats = {
            "total":          0,
            "succes":         0,
            "echecs":         0,
            "timeouts":       0,
            "court_circuits": 0,   # Requêtes bloquées par le circuit ouvert
        }

    # ── API publique ──────────────────────────────────────────────────────────

    def appeler(self, fn: Callable, *args, **kwargs) -> tuple[Any, ResultatAppel]:
        """
        Tente d'appeler fn() via le circuit breaker.
        Retourne (résultat, ResultatAppel).
        """
        with self._lock:
            self.stats["total"] += 1
            etat_actuel = self._evaluer_etat()

            if etat_actuel == EtatCircuit.OPEN:
                self.stats["court_circuits"] += 1
                return None, ResultatAppel.COURT_CIRCUITE

            if etat_actuel == EtatCircuit.HALF_OPEN:
                self._sondes_envoyees += 1

        # Exécuter l'appel avec timeout
        resultat, ra = self._executer_avec_timeout(fn, *args, **kwargs)

        with self._lock:
            self._enregistrer_resultat(ra, etat_actuel)
            return resultat, ra

    def etat(self) -> EtatCircuit:
        with self._lock:
            return self._evaluer_etat()

    def taux_echec(self) -> float:
        with self._lock:
            if not self._fenetre:
                return 0.0
            echecs = sum(1 for ok in self._fenetre if not ok)
            return echecs / len(self._fenetre) * 100

    def forcer_ouverture(self):
        """Pour les tests : ouvrir le circuit manuellement."""
        with self._lock:
            self._transitionner(EtatCircuit.OPEN, "forcé manuellement")

    def reinitialiser(self):
        """Réinitialiser le circuit (tests)."""
        with self._lock:
            self._etat = EtatCircuit.CLOSED
            self._fenetre.clear()
            self._ts_ouverture = 0.0
            self._sondes_reussies = 0
            self._sondes_envoyees = 0

    # ── Logique interne ───────────────────────────────────────────────────────

    def _evaluer_etat(self) -> EtatCircuit:
        """Évaluer l'état courant (peut déclencher une transition)."""
        if self._etat == EtatCircuit.OPEN:
            # Vérifier si le timeout d'ouverture est écoulé
            if time.time() - self._ts_ouverture >= self.timeout_ouvert_s:
                self._transitionner(EtatCircuit.HALF_OPEN,
                                    f"timeout {self.timeout_ouvert_s}s écoulé")
        return self._etat

    def _enregistrer_resultat(self, ra: ResultatAppel, etat_avant: EtatCircuit):
        """Met à jour la fenêtre et évalue si une transition est nécessaire."""
        succes = (ra == ResultatAppel.SUCCES)
        self._fenetre.append(succes)

        if succes:
            self.stats["succes"] += 1
        elif ra == ResultatAppel.TIMEOUT:
            self.stats["timeouts"] += 1
        else:
            self.stats["echecs"] += 1

        if etat_avant == EtatCircuit.HALF_OPEN:
            if succes:
                self._sondes_reussies += 1
                if self._sondes_reussies >= self.nb_sondes:
                    self._transitionner(EtatCircuit.CLOSED,
                                        f"{self.nb_sondes} sondes réussies")
            else:
                # Sonde échouée → revenir en OPEN
                self._transitionner(EtatCircuit.OPEN,
                                    "sonde échouée en HALF_OPEN")
                self._sondes_reussies = 0
                self._sondes_envoyees = 0

        elif etat_avant == EtatCircuit.CLOSED:
            # Vérifier si on dépasse le seuil d'échec
            if (len(self._fenetre) >= self.min_requetes
                    and self.taux_echec() >= self.seuil_echec_pct):
                self._transitionner(EtatCircuit.OPEN,
                                    f"taux d'échec {self.taux_echec():.0f}% ≥ {self.seuil_echec_pct}%")

    def _executer_avec_timeout(
        self, fn: Callable, *args, **kwargs
    ) -> tuple[Any, ResultatAppel]:
        """Exécute fn() avec un timeout strict."""
        resultat_container = [None]
        erreur_container   = [None]
        ra_container       = [ResultatAppel.SUCCES]

        def cible():
            try:
                resultat_container[0] = fn(*args, **kwargs)
                ra_container[0]       = ResultatAppel.SUCCES
            except Exception as e:
                erreur_container[0]   = e
                ra_container[0]       = ResultatAppel.ECHEC

        t = threading.Thread(target=cible, daemon=True)
        t.start()
        t.join(timeout=self.timeout_requete_s)

        if t.is_alive():
            return None, ResultatAppel.TIMEOUT

        return resultat_container[0], ra_container[0]

    def _transitionner(self, nouvel_etat: EtatCircuit, raison: str):
        """Enregistre la transition d'état."""
        if nouvel_etat == self._etat:
            return
        transition = TransitionEtat(
            ts         = time.time(),
            etat_avant = self._etat,
            etat_apres = nouvel_etat,
            raison     = raison,
        )
        self.transitions.append(transition)
        ancien = self._etat
        self._etat = nouvel_etat
        if nouvel_etat == EtatCircuit.OPEN:
            self._ts_ouverture    = time.time()
            self._sondes_reussies = 0
            self._sondes_envoyees = 0

    def afficher_etat(self):
        etat = self.etat()
        taux = self.taux_echec()
        icone = {"CLOSED": "🟢", "OPEN": "🔴", "HALF_OPEN": "🟡"}.get(etat.value, "?")
        print(f"  {icone} {self.nom:<25} état={etat.value:<10} "
              f"taux_échec={taux:.0f}%  "
              f"stats={self.stats['succes']}✅/{self.stats['echecs']}❌/"
              f"{self.stats['court_circuits']}🚫")

    def afficher_transitions(self):
        if not self.transitions:
            print(f"  {self.nom} : aucune transition")
            return
        for t in self.transitions:
            print(f"  {self.nom}: {t.etat_avant.value:10} → {t.etat_apres.value:10}  ({t.raison})")


# ─── FALLBACK ─────────────────────────────────────────────────────────────────

class CircuitBreakerAvecFallback:
    """
    Circuit breaker avec stratégie de fallback.
    Quand le circuit est OPEN, au lieu de retourner une erreur,
    on appelle une fonction de fallback (cache, valeur par défaut…).
    """

    def __init__(self, cb: CircuitBreaker,
                 fallback: Callable = None):
        self.cb       = cb
        self.fallback = fallback or (lambda *a, **k: None)
        self.stats_fallback = 0

    def appeler(self, fn: Callable, *args, **kwargs) -> Any:
        resultat, ra = self.cb.appeler(fn, *args, **kwargs)
        if ra == ResultatAppel.COURT_CIRCUITE:
            self.stats_fallback += 1
            return self.fallback(*args, **kwargs)
        if ra in (ResultatAppel.ECHEC, ResultatAppel.TIMEOUT):
            self.stats_fallback += 1
            return self.fallback(*args, **kwargs)
        return resultat
