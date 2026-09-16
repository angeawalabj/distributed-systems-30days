"""Jour 28 - Chaos Engineering core

Vocabulaire :
  Steady State    : comportement normal mesurable (taux erreur, latence P99)
  Blast Radius    : périmètre de l'expérience (1 service, X% du trafic, tout)
  Abort Condition : seuil au-delà duquel l'expérience doit s'arrêter
  Game Day        : session intensive d'injections multiples

Flux : Hypothèse -> Injection -> Mesure -> Comparaison au SLO -> Correctif
"""
from __future__ import annotations
import time, random, threading, operator, statistics
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
from collections import defaultdict
from enum import Enum


class TypePanne(Enum):
    LATENCE      = "latence"
    ERREUR       = "erreur"
    CRASH        = "crash"
    PERTE_PAQUET = "perte_paquet"


class ServiceException(Exception):
    """Levée par Service.appeler() quand l'appel échoue (panne injectée ou nominale)."""
    def __init__(self, service: str, status: int, message: str):
        super().__init__(f"{service}: {message} (status={status})")
        self.service = service
        self.status  = status
        self.message = message


# ─── INJECTEUR DE PANNES ──────────────────────────────────────────────────────

class InjecteurPanne:
    """
    Registre central des pannes actives, par service cible.
    Un Service consulte l'injecteur à chaque appel pour savoir s'il doit
    se comporter anormalement — exactement comme un proxy/sidecar en
    production (ex: Envoy fault injection, Chaos Mesh).
    """
    def __init__(self):
        self._pannes: dict[str, dict] = {}
        self._stats  = defaultdict(int)
        self._lock   = threading.Lock()

    def injecter(self, cible: str, type_panne: TypePanne, probabilite: float, **params):
        with self._lock:
            self._pannes[cible] = {"type": type_panne, "probabilite": probabilite, **params}

    def effacer(self, cible: Optional[str] = None):
        with self._lock:
            if cible is None:
                self._pannes.clear()
            else:
                self._pannes.pop(cible, None)

    def tirer(self, cible: str) -> Optional[dict]:
        """Décide si la panne configurée pour `cible` se déclenche cette fois-ci."""
        with self._lock:
            cfg = self._pannes.get(cible)
        if cfg is None:
            return None
        if random.random() < cfg["probabilite"]:
            with self._lock:
                self._stats[f"{cible}:{cfg['type'].value}"] += 1
            return cfg
        return None

    def stats(self) -> dict:
        with self._lock:
            return dict(self._stats)


# ─── SERVICE SIMULÉ (avec auto-surveillance type circuit breaker) ────────────

class Service:
    """
    Simule un microservice avec latence nominale, un petit taux d'erreur
    "naturel", et une sensibilité aux pannes injectées via un InjecteurPanne
    partagé.

    Chaque service surveille son propre taux d'échec récent (fenêtre
    glissante) et expose `cb_ouvert` dans `stats()` — mais NE bloque PAS
    les appels lui-même : c'est un indicateur, pas une protection. La leçon
    du GameDay (jour 28) est justement que ce service n'a PAS de vrai
    circuit breaker branché (cf. jour 21 pour l'implémentation qui, elle,
    coupe réellement les appels).
    """
    FENETRE_CB = 10
    SEUIL_CB_PCT = 50.0

    def __init__(self, nom: str, injecteur: InjecteurPanne,
                 latence_nominale_ms: float = 20.0, taux_erreur_nominal_pct: float = 0.5):
        self.nom        = nom
        self._injecteur = injecteur
        self._base       = latence_nominale_ms
        self._nominal_err = taux_erreur_nominal_pct
        self._lock       = threading.Lock()
        self._total      = 0
        self._echecs     = 0
        self._fenetre: list[bool] = []   # historique récent (True = succès)

    def appeler(self, action: str = "default") -> dict:
        with self._lock:
            self._total += 1

        panne = self._injecteur.tirer(self.nom)

        if panne and panne["type"] == TypePanne.CRASH:
            self._enregistrer(False)
            raise ServiceException(self.nom, 503, f"{self.nom} indisponible (crash)")

        base_ms  = max(0.5, random.gauss(self._base, self._base * 0.15))
        extra_ms = panne.get("ms", 0.0) if panne and panne["type"] == TypePanne.LATENCE else 0.0
        time.sleep((base_ms + extra_ms) / 1000)

        if panne and panne["type"] == TypePanne.ERREUR:
            self._enregistrer(False)
            raise ServiceException(self.nom, panne.get("code", 500),
                                    panne.get("message", "erreur injectée"))

        if panne and panne["type"] == TypePanne.PERTE_PAQUET:
            self._enregistrer(False)
            raise ServiceException(self.nom, 0, "paquet perdu")

        if random.random() < self._nominal_err / 100:
            self._enregistrer(False)
            raise ServiceException(self.nom, 500, "erreur transitoire")

        self._enregistrer(True)
        return {"ok": True, "action": action, "latence_ms": base_ms + extra_ms}

    def _enregistrer(self, succes: bool):
        with self._lock:
            if not succes:
                self._echecs += 1
            self._fenetre.append(succes)
            if len(self._fenetre) > self.FENETRE_CB:
                self._fenetre.pop(0)

    def stats(self) -> dict:
        with self._lock:
            total, echecs, fenetre = self._total, self._echecs, list(self._fenetre)
        taux_fenetre = (fenetre.count(False) / len(fenetre) * 100) if fenetre else 0.0
        return {
            "total": total,
            "echec": echecs,
            "taux_echec": round((echecs / max(total, 1)) * 100, 1),
            "cb_ouvert": len(fenetre) >= self.FENETRE_CB and taux_fenetre >= self.SEUIL_CB_PCT,
        }


# ─── SLO ──────────────────────────────────────────────────────────────────────

_OPERATEURS = {"<": operator.lt, "<=": operator.le, ">": operator.gt,
               ">=": operator.ge, "==": operator.eq}


@dataclass
class SLO:
    """Service Level Objective : une métrique doit rester du bon côté d'un seuil."""
    nom:       str
    metrique:  str          # clé à lire dans le dict de mesures (ex: "latence_p99_ms")
    seuil:     float
    operateur: str = "<"

    def evaluer(self, valeur: float) -> bool:
        return _OPERATEURS[self.operateur](valeur, self.seuil)


class MoniteurSLO:
    """Évalue un ensemble de SLOs contre une mesure et rapporte les violations."""
    def __init__(self, slos: list[SLO]):
        self.slos = slos

    def violations(self, metriques: dict) -> list[str]:
        out = []
        for slo in self.slos:
            valeur = metriques.get(slo.metrique, 0)
            if not slo.evaluer(valeur):
                out.append(f"{slo.nom} : {valeur} viole le seuil ({slo.operateur} {slo.seuil})")
        return out

    def tous_ok(self, metriques: dict) -> bool:
        return not self.violations(metriques)


# ─── EXPÉRIENCE & MOTEUR DE CHAOS ─────────────────────────────────────────────

@dataclass
class ExperienceChaos:
    """Hypothèse + configuration d'une expérience de chaos engineering."""
    nom:         str
    hypothese:   str
    cible:       str
    type_panne:  TypePanne
    probabilite: float
    parametres:  dict = field(default_factory=dict)


@dataclass
class ResultatExperience:
    experience:     ExperienceChaos
    metriques:      dict            # {"baseline": {...}, "sous_panne": {...}}
    incidents:      list
    hypothese_ok:   bool
    recommendation: str


class MoteurChaos:
    """
    Exécute le cycle Hypothèse -> Injection -> Mesure -> Verdict.
    Les requêtes sont envoyées en concurrence (comme un vrai test de charge)
    pour que la latence simulée par service n'accumule pas en temps réel.
    """
    def __init__(self, injecteur: InjecteurPanne):
        self._injecteur = injecteur

    def _mesurer(self, charge_fn: Callable[[], Any], nb_requetes: int) -> dict:
        latences: list[float] = []
        echecs = 0
        lock = threading.Lock()

        def une_requete():
            nonlocal echecs
            t0 = time.perf_counter()
            ok = True
            try:
                charge_fn()
            except ServiceException:
                ok = False
            dt = (time.perf_counter() - t0) * 1000
            with lock:
                latences.append(dt)
                if not ok:
                    echecs += 1

        threads = [threading.Thread(target=une_requete) for _ in range(nb_requetes)]
        for t in threads: t.start()
        for t in threads: t.join()

        latences_triees = sorted(latences)
        p99 = latences_triees[min(int(len(latences_triees) * 0.99), len(latences_triees) - 1)] \
              if latences_triees else 0.0

        return {
            "taux_erreur":     round(echecs / max(nb_requetes, 1) * 100, 1),
            "latence_moy_ms":  round(statistics.mean(latences), 1) if latences else 0.0,
            "latence_p99_ms":  round(p99, 1),
        }

    def executer(self, experience: ExperienceChaos, charge_fn: Callable[[], Any],
                 slos: list[SLO], nb_requetes: int = 40) -> ResultatExperience:
        self._injecteur.effacer()
        baseline = self._mesurer(charge_fn, nb_requetes)

        self._injecteur.injecter(experience.cible, experience.type_panne,
                                  experience.probabilite, **experience.parametres)
        sous_panne = self._mesurer(charge_fn, nb_requetes)
        self._injecteur.effacer(experience.cible)

        moniteur = MoniteurSLO(slos)
        incidents = moniteur.violations(sous_panne)
        hypothese_ok = not incidents

        recommendation = (
            f"Hypothèse « {experience.hypothese} » confirmée : SLOs tenus sous panne."
            if hypothese_ok else
            f"Hypothèse « {experience.hypothese} » réfutée : {len(incidents)} SLO(s) violé(s) "
            f"→ faiblesse à corriger avant production."
        )

        return ResultatExperience(
            experience=experience,
            metriques={"baseline": baseline, "sous_panne": sous_panne},
            incidents=incidents,
            hypothese_ok=hypothese_ok,
            recommendation=recommendation,
        )
