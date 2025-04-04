"""Jour 28 - Chaos Engineering core"""
from __future__ import annotations
import time, random, threading
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from collections import defaultdict
from enum import Enum

class TypePanne(Enum):
    LATENCE      = "latence"
    ERREUR       = "erreur"
    CRASH        = "crash"
    PERTE_PAQUET = "perte_paquet"

@dataclass
class ConfigPanne:
    type:        TypePanne
    cible:       str
    intensite:   float
    duree_s:     float
    latence_ms:  float = 0.0
    code_erreur: int   = 500
    description: str   = ""

@dataclass
class MetriquesService:
    service:        str
    requetes_total: int  = 0
    requetes_ok:    int  = 0
    requetes_err:   int  = 0
    latences_ms:    list = field(default_factory=list)
    periodes:       list = field(default_factory=list)

    @property
    def taux_erreur(self):
        return (self.requetes_err / max(self.requetes_total, 1)) * 100

    @property
    def disponibilite(self):
        return 100.0 - self.taux_erreur

    @property
    def latence_p99(self):
        if not self.latences_ms: return 0.0
        s = sorted(self.latences_ms)
        return s[min(int(len(s)*0.99), len(s)-1)]

    def snapshot(self, label):
        self.periodes.append({"label": label,
            "taux_erreur": round(self.taux_erreur, 2),
            "dispo": round(self.disponibilite, 2),
            "p99_ms": round(self.latence_p99, 2),
            "total": self.requetes_total})

    def reset(self):
        self.requetes_total = self.requetes_ok = self.requetes_err = 0
        self.latences_ms = []

class Service:
    def __init__(self, nom, base_latence_ms=20.0, taux_erreur_nominal_pct=0.5):
        self.nom = nom
        self._base = base_latence_ms
        self._nominal_err = taux_erreur_nominal_pct
        self._panne = None
        self._lock = threading.Lock()
        self._en_vie = True
        self.metriques = MetriquesService(nom)

    def injecter(self, cfg):
        with self._lock:
            self._panne = cfg
            self._en_vie = (cfg.type != TypePanne.CRASH)

    def retirer(self):
        with self._lock:
            self._panne = None
            self._en_vie = True

    def appeler(self):
        t0 = time.perf_counter()
        with self._lock:
            en_vie = self._en_vie
            panne = self._panne

        if not en_vie:
            ms = (time.perf_counter()-t0)*1000 + 1.0
            self._rec(False, ms)
            return {"ok": False, "status": 503, "erreur": f"{self.nom} crashe"}

        base_ms = max(0.5, random.gauss(self._base, self._base*0.15))

        if panne and random.random() < panne.intensite:
            if panne.type == TypePanne.LATENCE:
                time.sleep((base_ms + panne.latence_ms)/1000)
                ms = (time.perf_counter()-t0)*1000
                ok = ms < 3000
                self._rec(ok, ms)
                return ({"ok": True, "status": 200, "latence_ms": ms} if ok
                        else {"ok": False, "status": 504, "erreur": f"Timeout {ms:.0f}ms"})
            elif panne.type == TypePanne.ERREUR:
                time.sleep(base_ms/1000)
                ms = (time.perf_counter()-t0)*1000
                self._rec(False, ms)
                return {"ok": False, "status": panne.code_erreur, "erreur": f"Erreur {panne.code_erreur}"}
            elif panne.type == TypePanne.PERTE_PAQUET:
                ms = (time.perf_counter()-t0)*1000 + 0.5
                self._rec(False, ms)
                return {"ok": False, "status": 0, "erreur": "Paquet perdu"}

        if random.random() < self._nominal_err/100:
            time.sleep(base_ms/1000)
            ms = (time.perf_counter()-t0)*1000
            self._rec(False, ms)
            return {"ok": False, "status": 500, "erreur": "Erreur transitoire"}

        time.sleep(base_ms/1000)
        ms = (time.perf_counter()-t0)*1000
        self._rec(True, ms)
        return {"ok": True, "status": 200, "latence_ms": ms}

    def _rec(self, ok, ms):
        m = self.metriques
        m.requetes_total += 1
        m.requetes_ok += int(ok)
        m.requetes_err += int(not ok)
        m.latences_ms.append(ms)

class CircuitBreaker:
    class Etat(Enum):
        CLOSED    = "CLOSED"
        OPEN      = "OPEN"
        HALF_OPEN = "HALF_OPEN"

    def __init__(self, svc, seuil_pct=50.0, fenetre=10, timeout_s=1.5):
        self._svc = svc
        self._seuil = seuil_pct
        self._fenetre = fenetre
        self._timeout = timeout_s
        self._etat = self.Etat.CLOSED
        self._resultats = []
        self._ouvert_a = 0.0
        self._stats = defaultdict(int)

    @property
    def etat(self):
        return self._etat.value

    def appeler(self):
        if self._etat == self.Etat.OPEN:
            if time.time() - self._ouvert_a > self._timeout:
                self._etat = self.Etat.HALF_OPEN
            else:
                self._stats["court_circuit"] += 1
                return {"ok": False, "status": 503, "erreur": "Circuit ouvert"}
        rep = self._svc.appeler()
        self._resultats.append(rep["ok"])
        if len(self._resultats) > self._fenetre:
            self._resultats.pop(0)
        if self._etat == self.Etat.HALF_OPEN:
            if rep["ok"]:
                self._etat = self.Etat.CLOSED
                self._stats["fermetures"] += 1
            else:
                self._etat = self.Etat.OPEN
                self._ouvert_a = time.time()
        elif len(self._resultats) >= self._fenetre:
            taux = self._resultats.count(False)/len(self._resultats)*100
            if taux >= self._seuil:
                self._etat = self.Etat.OPEN
                self._ouvert_a = time.time()
                self._stats["ouvertures"] += 1
        return rep

    def stats(self):
        return {**dict(self._stats), "etat": self._etat.value}

class InjecteurChaos:
    def __init__(self, services):
        self._services = services
        self._historique = []

    def mesurer(self, nb, label):
        for svc in self._services.values():
            svc.metriques.reset()
        for _ in range(nb):
            for svc in self._services.values():
                svc.appeler()
        res = {}
        for nom, svc in self._services.items():
            svc.metriques.snapshot(label)
            res[nom] = {"taux_erreur": round(svc.metriques.taux_erreur,2),
                        "dispo": round(svc.metriques.disponibilite,2),
                        "p99_ms": round(svc.metriques.latence_p99,2)}
        return res

    def experiment(self, cfg, nb=80, abort_pct=80.0):
        cible = self._services.get(cfg.cible)
        if not cible:
            return {"erreur": f"Service inconnu: {cfg.cible}"}
        baseline = self.mesurer(nb, "baseline")
        cible.injecter(cfg)
        for svc in self._services.values():
            svc.metriques.reset()
        aborted = False
        for i in range(nb):
            for svc in self._services.values():
                svc.appeler()
            if i == 25 and cible.metriques.taux_erreur > abort_pct:
                aborted = True
                break
        sous_panne = {}
        for nom, svc in self._services.items():
            svc.metriques.snapshot("sous_panne")
            sous_panne[nom] = {"taux_erreur": round(svc.metriques.taux_erreur,2),
                                "dispo": round(svc.metriques.disponibilite,2),
                                "p99_ms": round(svc.metriques.latence_p99,2)}
        cible.retirer()
        apres = self.mesurer(nb, "recuperation")
        res = {"panne": cfg, "aborted": aborted,
               "baseline": baseline, "sous_panne": sous_panne, "apres": apres}
        self._historique.append(res)
        return res
