"""
Jour 13 — Réplication Multi-Maître
=====================================
Réplication classique (single-master) :
  Un seul nœud accepte les écritures → les réplique sur les secondaires.
  Limitation : le maître est un goulot d'étranglement et un SPOF.

Réplication multi-maître :
  Chaque nœud accepte les écritures localement.
  Chaque nœud réplique ses écritures sur les autres.
  → Haute disponibilité, écritures géo-distribuées (chaque DC écrit localement)
  → Problème : deux maîtres peuvent recevoir des écritures contradictoires
    sur la même clé au même instant → CONFLIT.

Les 4 stratégies de résolution de conflits :

  1. Last-Write-Wins (LWW)
     Le timestamp le plus récent gagne.
     ✅ Simple, automatique
     ❌ Perd silencieusement des données
     ❌ Les horloges des serveurs ne sont jamais parfaitement sync
     Utilisé par : Cassandra (par défaut), DynamoDB

  2. First-Write-Wins (FWW)
     Le premier écrit gagne, les suivants sont rejetés.
     ✅ Prévisible
     ❌ Les retards réseau peuvent rendre arbitraire qui "gagne"
     Utilisé par : certains systèmes de réservation

  3. Merge automatique
     Les valeurs sont combinées algorithmiquement.
     ✅ Aucune donnée perdue
     ✅ Fonctionne bien pour compteurs, ensembles, listes
     ❌ Impossible pour tous les types de données
     Utilisé par : Git (merge), Google Docs (OT/CRDT)

  4. Résolution applicative (Custom)
     Le conflit est remonté à l'application pour décision.
     ✅ Logique métier respectée
     ❌ Plus complexe à implémenter
     Utilisé par : CouchDB, DynamoDB (mode personnalisé), Riak

  Vector Clocks (Jour 14) permettent de DÉTECTER précisément
  quand il y a un conflit (vs. une simple succession d'écritures).
"""

import time
import threading
import random
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Callable


# ─── STRATÉGIE DE RÉSOLUTION ─────────────────────────────────────────────────

class StrategieConflit(Enum):
    LWW      = "LWW"       # Last-Write-Wins
    FWW      = "FWW"       # First-Write-Wins
    MERGE    = "MERGE"     # Merge automatique
    CUSTOM   = "CUSTOM"    # Résolution applicative


# ─── ENREGISTREMENT VERSIONNÉ ────────────────────────────────────────────────

@dataclass
class EntreeVersionnee:
    """
    Valeur avec métadonnées pour détecter et résoudre les conflits.
    """
    cle:       str
    valeur:    Any
    ts:        float        # Timestamp physique (horloge du nœud)
    noeud_src: str          # Nœud qui a écrit en premier
    version:   int = 1      # Numéro de version local

    def __repr__(self):
        return f"EntreeVersionnee(cle={self.cle!r}, valeur={self.valeur!r}, ts={self.ts:.3f}, src={self.noeud_src!r}, v={self.version})"


# ─── ÉVÉNEMENT DE CONFLIT ────────────────────────────────────────────────────

@dataclass
class EvenementConflit:
    """Trace d'un conflit détecté et de sa résolution."""
    ts:         float
    cle:        str
    locale:     EntreeVersionnee
    distante:   EntreeVersionnee
    gagnant:    EntreeVersionnee
    strategie:  StrategieConflit
    noeud_id:   str

    def resume(self) -> str:
        return (f"CONFLIT sur '{self.cle}' : "
                f"local(v={self.locale.valeur!r} @{self.locale.noeud_src}) "
                f"vs distant(v={self.distante.valeur!r} @{self.distante.noeud_src}) "
                f"→ {self.strategie.value} → {self.gagnant.valeur!r}")


# ─── NŒUD MULTI-MAÎTRE ───────────────────────────────────────────────────────

class NoeudMultiMaitre:
    """
    Nœud dans un cluster multi-maître.

    Accepte les écritures localement (toujours).
    Réplique de façon asynchrone vers les autres nœuds.
    Résout les conflits selon la stratégie configurée.
    """

    def __init__(
        self,
        noeud_id: str,
        strategie: StrategieConflit = StrategieConflit.LWW,
        latence_ms: float = 15.0,
        fn_merge: Optional[Callable] = None,
        fn_custom: Optional[Callable] = None,
        skew_ms: float = 0.0,   # Décalage d'horloge simulé
    ):
        self.id        = noeud_id
        self.strategie = strategie
        self.latence   = latence_ms / 1000
        self.skew      = skew_ms / 1000   # Dérive de l'horloge locale
        self.fn_merge  = fn_merge
        self.fn_custom = fn_custom

        self._store: dict[str, EntreeVersionnee] = {}
        self._lock   = threading.RLock()
        self._pairs: dict[str, "NoeudMultiMaitre"] = {}
        self._actif  = True

        # Journal des conflits
        self.conflits: list[EvenementConflit] = []
        self.stats = {
            "ecritures_locales":  0,
            "replications_recues": 0,
            "conflits_detectes":  0,
            "conflits_lww":       0,
            "conflits_merge":     0,
            "conflits_custom":    0,
        }

    def _now(self) -> float:
        """Heure locale avec dérive simulée."""
        return time.time() + self.skew

    def enregistrer_pairs(self, pairs: dict[str, "NoeudMultiMaitre"]):
        self._pairs = {k: v for k, v in pairs.items() if k != self.id}

    # ── Écriture locale ───────────────────────────────────────────────────────

    def ecrire(self, cle: str, valeur: Any) -> EntreeVersionnee:
        """
        Écriture locale : toujours acceptée immédiatement.
        Réplication asynchrone vers les pairs.
        """
        with self._lock:
            version_actuelle = self._store.get(cle)
            nouvelle_version = (version_actuelle.version + 1) if version_actuelle else 1
            entree = EntreeVersionnee(
                cle=cle,
                valeur=valeur,
                ts=self._now(),
                noeud_src=self.id,
                version=nouvelle_version,
            )
            self._store[cle] = entree
            self.stats["ecritures_locales"] += 1

        # Réplication asynchrone
        threading.Thread(
            target=self._repliquer_vers_tous,
            args=(entree,),
            daemon=True,
        ).start()

        return entree

    def lire(self, cle: str) -> Optional[Any]:
        with self._lock:
            e = self._store.get(cle)
            return e.valeur if e else None

    def lire_entree(self, cle: str) -> Optional[EntreeVersionnee]:
        with self._lock:
            return self._store.get(cle)

    # ── Réplication ───────────────────────────────────────────────────────────

    def _repliquer_vers_tous(self, entree: EntreeVersionnee):
        for pair in self._pairs.values():
            if pair._actif:
                time.sleep(self.latence * random.uniform(0.5, 1.5))
                pair.recevoir_replication(entree)

    def recevoir_replication(self, distante: EntreeVersionnee):
        """
        Reçoit une réplication d'un pair.
        Si la clé existe déjà avec une valeur différente → CONFLIT.
        """
        with self._lock:
            self.stats["replications_recues"] += 1
            locale = self._store.get(distante.cle)

            if locale is None:
                # Pas de conflit : nouvelle clé
                self._store[distante.cle] = distante
                return

            if locale.valeur == distante.valeur:
                # Même valeur : pas de conflit
                return

            if locale.noeud_src == distante.noeud_src:
                # Même source : garder la version la plus récente
                if distante.version > locale.version:
                    self._store[distante.cle] = distante
                return

            # CONFLIT RÉEL : deux nœuds différents ont écrit des valeurs différentes
            self.stats["conflits_detectes"] += 1
            gagnant = self._resoudre_conflit(locale, distante)
            self._store[distante.cle] = gagnant

            self.conflits.append(EvenementConflit(
                ts=time.time(),
                cle=distante.cle,
                locale=locale,
                distante=distante,
                gagnant=gagnant,
                strategie=self.strategie,
                noeud_id=self.id,
            ))

    # ── Résolution de conflits ────────────────────────────────────────────────

    def _resoudre_conflit(
        self, locale: EntreeVersionnee, distante: EntreeVersionnee
    ) -> EntreeVersionnee:

        if self.strategie == StrategieConflit.LWW:
            return self._lww(locale, distante)

        elif self.strategie == StrategieConflit.FWW:
            return self._fww(locale, distante)

        elif self.strategie == StrategieConflit.MERGE:
            return self._merge(locale, distante)

        elif self.strategie == StrategieConflit.CUSTOM:
            return self._custom(locale, distante)

        return locale

    def _lww(self, locale: EntreeVersionnee, distante: EntreeVersionnee) -> EntreeVersionnee:
        """Last-Write-Wins : le timestamp le plus récent gagne."""
        self.stats["conflits_lww"] += 1
        return distante if distante.ts > locale.ts else locale

    def _fww(self, locale: EntreeVersionnee, distante: EntreeVersionnee) -> EntreeVersionnee:
        """First-Write-Wins : le timestamp le plus ancien gagne."""
        return locale if locale.ts <= distante.ts else distante

    def _merge(self, locale: EntreeVersionnee, distante: EntreeVersionnee) -> EntreeVersionnee:
        """
        Merge automatique. Délègue à fn_merge si fournie.
        Fallback : fusion de listes, addition de compteurs, LWW sinon.
        """
        self.stats["conflits_merge"] += 1

        if self.fn_merge:
            valeur_mergee = self.fn_merge(locale.valeur, distante.valeur)
        elif isinstance(locale.valeur, list) and isinstance(distante.valeur, list):
            # Union de listes (ordre-insensible)
            seen = set()
            valeur_mergee = []
            for v in locale.valeur + distante.valeur:
                key = str(v)
                if key not in seen:
                    seen.add(key)
                    valeur_mergee.append(v)
        elif isinstance(locale.valeur, dict) and isinstance(distante.valeur, dict):
            # Merge de dicts : LWW par clé
            valeur_mergee = {**locale.valeur, **distante.valeur}
        elif isinstance(locale.valeur, (int, float)) and isinstance(distante.valeur, (int, float)):
            # Pour les compteurs : prendre le max (converge vers la vraie valeur)
            valeur_mergee = max(locale.valeur, distante.valeur)
        else:
            # Fallback LWW
            return self._lww(locale, distante)

        return EntreeVersionnee(
            cle=locale.cle,
            valeur=valeur_mergee,
            ts=max(locale.ts, distante.ts),
            noeud_src=f"merge({locale.noeud_src},{distante.noeud_src})",
            version=max(locale.version, distante.version) + 1,
        )

    def _custom(self, locale: EntreeVersionnee, distante: EntreeVersionnee) -> EntreeVersionnee:
        """Résolution applicative : délègue à fn_custom."""
        self.stats["conflits_custom"] += 1
        if self.fn_custom:
            valeur = self.fn_custom(locale, distante)
            return EntreeVersionnee(
                cle=locale.cle,
                valeur=valeur,
                ts=max(locale.ts, distante.ts),
                noeud_src=f"custom",
                version=max(locale.version, distante.version) + 1,
            )
        return self._lww(locale, distante)

    # ── Utilitaires ───────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {k: e.valeur for k, e in self._store.items()}

    def est_coherent_avec(self, autre: "NoeudMultiMaitre", cle: str) -> bool:
        return self.lire(cle) == autre.lire(cle)

    def nb_conflits(self) -> int:
        return self.stats["conflits_detectes"]


# ─── CLUSTER MULTI-MAÎTRE ────────────────────────────────────────────────────

class ClusterMultiMaitre:
    """Gère un ensemble de nœuds multi-maîtres."""

    def __init__(self, noeud_ids: list[str], strategie: StrategieConflit,
                 latence_ms: float = 15.0, skews_ms: dict = None,
                 fn_merge=None, fn_custom=None):
        self.noeuds: dict[str, NoeudMultiMaitre] = {}
        for nid in noeud_ids:
            skew = (skews_ms or {}).get(nid, 0.0)
            self.noeuds[nid] = NoeudMultiMaitre(
                nid, strategie=strategie, latence_ms=latence_ms,
                fn_merge=fn_merge, fn_custom=fn_custom, skew_ms=skew,
            )
        pairs = self.noeuds
        for n in self.noeuds.values():
            n.enregistrer_pairs(pairs)

    def attendre_convergence(self, cle: str, timeout: float = 3.0) -> bool:
        """Attend que tous les nœuds aient la même valeur pour une clé."""
        debut = time.time()
        while time.time() - debut < timeout:
            valeurs = [str(n.lire(cle)) for n in self.noeuds.values()]
            if len(set(valeurs)) == 1:
                return True
            time.sleep(0.05)
        return False

    def valeurs(self, cle: str) -> dict[str, Any]:
        return {nid: n.lire(cle) for nid, n in self.noeuds.items()}

    def nb_conflits_total(self) -> int:
        return sum(n.nb_conflits() for n in self.noeuds.values())

    def coherence_globale(self, cle: str) -> bool:
        valeurs = [str(n.lire(cle)) for n in self.noeuds.values()]
        return len(valeurs) == 1
