"""
Jour 9 — Quorum (Lecture/Écriture)
=====================================
Le Jour 8 présentait CP et AP comme un choix binaire.
La réalité est plus fine : Cassandra, DynamoDB, Riak exposent
un CURSEUR entre cohérence et disponibilité via les niveaux W et R.

  N = nombre total de nœuds de répliques
  W = nombre de nœuds qui doivent confirmer une ÉCRITURE
  R = nombre de nœuds qui doivent répondre à une LECTURE

Règle fondamentale :
  W + R > N  →  cohérence forte (toute lecture voit la dernière écriture)
  W + R ≤ N  →  cohérence éventuelle (stale reads possibles)

Preuve intuitive de W + R > N :
  Si on écrit sur W nœuds et lit depuis R nœuds,
  et que W + R > N, alors au moins 1 nœud est dans les DEUX ensembles.
  Ce nœud a forcément la dernière valeur écrite.
  → La lecture retournera toujours la valeur la plus récente.

Combinaisons classiques (N=3) :
  W=1, R=1  → latence minimale, aucune cohérence garantie
  W=1, R=3  → lectures cohérentes, écritures rapides (W+R=4 > 3 ✅)
  W=3, R=1  → écritures sûres, lectures rapides (W+R=4 > 3 ✅)
  W=2, R=2  → équilibre (QUORUM) (W+R=4 > 3 ✅)
  W=3, R=3  → cohérence maximale, latence maximale
"""

import time
import threading
import random
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum


# ─── VERSION D'UNE VALEUR ────────────────────────────────────────────────────

@dataclass
class Versioned:
    """Valeur avec timestamp (pour Last-Write-Wins lors du merge de lecture)."""
    valeur:    Any
    timestamp: float
    noeud_id:  int

    def __gt__(self, other: "Versioned") -> bool:
        return self.timestamp > other.timestamp


# ─── RÉSULTAT D'OPÉRATION ────────────────────────────────────────────────────

class StatutOp(Enum):
    OK           = "OK"
    TIMEOUT      = "TIMEOUT"      # Pas assez de nœuds ont répondu
    INCOHERENCE  = "INCOHERENCE"  # Les nœuds ont retourné des valeurs différentes


@dataclass
class ResultatQuorum:
    statut:        StatutOp
    valeur:        Any            = None
    reponses:      int            = 0     # Combien de nœuds ont répondu
    requis:        int            = 0     # W ou R requis
    latence_ms:    float          = 0.0
    incoherences:  int            = 0     # Nœuds avec valeur différente


# ─── NŒUD DE RÉPLIQUE ────────────────────────────────────────────────────────

class NoeudReplique:
    """
    Simule un nœud de réplication.
    Peut être lent (latence variable) ou défaillant.
    """

    def __init__(self, noeud_id: int, latence_ms: float = 5.0, fiabilite: float = 1.0):
        self.id        = noeud_id
        self.latence   = latence_ms / 1000
        self.fiabilite = fiabilite   # 1.0 = 100% fiable, 0.8 = 20% de pannes
        self._store: dict[str, Versioned] = {}
        self._lock  = threading.Lock()
        self._actif = True

    def ecrire(self, cle: str, valeur: Any, timestamp: float) -> bool:
        """Simule l'écriture avec latence variable et fiabilité."""
        if not self._actif or random.random() > self.fiabilite:
            return False
        # Latence variable (±50% pour simuler la variabilité réseau)
        time.sleep(self.latence * random.uniform(0.5, 1.5))
        with self._lock:
            existant = self._store.get(cle)
            # LWW : on accepte seulement les timestamps plus récents
            if existant is None or timestamp > existant.timestamp:
                self._store[cle] = Versioned(valeur, timestamp, self.id)
        return True

    def lire(self, cle: str) -> Optional[Versioned]:
        """Simule la lecture avec latence variable."""
        if not self._actif or random.random() > self.fiabilite:
            return None
        time.sleep(self.latence * random.uniform(0.5, 1.5))
        with self._lock:
            return self._store.get(cle)

    def etat(self, cle: str) -> str:
        with self._lock:
            v = self._store.get(cle)
            return f"{v.valeur!r}@t={v.timestamp:.3f}" if v else "∅"


# ─── COORDINATEUR DE QUORUM ───────────────────────────────────────────────────

class CoordinateurQuorum:
    """
    Implémente la logique de quorum W/R sur un ensemble de répliques.

    Le coordinateur :
      1. Envoie les requêtes à TOUS les nœuds en parallèle
      2. Attend les W (ou R) premières réponses
      3. Retourne dès que le quorum est atteint (sloppy quorum)
      4. Continue de collecter pour la réparation en arrière-plan (read repair)
    """

    def __init__(self, noeuds: list[NoeudReplique], W: int, R: int, timeout_ms: float = 500):
        self.noeuds   = noeuds
        self.N        = len(noeuds)
        self.W        = W
        self.R        = R
        self.timeout  = timeout_ms / 1000

        # Vérification de la configuration
        assert 1 <= W <= self.N, f"W={W} invalide pour N={self.N}"
        assert 1 <= R <= self.N, f"R={R} invalide pour N={self.N}"

        self.coherence_forte = (W + R) > self.N

        # Statistiques
        self.stats = {
            "ecritures_ok":      0,
            "ecritures_timeout": 0,
            "lectures_ok":       0,
            "lectures_timeout":  0,
            "read_repairs":      0,
            "incoherences_lues": 0,
        }

    def ecrire(self, cle: str, valeur: Any) -> ResultatQuorum:
        """
        Écriture avec quorum W.
        Envoie à tous, attend W confirmations, abandonne après timeout.
        """
        debut     = time.perf_counter()
        timestamp = time.time()

        confirmations = [0]
        echecs        = [0]
        lock          = threading.Lock()
        quorum_evt    = threading.Event()

        def tenter_ecriture(noeud: NoeudReplique):
            succes = noeud.ecrire(cle, valeur, timestamp)
            with lock:
                if succes:
                    confirmations[0] += 1
                    if confirmations[0] >= self.W:
                        quorum_evt.set()
                else:
                    echecs[0] += 1

        threads = [threading.Thread(target=tenter_ecriture, args=(n,), daemon=True)
                   for n in self.noeuds]
        for t in threads: t.start()

        atteint = quorum_evt.wait(timeout=self.timeout)
        latence = (time.perf_counter() - debut) * 1000

        if atteint:
            self.stats["ecritures_ok"] += 1
            return ResultatQuorum(
                statut=StatutOp.OK,
                valeur=valeur,
                reponses=confirmations[0],
                requis=self.W,
                latence_ms=latence,
            )
        else:
            self.stats["ecritures_timeout"] += 1
            return ResultatQuorum(
                statut=StatutOp.TIMEOUT,
                reponses=confirmations[0],
                requis=self.W,
                latence_ms=latence,
            )

    def lire(self, cle: str, read_repair: bool = True) -> ResultatQuorum:
        """
        Lecture avec quorum R.

        1. Collecte R réponses
        2. Retourne la valeur avec le timestamp le plus récent (LWW)
        3. Si des nœuds ont une valeur ancienne → Read Repair en async

        Read Repair : mécanisme clé de Cassandra.
        Quand on lit et qu'un nœud retourne une vieille valeur,
        on lui renvoie silencieusement la valeur à jour.
        → La cohérence s'améliore au fur et à mesure des lectures.
        """
        debut     = time.perf_counter()
        reponses  = []
        lock      = threading.Lock()
        quorum_evt = threading.Event()
        toutes_reponses = []

        def tenter_lecture(noeud: NoeudReplique):
            versioned = noeud.lire(cle)
            with lock:
                if versioned is not None:
                    reponses.append((noeud, versioned))
                    toutes_reponses.append((noeud, versioned))
                    if len(reponses) >= self.R:
                        quorum_evt.set()
                else:
                    toutes_reponses.append((noeud, None))

        threads = [threading.Thread(target=tenter_lecture, args=(n,), daemon=True)
                   for n in self.noeuds]
        for t in threads: t.start()

        atteint = quorum_evt.wait(timeout=self.timeout)
        latence = (time.perf_counter() - debut) * 1000

        if not atteint or not reponses:
            self.stats["lectures_timeout"] += 1
            return ResultatQuorum(
                statut=StatutOp.TIMEOUT,
                reponses=len(reponses),
                requis=self.R,
                latence_ms=latence,
            )

        # Valeur la plus récente parmi les R réponses
        meilleure = max(r for _, r in reponses)
        valeurs_distinctes = len(set(r.valeur for _, r in reponses if r))

        # Détecter les incohérences
        incoherences = sum(
            1 for _, r in reponses
            if r and r.valeur != meilleure.valeur
        )
        if incoherences > 0:
            self.stats["incoherences_lues"] += 1

        # Read Repair en arrière-plan
        if read_repair:
            threading.Thread(
                target=self._read_repair,
                args=(cle, meilleure, toutes_reponses),
                daemon=True
            ).start()

        self.stats["lectures_ok"] += 1
        return ResultatQuorum(
            statut=StatutOp.OK,
            valeur=meilleure.valeur,
            reponses=len(reponses),
            requis=self.R,
            latence_ms=latence,
            incoherences=incoherences,
        )

    def _read_repair(self, cle: str, meilleure: Versioned,
                     toutes: list[tuple[NoeudReplique, Optional[Versioned]]]):
        """
        Répare silencieusement les nœuds en retard.
        Appelé en arrière-plan après chaque lecture.
        """
        repares = 0
        for noeud, versioned in toutes:
            if versioned is None or versioned.timestamp < meilleure.timestamp:
                noeud.ecrire(cle, meilleure.valeur, meilleure.timestamp)
                repares += 1
        if repares > 0:
            self.stats["read_repairs"] += repares

    def rapport_config(self) -> str:
        coherence = "FORTE ✅" if self.coherence_forte else "ÉVENTUELLE ⚠️"
        return (
            f"N={self.N}, W={self.W}, R={self.R}  "
            f"W+R={self.W+self.R} {'>' if self.coherence_forte else '≤'} N={self.N}  "
            f"→ Cohérence {coherence}"
        )

    def rapport_stats(self) -> str:
        s = self.stats
        return (
            f"  Écritures OK={s['ecritures_ok']} timeout={s['ecritures_timeout']}\n"
            f"  Lectures  OK={s['lectures_ok']}  timeout={s['lectures_timeout']}  "
            f"incohérences={s['incoherences_lues']}\n"
            f"  Read repairs effectués : {s['read_repairs']}"
        )
