"""
Jour 14 — Vector Clocks (Horloges Vectorielles)
=================================================
Problème du Jour 13 : LWW utilise des timestamps physiques.
  → Sensible au clock skew (dérive d'horloge)
  → Ne distingue pas "A a causé B" de "A et B sont concurrents"

Vector Clock (Lamport, 1978 — étendu par Fidge/Mattern, 1988) :
  Chaque nœud maintient un vecteur de compteurs, un par nœud connu.
  VC = {paris: 3, tokyo: 2, sydney: 1}

  Règles :
    1. Événement local   → incrémenter son propre compteur
    2. Envoyer un message → joindre son VC actuel
    3. Recevoir un message → prendre le max de chaque composante
                             puis incrémenter son propre compteur

  Comparaison de deux VCs :
    VC_A < VC_B  (A précède B) :
      ∀i : VC_A[i] ≤ VC_B[i]  ET  ∃j : VC_A[j] < VC_B[j]
      → Causalité : A a (peut-être) causé B

    VC_A ∥ VC_B  (concurrents) :
      ∃i : VC_A[i] > VC_B[i]  ET  ∃j : VC_A[j] < VC_B[j]
      → Conflit réel : aucun des deux ne précède l'autre

    VC_A == VC_B  (identiques) :
      ∀i : VC_A[i] == VC_B[i]

  Lien avec Jour 3 (Lamport Timestamps) :
    Lamport = 1 compteur global → détecte la causalité mais
              peut déclarer concurrents des événements liés
    Vector   = N compteurs      → détection précise, sans faux positifs

Utilisé par : Riak, Amazon DynamoDB (versions internes),
              Git (DAG de commits), CRDTs, Bayou
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum
import threading
import time
import copy


# ─── RELATION CAUSALE ────────────────────────────────────────────────────────

class Relation(Enum):
    PRECEDE    = "PRÉCÈDE"      # VC_A < VC_B  (A a causé B)
    SUIT       = "SUIT"         # VC_A > VC_B  (B a causé A)
    IDENTIQUE  = "IDENTIQUE"    # VC_A == VC_B
    CONCURRENT = "CONCURRENT"   # VC_A ∥ VC_B  (conflit potentiel)


# ─── VECTOR CLOCK ────────────────────────────────────────────────────────────

class VectorClock:
    """
    Un vecteur de compteurs logiques, un par nœud participant.

    Immuable une fois créé (les opérations retournent un nouveau VC).
    Thread-safe pour la lecture, la mutation nécessite un verrou externe.
    """

    def __init__(self, vecteur: dict[str, int] = None):
        self._v: dict[str, int] = dict(vecteur or {})

    # ── Opérations de base ────────────────────────────────────────────────────

    def incrementer(self, noeud_id: str) -> "VectorClock":
        """Règle 1 : événement local → incrémenter son propre compteur."""
        nouveau = dict(self._v)
        nouveau[noeud_id] = nouveau.get(noeud_id, 0) + 1
        return VectorClock(nouveau)

    def merger(self, autre: "VectorClock") -> "VectorClock":
        """
        Règle 3 (réception) : prendre le max de chaque composante.
        NE PAS incrémenter — c'est la responsabilité de l'appelant.
        """
        tous_noeuds = set(self._v) | set(autre._v)
        nouveau = {n: max(self._v.get(n, 0), autre._v.get(n, 0))
                   for n in tous_noeuds}
        return VectorClock(nouveau)

    def recevoir(self, expediteur: str, vc_recu: "VectorClock") -> "VectorClock":
        """
        Règle 2+3 combinées : merger puis incrémenter le récepteur.
        C'est ce qu'un nœud fait quand il reçoit un message.
        """
        return self.merger(vc_recu).incrementer(expediteur)

    def get(self, noeud_id: str) -> int:
        return self._v.get(noeud_id, 0)

    # ── Comparaison ───────────────────────────────────────────────────────────

    def relation_avec(self, autre: "VectorClock") -> Relation:
        """
        Détermine la relation causale entre deux Vector Clocks.

        VC_A < VC_B : tous les compteurs de A ≤ B, au moins un strictement
        VC_A > VC_B : tous les compteurs de A ≥ B, au moins un strictement
        VC_A ∥ VC_B : ni l'un ni l'autre → concurrents
        """
        tous_noeuds = set(self._v) | set(autre._v)

        a_precede = False  # ∃ composante où self < autre
        b_precede = False  # ∃ composante où self > autre

        for n in tous_noeuds:
            va = self._v.get(n, 0)
            vb = autre._v.get(n, 0)
            if va < vb:
                a_precede = True
            elif va > vb:
                b_precede = True

        if not a_precede and not b_precede:
            return Relation.IDENTIQUE
        elif a_precede and not b_precede:
            return Relation.PRECEDE      # self < autre
        elif b_precede and not a_precede:
            return Relation.SUIT         # self > autre
        else:
            return Relation.CONCURRENT   # self ∥ autre — conflit !

    def precede(self, autre: "VectorClock") -> bool:
        return self.relation_avec(autre) == Relation.PRECEDE

    def concurrent_avec(self, autre: "VectorClock") -> bool:
        return self.relation_avec(autre) == Relation.CONCURRENT

    # ── Utilitaires ───────────────────────────────────────────────────────────

    def copie(self) -> "VectorClock":
        return VectorClock(dict(self._v))

    def to_dict(self) -> dict[str, int]:
        return dict(self._v)

    def __repr__(self) -> str:
        if not self._v:
            return "VC{}"
        parties = ", ".join(f"{k}:{v}" for k, v in sorted(self._v.items()))
        return f"VC{{{parties}}}"

    def __eq__(self, autre) -> bool:
        if not isinstance(autre, VectorClock):
            return False
        tous = set(self._v) | set(autre._v)
        return all(self._v.get(n, 0) == autre._v.get(n, 0) for n in tous)


# ─── ENTRÉE VERSIONNÉE AVEC VC ────────────────────────────────────────────────

@dataclass
class EntreeVC:
    """
    Valeur stockée avec son Vector Clock.
    Quand deux entrées sont CONCURRENTES → conflit réel détecté.
    Quand l'une PRÉCÈDE l'autre → mise à jour normale.
    """
    cle:      str
    valeur:   Any
    vc:       VectorClock
    noeud:    str

    def __repr__(self) -> str:
        return f"EntreeVC(val={self.valeur!r}, {self.vc}, src={self.noeud!r})"


# ─── NŒUD AVEC VECTOR CLOCK ──────────────────────────────────────────────────

class NoeudVC:
    """
    Nœud de base de données utilisant des Vector Clocks
    pour la détection précise des conflits.

    Contrairement au Jour 13 (LWW par timestamp),
    on sait ici avec CERTITUDE si deux écritures sont concurrentes
    ou si l'une a causé l'autre.
    """

    def __init__(self, noeud_id: str, latence_ms: float = 10.0):
        self.id      = noeud_id
        self.latence = latence_ms / 1000
        self._vc     = VectorClock({noeud_id: 0})
        self._store: dict[str, list[EntreeVC]] = {}
        # Plusieurs entrées pour une clé = conflit non résolu (siblings)
        self._lock   = threading.RLock()
        self._pairs: dict[str, "NoeudVC"] = {}
        self._actif  = True

        self.stats = {
            "ecritures": 0,
            "lectures": 0,
            "conflits_detectes": 0,
            "replications_recues": 0,
        }
        self.journal_conflits: list[dict] = []

    def enregistrer_pairs(self, pairs: dict[str, "NoeudVC"]):
        self._pairs = {k: v for k, v in pairs.items() if k != self.id}

    # ── Écriture ─────────────────────────────────────────────────────────────

    def ecrire(self, cle: str, valeur: Any,
               vc_contexte: Optional[VectorClock] = None) -> EntreeVC:
        """
        Écriture avec Vector Clock.

        vc_contexte : le VC lu lors de la dernière lecture de cette clé.
        Si fourni, on sait exactement quelle version on met à jour.
        Si absent, on écrit "à l'aveugle" (risque de conflit).
        """
        with self._lock:
            # Incrémenter notre VC local
            self._vc = self._vc.incrementer(self.id)

            if vc_contexte is not None:
                # Merger le contexte fourni par le client
                self._vc = self._vc.merger(vc_contexte).incrementer(self.id)

            nouvelle_entree = EntreeVC(
                cle=cle,
                valeur=valeur,
                vc=self._vc.copie(),
                noeud=self.id,
            )

            # Supprimer les entrées que cette écriture "domine" (précède)
            entrees_actuelles = self._store.get(cle, [])
            survivantes = []
            for e in entrees_actuelles:
                rel = nouvelle_entree.vc.relation_avec(e.vc)
                if rel not in (Relation.SUIT, Relation.PRECEDE):
                    # L'ancienne n'est pas dominée → garder (conflit possible)
                    survivantes.append(e)
                # Si nouvelle_entree.vc > e.vc → e est obsolète, on la supprime

            survivantes.append(nouvelle_entree)
            self._store[cle] = survivantes
            self.stats["ecritures"] += 1

        # Réplication asynchrone
        threading.Thread(
            target=self._repliquer,
            args=(nouvelle_entree,),
            daemon=True,
        ).start()

        return nouvelle_entree

    # ── Lecture ───────────────────────────────────────────────────────────────

    def lire(self, cle: str) -> tuple[list[EntreeVC], bool]:
        """
        Retourne (entrées, a_conflit).

        Si len(entrées) > 1 → conflit non résolu (siblings).
        Le client doit merger les valeurs et réécrire avec le VC fusionné.
        """
        with self._lock:
            self.stats["lectures"] += 1
            entrees = self._store.get(cle, [])
            return list(entrees), len(entrees) > 1

    def lire_valeur(self, cle: str) -> Optional[Any]:
        """Lecture simple : retourne la valeur si pas de conflit."""
        entrees, conflit = self.lire(cle)
        if not entrees:
            return None
        if conflit:
            return [e.valeur for e in entrees]  # Retourner tous les siblings
        return entrees[0].valeur

    # ── Réplication ──────────────────────────────────────────────────────────

    def _repliquer(self, entree: EntreeVC):
        import random
        time.sleep(self.latence * random.uniform(0.5, 1.5))
        for pair in self._pairs.values():
            if pair._actif:
                pair.recevoir_replication(entree)

    def recevoir_replication(self, distante: EntreeVC):
        """
        Reçoit une réplication. Applique les règles VC :
          - Si distante.vc > locale.vc → mettre à jour (causalité)
          - Si distante.vc ∥ locale.vc → CONFLIT réel → garder les deux (siblings)
          - Si distante.vc < locale.vc → ignorer (ancienne version)
        """
        with self._lock:
            self.stats["replications_recues"] += 1
            # Merger le VC reçu dans notre état global
            self._vc = self._vc.merger(distante.vc)

            entrees = self._store.get(distante.cle, [])

            if not entrees:
                self._store[distante.cle] = [distante]
                return

            nouvelles = []
            conflit_detecte = False

            for locale in entrees:
                rel = distante.vc.relation_avec(locale.vc)

                if rel == Relation.PRECEDE:
                    # distante < locale → distante est obsolète, ignorer
                    nouvelles.append(locale)

                elif rel == Relation.SUIT:
                    # distante > locale → distante est plus récente, remplacer
                    nouvelles.append(distante)

                elif rel == Relation.IDENTIQUE:
                    nouvelles.append(locale)

                else:  # CONCURRENT → CONFLIT
                    conflit_detecte = True
                    nouvelles.append(locale)
                    nouvelles.append(distante)
                    self.stats["conflits_detectes"] += 1
                    self.journal_conflits.append({
                        "cle": distante.cle,
                        "locale": locale,
                        "distante": distante,
                        "ts": time.time(),
                    })

            # Dédoublonner (même VC = même version)
            vus = set()
            dedup = []
            for e in nouvelles:
                key = repr(e.vc)
                if key not in vus:
                    vus.add(key)
                    dedup.append(e)

            self._store[distante.cle] = dedup

    def resoudre_conflit(self, cle: str, valeur_mergee: Any,
                         vc_contexte: Optional[VectorClock] = None) -> EntreeVC:
        """
        Résoudre un conflit : l'application a mergé les valeurs
        et réécrit avec un VC qui domine tous les siblings.
        """
        # Construire un VC qui "suit" tous les siblings
        with self._lock:
            entrees = self._store.get(cle, [])
            vc_fusion = VectorClock()
            for e in entrees:
                vc_fusion = vc_fusion.merger(e.vc)
        return self.ecrire(cle, valeur_mergee, vc_contexte=vc_fusion)

    def etat_store(self) -> dict:
        with self._lock:
            return {k: [e.valeur for e in v] for k, v in self._store.items()}

    def nb_conflits(self) -> int:
        return self.stats["conflits_detectes"]
