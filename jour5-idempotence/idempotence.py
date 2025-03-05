"""
Jour 5 — Idempotence
=====================
Problème : Le client envoie une requête de paiement.
Le réseau coupe. Le client ne sait pas si le serveur
a traité la requête. Il la renvoie. Résultat : 2 paiements.

Ou : le Heartbeat du Jour 4 déclare un nœud mort.
Le client bascule sur un autre nœud et renvoie.
Si la 1ère requête avait quand même abouti → doublon.

Solution : Idempotence
  Une opération est idempotente si l'appliquer N fois
  produit le même résultat que l'appliquer 1 fois.

  f(f(x)) = f(x)   ←   propriété mathématique

Mécanisme : Clé d'idempotence (Idempotency Key)
  Le CLIENT génère un UUID unique par intention métier.
  Il envoie cette clé avec chaque tentative.
  Le SERVEUR mémorise les résultats et les rejoue
  sans ré-exécuter l'opération si la clé est connue.

Utilisé par :  Stripe, Braintree, PayPal, AWS, Twilio
"""

import time
import uuid
import threading
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Callable


# ─── ÉTAT D'UNE REQUÊTE IDEMPOTENTE ──────────────────────────────────────────

class StatutRequete(Enum):
    EN_COURS   = "EN_COURS"    # Traitement en cours (verrou actif)
    COMPLETE   = "COMPLETE"    # Résultat disponible
    ERREUR     = "ERREUR"      # Erreur définitive (aussi mémorisée !)


@dataclass
class EntreeIdempotence:
    """
    Ce que le serveur mémorise pour chaque clé d'idempotence.
    """
    cle:          str
    statut:       StatutRequete
    resultat:     Any             = None
    erreur:       Optional[str]   = None
    cree_le:      float           = field(default_factory=time.time)
    complete_le:  Optional[float] = None
    nb_tentatives: int            = 0       # Combien de fois on a reçu cette clé
    hash_requete: Optional[str]   = None    # Hash du payload (détecte les mutations)

    def age(self) -> float:
        return time.time() - self.cree_le


# ─── STORE D'IDEMPOTENCE ─────────────────────────────────────────────────────

class StoreIdempotence:
    """
    Composant central : mémorise les résultats par clé d'idempotence.

    En production : Redis avec TTL
      SET idempotency:{key} {result} EX 86400
      SETNX pour le verrou (atomique)

    Ici : dict en mémoire thread-safe (même sémantique).

    Subtilités implémentées :
      1. Verrou par clé  → deux requêtes simultanées avec la même clé
                           → la 2ème attend le résultat de la 1ère
      2. TTL             → nettoyage automatique des vieilles entrées
      3. Hash payload    → détecte si le client renvoie une clé avec
                           un payload DIFFÉRENT (erreur client)
    """

    def __init__(self, ttl_secondes: float = 86400):  # 24h par défaut
        self._store: dict[str, EntreeIdempotence] = {}
        self._verrous: dict[str, threading.Event] = {}
        self._lock = threading.RLock()
        self.ttl = ttl_secondes

        # Statistiques
        self.stats = {
            "nouvelles":   0,   # Premières exécutions
            "replays":     0,   # Réponses rejouées depuis le store
            "en_attente":  0,   # Requêtes qui ont attendu un verrou
            "conflits":    0,   # Clés avec payload différent
        }

    @staticmethod
    def _hasher_payload(payload: dict) -> str:
        """Hash déterministe du payload pour détecter les mutations."""
        canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def obtenir_ou_reserver(
        self,
        cle: str,
        payload: dict,
    ) -> tuple[bool, Optional[EntreeIdempotence]]:
        """
        Tente de réserver une clé pour traitement.

        Retourne (True, None)    → clé nouvelle, traitement autorisé
        Retourne (False, entree) → clé connue, rejouer le résultat
        
        Si la clé est EN_COURS (autre thread la traite) → attend.

        Implémente le pattern "Check-then-Act" atomique.
        """
        hash_payload = self._hasher_payload(payload)

        with self._lock:
            # Nettoyage TTL opportuniste
            self._nettoyer_expires()

            # Clé déjà connue ?
            if cle in self._store:
                entree = self._store[cle]
                entree.nb_tentatives += 1

                # Détection de mutation : même clé, payload différent
                if entree.hash_requete and entree.hash_requete != hash_payload:
                    self.stats["conflits"] += 1
                    raise ValueError(
                        f"Conflit d'idempotence : la clé '{cle}' existe déjà "
                        f"avec un payload différent. Utilisez une nouvelle clé."
                    )

                # En cours ? → attendre le résultat
                if entree.statut == StatutRequete.EN_COURS:
                    self.stats["en_attente"] += 1
                    verrou = self._verrous.get(cle)

                # Déjà terminé → rejouer
                else:
                    self.stats["replays"] += 1
                    return False, entree

            else:
                # Nouvelle clé → réserver
                entree = EntreeIdempotence(
                    cle=cle,
                    statut=StatutRequete.EN_COURS,
                    hash_requete=hash_payload,
                    nb_tentatives=1,
                )
                self._store[cle] = entree
                verrou = threading.Event()
                self._verrous[cle] = verrou
                self.stats["nouvelles"] += 1
                return True, None

        # Si on arrive ici : EN_COURS, on attend hors du lock principal
        if verrou:
            verrou.wait(timeout=30)
            with self._lock:
                entree = self._store.get(cle)
                if entree and entree.statut != StatutRequete.EN_COURS:
                    self.stats["replays"] += 1
                    return False, entree
            # Timeout : autoriser le retry
            return True, None

    def marquer_complete(self, cle: str, resultat: Any):
        """Enregistre le résultat et libère les éventuels waiters."""
        with self._lock:
            if cle in self._store:
                entree = self._store[cle]
                entree.statut      = StatutRequete.COMPLETE
                entree.resultat    = resultat
                entree.complete_le = time.time()
            if cle in self._verrous:
                self._verrous[cle].set()
                del self._verrous[cle]

    def marquer_erreur(self, cle: str, erreur: str):
        """
        Les erreurs sont AUSSI mémorisées.
        Stripe rejoue la même erreur pour la même clé — comportement correct.
        """
        with self._lock:
            if cle in self._store:
                entree = self._store[cle]
                entree.statut      = StatutRequete.ERREUR
                entree.erreur      = erreur
                entree.complete_le = time.time()
            if cle in self._verrous:
                self._verrous[cle].set()
                del self._verrous[cle]

    def _nettoyer_expires(self):
        """Supprime les entrées expirées (appelé sous lock)."""
        expires = [k for k, v in self._store.items() if v.age() > self.ttl]
        for k in expires:
            del self._store[k]
            self._verrous.pop(k, None)

    def rapport(self) -> str:
        with self._lock:
            lignes = [
                f"\n  Store d'idempotence ({len(self._store)} entrées) :",
                "  " + "─"*55,
                f"  Nouvelles exécutions : {self.stats['nouvelles']}",
                f"  Replays (économisés) : {self.stats['replays']}",
                f"  Requêtes attendues   : {self.stats['en_attente']}",
                f"  Conflits de payload  : {self.stats['conflits']}",
                "",
                f"  {'Clé':<38} {'Statut':<12} {'Tentatives':<12} {'Age'}",
                "  " + "─"*55,
            ]
            for cle, e in sorted(self._store.items()):
                cle_courte = cle[:8] + "…"
                lignes.append(
                    f"  {cle_courte:<38} {e.statut.value:<12} "
                    f"{e.nb_tentatives:<12} {e.age():.2f}s"
                )
            return "\n".join(lignes)


# ─── DÉCORATEUR D'IDEMPOTENCE ─────────────────────────────────────────────────

def idempotent(store: StoreIdempotence):
    """
    Décorateur qui rend n'importe quelle fonction idempotente.

    La fonction décorée doit recevoir 'idempotency_key' en premier arg.

    Usage :
        @idempotent(store)
        def creer_paiement(idempotency_key, montant, utilisateur):
            ...
    """
    def decorateur(fn: Callable) -> Callable:
        def wrapper(idempotency_key: str, payload: dict, *args, **kwargs):
            est_nouveau, entree = store.obtenir_ou_reserver(idempotency_key, payload)

            if not est_nouveau:
                # Résultat connu → rejouer sans ré-exécuter
                if entree.statut == StatutRequete.ERREUR:
                    raise RuntimeError(f"[REPLAY ERREUR] {entree.erreur}")
                return entree.resultat

            # Nouvelle exécution
            try:
                resultat = fn(idempotency_key, payload, *args, **kwargs)
                store.marquer_complete(idempotency_key, resultat)
                return resultat
            except Exception as e:
                store.marquer_erreur(idempotency_key, str(e))
                raise

        return wrapper
    return decorateur
