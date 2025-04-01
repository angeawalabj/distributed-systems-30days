"""
Jour 26 — MVCC (Multi-Version Concurrency Control)
====================================================
Problème : lecteurs et écrivains se bloquent mutuellement avec des verrous.

  SANS MVCC (verrouillage pessimiste) :
    T1 lit   users WHERE id=1   → pose un verrou partagé
    T2 écrit users SET age=30   → bloque en attendant T1
    → Les lectures bloquent les écritures et vice-versa

  AVEC MVCC :
    Chaque ligne a plusieurs versions horodatées.
    Chaque transaction voit un SNAPSHOT immutable du moment de son début.
    T1 lit la version qui était visible à son démarrage.
    T2 écrit une NOUVELLE version (ne modifie pas l'ancienne).
    → Lecteurs et écrivains ne se bloquent JAMAIS.

Modèle PostgreSQL (xmin / xmax) :
  Chaque version de ligne a :
    xmin : transaction qui a CRÉÉ cette version
    xmax : transaction qui a SUPPRIMÉ/REMPLACÉ cette version (0 si vivante)
  Une version est VISIBLE pour une transaction T si :
    xmin est committée AVANT le snapshot de T
    xmax est 0  OU  xmax n'est PAS committée dans le snapshot de T

Isolation levels :
  READ COMMITTED  : voir les données commitées au moment de chaque requête
  REPEATABLE READ : voir les données commitées au moment du BEGIN (snapshot)
  SERIALIZABLE    : transactions exécutées comme si elles étaient séquentielles

Anomalies :
  Dirty Read     : lire les écrits non-committés d'une autre transaction
  Non-Repeatable : lire deux fois la même ligne, obtenir des résultats différents
  Phantom Read   : une requête retourne des lignes différentes si répétée
  Write Skew     : deux transactions lisent un ensemble, écrivent chacune
                   une partie → invariant global violé

Vacuum / GC :
  Les anciennes versions s'accumulent → table "bloat".
  VACUUM (PostgreSQL) scanne et supprime les versions obsolètes.
  Une version est obsolète quand aucune transaction active ne peut la voir.
"""

from __future__ import annotations
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum


class EtatTransaction(Enum):
    ACTIVE    = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED   = "ABORTED"


class NiveauIsolation(Enum):
    READ_COMMITTED  = "READ_COMMITTED"
    REPEATABLE_READ = "REPEATABLE_READ"
    SERIALIZABLE    = "SERIALIZABLE"


# ─── GESTIONNAIRE DE TRANSACTIONS ────────────────────────────────────────────

class GestionnaireTransactions:
    """
    Registre global des transactions et de leur état.
    Attribue des XID (transaction IDs) monotones.
    """

    def __init__(self):
        self._xid_courant = 0
        self._transactions: dict[int, "Transaction"] = {}
        self._lock = threading.Lock()

    def nouvelle_transaction(self, isolation: NiveauIsolation,
                              db: "MoteurMVCC") -> "Transaction":
        with self._lock:
            self._xid_courant += 1
            xid = self._xid_courant
            # Snapshot = ensemble des XIDs actifs au moment du BEGIN
            # (transactions démarrées mais pas encore commitées/abortées)
            actives = {x for x, t in self._transactions.items()
                       if t.etat == EtatTransaction.ACTIVE}
            tx = Transaction(xid=xid, isolation=isolation,
                             snapshot_actives=actives,
                             snapshot_xmax=xid,
                             db=db, gestionnaire=self)
            self._transactions[xid] = tx
            return tx

    def commiter(self, xid: int):
        with self._lock:
            if xid in self._transactions:
                self._transactions[xid].etat = EtatTransaction.COMMITTED
                self._transactions[xid].ts_fin = time.time()

    def aborter(self, xid: int):
        with self._lock:
            if xid in self._transactions:
                self._transactions[xid].etat = EtatTransaction.ABORTED
                self._transactions[xid].ts_fin = time.time()

    def est_committee(self, xid: int) -> bool:
        with self._lock:
            tx = self._transactions.get(xid)
            return tx is not None and tx.etat == EtatTransaction.COMMITTED

    def transactions_actives(self) -> set[int]:
        with self._lock:
            return {x for x, t in self._transactions.items()
                    if t.etat == EtatTransaction.ACTIVE}

    def plus_vieux_xid_actif(self) -> int:
        actives = self.transactions_actives()
        return min(actives) if actives else self._xid_courant + 1


# ─── VERSION D'UNE LIGNE ──────────────────────────────────────────────────────

@dataclass
class Version:
    """
    Une version d'une ligne dans la table.
    Analogie PostgreSQL : une ligne dans le heap avec xmin/xmax.
    """
    cle:   str
    valeur: Any
    xmin:  int           # XID de la transaction qui a créé cette version
    xmax:  int = 0       # XID qui a supprimé/remplacé (0 = toujours vivante)
    ts:    float = field(default_factory=time.time)

    def est_vivante(self) -> bool:
        return self.xmax == 0

    def __repr__(self):
        xmax_str = str(self.xmax) if self.xmax else "∞"
        return f"Version(val={self.valeur}, xmin={self.xmin}, xmax={xmax_str})"


# ─── TRANSACTION ─────────────────────────────────────────────────────────────

class Transaction:
    """
    Une transaction MVCC avec snapshot et isolation configurable.
    """

    def __init__(self, xid: int, isolation: NiveauIsolation,
                 snapshot_actives: set[int], snapshot_xmax: int,
                 db: "MoteurMVCC", gestionnaire: GestionnaireTransactions):
        self.xid              = xid
        self.isolation        = isolation
        self.snapshot_actives = snapshot_actives   # XIDs actifs au BEGIN
        self.snapshot_xmax    = snapshot_xmax      # Le plus grand XID connu au BEGIN
        self.etat             = EtatTransaction.ACTIVE
        self.ts_debut         = time.time()
        self.ts_fin:          Optional[float] = None
        self._db              = db
        self._gest            = gestionnaire
        self._ecritures: dict[str, Any] = {}   # Buffer d'écriture local
        self._suppressions: set[str] = set()
        self.journal: list[str] = []

    # ── Visibilité ────────────────────────────────────────────────────────────

    def peut_voir(self, version: Version) -> bool:
        """
        Règle de visibilité MVCC.
        Une version est visible si :
          1. xmin est committée ET antérieure au snapshot
          2. xmax est 0 (version toujours vivante) OU
             xmax n'est PAS committée dans notre snapshot
        """
        # Règle xmin : créée par une TX committée avant notre snapshot
        if version.xmin == self.xid:
            xmin_visible = True  # Notre propre écriture
        elif version.xmin > self.snapshot_xmax:
            xmin_visible = False  # Trop récente
        elif version.xmin in self.snapshot_actives:
            xmin_visible = False  # Active au moment du snapshot → pas visible
        else:
            xmin_visible = self._gest.est_committee(version.xmin)

        if not xmin_visible:
            return False

        # Règle xmax : pas encore supprimée, ou supprimée par une TX future
        if version.xmax == 0:
            return True
        if version.xmax == self.xid:
            return False  # Nous l'avons supprimée dans cette TX
        if version.xmax > self.snapshot_xmax:
            return True   # Supprimée après notre snapshot → encore visible
        if version.xmax in self.snapshot_actives:
            return True   # Supprimée par une TX active → encore visible pour nous
        return not self._gest.est_committee(version.xmax)

    # ── API ───────────────────────────────────────────────────────────────────

    def lire(self, cle: str) -> Optional[Any]:
        if self.etat != EtatTransaction.ACTIVE:
            raise RuntimeError(f"TX {self.xid} n'est pas active")

        # En READ COMMITTED : snapshot rafraîchi à chaque lecture
        if self.isolation == NiveauIsolation.READ_COMMITTED:
            actives = self._gest.transactions_actives() - {self.xid}
            snapshot_actives = actives
            snapshot_xmax = self._gest._xid_courant
        else:
            snapshot_actives = self.snapshot_actives
            snapshot_xmax = self.snapshot_xmax

        # Vérifier le buffer local d'abord
        if cle in self._suppressions:
            return None
        if cle in self._ecritures:
            return self._ecritures[cle]

        # Chercher dans le moteur
        return self._db._lire_avec_snapshot(cle, self, snapshot_actives, snapshot_xmax)

    def ecrire(self, cle: str, valeur: Any):
        if self.etat != EtatTransaction.ACTIVE:
            raise RuntimeError(f"TX {self.xid} n'est pas active")
        self._suppressions.discard(cle)
        self._ecritures[cle] = valeur
        self.journal.append(f"WRITE {cle}={valeur}")

    def supprimer(self, cle: str):
        if self.etat != EtatTransaction.ACTIVE:
            raise RuntimeError(f"TX {self.xid} n'est pas active")
        self._ecritures.pop(cle, None)
        self._suppressions.add(cle)
        self.journal.append(f"DELETE {cle}")

    def commit(self):
        if self.etat != EtatTransaction.ACTIVE:
            raise RuntimeError(f"TX {self.xid} déjà terminée")
        self._db._appliquer_ecritures(self)
        self._gest.commiter(self.xid)
        self.etat = EtatTransaction.COMMITTED
        self.journal.append("COMMIT")

    def abort(self):
        if self.etat != EtatTransaction.ACTIVE:
            return
        self._gest.aborter(self.xid)
        self.etat = EtatTransaction.ABORTED
        self.journal.append("ABORT")


# ─── MOTEUR MVCC ─────────────────────────────────────────────────────────────

class MoteurMVCC:
    """
    Moteur de stockage MVCC.
    Chaque clef peut avoir plusieurs versions simultanées.
    """

    def __init__(self):
        self._versions: dict[str, list[Version]] = {}
        self._lock = threading.Lock()
        self._gest = GestionnaireTransactions()

    def begin(self, isolation: NiveauIsolation = NiveauIsolation.REPEATABLE_READ
              ) -> Transaction:
        return self._gest.nouvelle_transaction(isolation, self)

    def _lire_avec_snapshot(self, cle: str, tx: Transaction,
                             snapshot_actives: set[int],
                             snapshot_xmax: int) -> Optional[Any]:
        with self._lock:
            versions = self._versions.get(cle, [])
        # Chercher la version la plus récente visible
        # (itérer en sens inverse = de la plus récente à la plus ancienne)
        for version in reversed(versions):
            # Recréer un snapshot local pour la vérif
            # (on passe les paramètres snapshot directement)
            visible = self._version_visible(version, tx, snapshot_actives, snapshot_xmax)
            if visible:
                return version.valeur
        return None

    def _version_visible(self, version: Version, tx: Transaction,
                          snapshot_actives: set[int],
                          snapshot_xmax: int) -> bool:
        """Même logique que Transaction.peut_voir mais paramétrée."""
        if version.xmin == tx.xid:
            xmin_ok = True
        elif version.xmin > snapshot_xmax:
            xmin_ok = False
        elif version.xmin in snapshot_actives:
            xmin_ok = False
        else:
            xmin_ok = self._gest.est_committee(version.xmin)

        if not xmin_ok:
            return False

        if version.xmax == 0:
            return True
        if version.xmax == tx.xid:
            return False
        if version.xmax > snapshot_xmax:
            return True
        if version.xmax in snapshot_actives:
            return True
        return not self._gest.est_committee(version.xmax)

    def _appliquer_ecritures(self, tx: Transaction):
        """Applique les écriture buffées au commit."""
        with self._lock:
            # Écriture : créer une nouvelle version, fermer l'ancienne
            for cle, valeur in tx._ecritures.items():
                versions = self._versions.setdefault(cle, [])
                # Fermer la version courante (si existe)
                for v in reversed(versions):
                    if v.xmax == 0:
                        v.xmax = tx.xid
                        break
                versions.append(Version(cle=cle, valeur=valeur, xmin=tx.xid))

            # Suppression : fermer la version courante
            for cle in tx._suppressions:
                versions = self._versions.get(cle, [])
                for v in reversed(versions):
                    if v.xmax == 0:
                        v.xmax = tx.xid
                        break

    def toutes_versions(self, cle: str) -> list[Version]:
        with self._lock:
            return list(self._versions.get(cle, []))

    def nb_versions_totales(self) -> int:
        with self._lock:
            return sum(len(vs) for vs in self._versions.values())

    def vacuum(self) -> int:
        """
        Nettoyer les versions obsolètes.
        Une version est obsolète quand :
          - xmax est committée
          - xmax < plus_vieux_xid_actif  (aucune TX active ne peut la voir)
        Retourne le nombre de versions supprimées.
        """
        horizon = self._gest.plus_vieux_xid_actif()
        supprimes = 0
        with self._lock:
            for cle in list(self._versions.keys()):
                avant = len(self._versions[cle])
                self._versions[cle] = [
                    v for v in self._versions[cle]
                    if not (v.xmax != 0
                            and self._gest.est_committee(v.xmax)
                            and v.xmax < horizon)
                ]
                supprimes += avant - len(self._versions[cle])
        return supprimes

    def stats(self) -> dict:
        with self._lock:
            total = sum(len(vs) for vs in self._versions.values())
            vivantes = sum(1 for vs in self._versions.values()
                          for v in vs if v.xmax == 0)
            return {
                "clefs": len(self._versions),
                "versions_totales": total,
                "versions_vivantes": vivantes,
                "versions_mortes": total - vivantes,
            }
