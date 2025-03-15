"""
Jour 15 — Distributed Locking : Redlock
==========================================
Problème : empêcher deux services d'accéder à la même ressource
en même temps dans un système distribué.

Verrou local (threading.Lock) : ne fonctionne que sur 1 processus.
Verrou sur 1 Redis             : Redis est un SPOF. Si Redis tombe
                                  ou si le réseau se partitionne,
                                  le verrou est soit perdu, soit bloqué.

Redlock (Antirez, créateur de Redis, 2016) :
  Algorithme sur N instances Redis indépendantes (N impair, N≥5).

  Acquisition :
    1. Lire l'heure courante t1 (en ms)
    2. Tenter SET key token NX PX ttl sur chacune des N instances
    3. Lire l'heure courante t2
    4. Temps écoulé = t2 - t1
    5. Le verrou est acquis si :
       a. Quorum atteint : au moins N/2+1 instances ont répondu OK
       b. Validity time restant > 0 : ttl - temps_écoulé - drift > 0
    6. Si non : libérer le verrou sur TOUTES les instances (même celles qui ont échoué)

  Libération :
    Lua script atomique : IF get(key) == token THEN del(key) END
    Le token unique empêche de libérer le verrou d'un autre client.

  Fencing Token :
    Pour résister aux GC pauses (scénario de Martin Kleppmann) :
    chaque acquisition retourne un token monotoniquement croissant.
    Le service protégé rejette les requêtes avec un token trop ancien.

  Débat Kleppmann vs Antirez (2016) :
    Kleppmann : "Redlock est unsafe. Une GC pause peut faire expirer
                 le verrou pendant que le client croit l'avoir encore."
    Antirez   : "Le fencing token résout ça. Redlock protège contre
                 les pannes Redis, pas contre les GC pauses."
    Consensus : Redlock est solide pour la majorité des cas,
                mais nécessite des fencing tokens pour la rigueur absolue.
"""

import time
import uuid
import threading
import random
from dataclasses import dataclass, field
from typing import Optional


# ─── INSTANCE REDIS SIMULÉE ───────────────────────────────────────────────────

class InstanceRedis:
    """
    Simule une instance Redis avec SET NX PX et un script Lua de release.
    Inclut simulation de latence, crashes et partitions réseau.
    """

    def __init__(self, instance_id: str, latence_ms: float = 5.0):
        self.id       = instance_id
        self.latence  = latence_ms / 1000
        self._store: dict[str, tuple[str, float]] = {}  # clé → (token, expire_ts)
        self._lock    = threading.RLock()
        self._actif   = True
        self._latence_extra = 0.0   # Pour simuler GC pause ou réseau lent

    # ── Opérations Redis ─────────────────────────────────────────────────────

    def set_nx_px(self, cle: str, token: str, ttl_ms: int) -> bool:
        """
        SET key token NX PX ttl
        NX = seulement si la clé n'existe pas
        PX = TTL en millisecondes
        Retourne True si SET réussi, False sinon.
        """
        if not self._actif:
            time.sleep(self.latence)   # Timeout simulé
            return False

        latence_totale = self.latence + self._latence_extra
        time.sleep(latence_totale * random.uniform(0.7, 1.3))

        with self._lock:
            self._expirer()
            if cle in self._store:
                return False   # NX : clé déjà présente
            expire_ts = time.time() + ttl_ms / 1000
            self._store[cle] = (token, expire_ts)
            return True

    def get(self, cle: str) -> Optional[str]:
        """GET key → token ou None."""
        if not self._actif:
            return None
        time.sleep(self.latence * random.uniform(0.5, 1.0))
        with self._lock:
            self._expirer()
            entree = self._store.get(cle)
            return entree[0] if entree else None

    def release(self, cle: str, token: str) -> bool:
        """
        Script Lua atomique :
          if redis.call('get', key) == token then
            return redis.call('del', key)
          end
        Empêche de libérer le verrou d'un autre client.
        """
        if not self._actif:
            return False
        time.sleep(self.latence * random.uniform(0.5, 1.0))
        with self._lock:
            self._expirer()
            entree = self._store.get(cle)
            if entree and entree[0] == token:
                del self._store[cle]
                return True
            return False

    def ttl_restant_ms(self, cle: str) -> int:
        """Retourne le TTL restant en ms, ou -1 si clé absente."""
        with self._lock:
            entree = self._store.get(cle)
            if not entree:
                return -1
            remaining = (entree[1] - time.time()) * 1000
            return max(0, int(remaining))

    def _expirer(self):
        """Supprime les clés expirées (lazy expiration)."""
        now = time.time()
        expirées = [k for k, (_, exp) in self._store.items() if exp <= now]
        for k in expirées:
            del self._store[k]

    # ── Simulation de pannes ─────────────────────────────────────────────────

    def tomber(self):
        self._actif = False

    def redemarrer(self):
        with self._lock:
            self._store.clear()   # Redis perd son état après restart
        self._actif = True

    def simuler_latence_reseau(self, ms: float):
        self._latence_extra = ms / 1000

    def simuler_gc_pause(self, duree_ms: float):
        """Simule une GC pause : l'instance ne répond pas pendant X ms."""
        threading.Thread(
            target=lambda: [setattr(self, '_actif', False),
                            time.sleep(duree_ms / 1000),
                            setattr(self, '_actif', True)],
            daemon=True
        ).start()


# ─── RÉSULTAT D'ACQUISITION ───────────────────────────────────────────────────

@dataclass
class ResultatVerrou:
    acquis:        bool
    token:         str
    fencing_token: int          # Monotoniquement croissant pour l'anti-GC-pause
    validity_ms:   float        # TTL restant après acquisition
    instances_ok:  list[str]    # Instances qui ont accordé le verrou
    instances_ko:  list[str]    # Instances qui ont refusé ou ne répondaient pas
    duree_ms:      float        # Temps d'acquisition
    ressource:     str


# ─── CLIENT REDLOCK ───────────────────────────────────────────────────────────

class ClientRedlock:
    """
    Implémente l'algorithme Redlock d'Antirez.

    Usage :
        with client.acquerir("ma_ressource") as verrou:
            if verrou.acquis:
                # Accès exclusif garanti
                faire_quelque_chose()
    """

    # Compteur global pour les fencing tokens (partagé entre toutes les instances)
    _fencing_counter = 0
    _fencing_lock    = threading.Lock()

    def __init__(
        self,
        client_id: str,
        instances: list[InstanceRedis],
        ttl_ms: int = 10_000,
        retry_count: int = 3,
        retry_delay_ms: float = 200,
        clock_drift_factor: float = 0.01,
    ):
        self.id                 = client_id
        self.instances          = instances
        self.N                  = len(instances)
        self.quorum             = self.N // 2 + 1
        self.ttl_ms             = ttl_ms
        self.retry_count        = retry_count
        self.retry_delay_ms     = retry_delay_ms
        self.clock_drift_factor = clock_drift_factor

        self.stats = {
            "tentatives":  0,
            "succes":      0,
            "echecs":      0,
            "expirations": 0,
        }

    def _prochain_fencing_token(self) -> int:
        with ClientRedlock._fencing_lock:
            ClientRedlock._fencing_counter += 1
            return ClientRedlock._fencing_counter

    def acquerir(self, ressource: str) -> ResultatVerrou:
        """
        Tente d'acquérir le verrou avec retry.
        Algorithme Redlock en 6 étapes.
        """
        for tentative in range(self.retry_count):
            self.stats["tentatives"] += 1
            resultat = self._tentative_acquisition(ressource)

            if resultat.acquis:
                self.stats["succes"] += 1
                return resultat

            # Attente exponentielle + jitter avant retry
            if tentative < self.retry_count - 1:
                delai = self.retry_delay_ms * (2 ** tentative)
                jitter = random.uniform(0, delai * 0.1)
                time.sleep((delai + jitter) / 1000)

        self.stats["echecs"] += 1
        return ResultatVerrou(
            acquis=False, token="", fencing_token=0, validity_ms=0,
            instances_ok=[], instances_ko=[i.id for i in self.instances],
            duree_ms=0, ressource=ressource,
        )

    def _tentative_acquisition(self, ressource: str) -> ResultatVerrou:
        """Une seule tentative d'acquisition Redlock."""
        token    = str(uuid.uuid4())
        t_debut  = time.perf_counter()

        # Étapes 1-2 : SET NX PX en parallèle sur toutes les instances
        instances_ok  = []
        instances_ko  = []
        resultats_lock = threading.Lock()

        def tenter_instance(inst: InstanceRedis):
            ok = inst.set_nx_px(ressource, token, self.ttl_ms)
            with resultats_lock:
                (instances_ok if ok else instances_ko).append(inst.id)

        threads = [threading.Thread(target=tenter_instance, args=(inst,), daemon=True)
                   for inst in self.instances]
        for t in threads: t.start()
        for t in threads: t.join(timeout=self.ttl_ms / 1000)

        # Étape 3 : mesurer le temps écoulé
        t_fin    = time.perf_counter()
        elapsed  = (t_fin - t_debut) * 1000

        # Étape 4 : calculer le validity time
        drift       = self.ttl_ms * self.clock_drift_factor + 2   # marge de sécurité
        validity_ms = self.ttl_ms - elapsed - drift

        # Étape 5 : vérifier quorum ET validity
        acquis = len(instances_ok) >= self.quorum and validity_ms > 0

        if not acquis:
            # Étape 6 : libérer partout en cas d'échec
            self._liberer_partout(ressource, token)
            return ResultatVerrou(
                acquis=False, token=token, fencing_token=0,
                validity_ms=max(0, validity_ms),
                instances_ok=instances_ok, instances_ko=instances_ko,
                duree_ms=elapsed, ressource=ressource,
            )

        return ResultatVerrou(
            acquis=True, token=token,
            fencing_token=self._prochain_fencing_token(),
            validity_ms=validity_ms,
            instances_ok=instances_ok, instances_ko=instances_ko,
            duree_ms=elapsed, ressource=ressource,
        )

    def liberer(self, ressource: str, token: str):
        """Libère le verrou sur toutes les instances."""
        self._liberer_partout(ressource, token)
        # Pas de stats ici — la libération peut partiellement échouer,
        # c'est normal (instances down) → le TTL expirera naturellement.

    def _liberer_partout(self, ressource: str, token: str):
        threads = [
            threading.Thread(target=lambda i=inst: i.release(ressource, token), daemon=True)
            for inst in self.instances
        ]
        for t in threads: t.start()
        for t in threads: t.join(timeout=1.0)


# ─── RESSOURCE PROTÉGÉE AVEC FENCING TOKEN ───────────────────────────────────

class RessourceProtegee:
    """
    Simule une ressource critique (ex: fichier, compte bancaire)
    qui accepte uniquement les écritures avec un fencing token croissant.
    Empêche les clients avec des verrous expirés d'écrire.
    """

    def __init__(self, nom: str):
        self.nom              = nom
        self._valeur: Any     = None
        self._dernier_token   = 0
        self._nb_ecritures    = 0
        self._nb_rejets       = 0
        self._lock            = threading.Lock()
        self.historique: list[dict] = []

    def ecrire(self, valeur, fencing_token: int, client_id: str) -> bool:
        with self._lock:
            if fencing_token <= self._dernier_token:
                self._nb_rejets += 1
                self.historique.append({
                    "action": "REJETÉ", "client": client_id,
                    "token": fencing_token, "token_actuel": self._dernier_token,
                    "valeur": valeur,
                })
                return False
            self._valeur           = valeur
            self._dernier_token    = fencing_token
            self._nb_ecritures    += 1
            self.historique.append({
                "action": "ÉCRIT", "client": client_id,
                "token": fencing_token, "valeur": valeur,
            })
            return True

    def lire(self):
        with self._lock:
            return self._valeur

    def resume(self) -> str:
        return (f"RessourceProtegee({self.nom!r}) : "
                f"val={self._valeur!r}, "
                f"ecritures={self._nb_ecritures}, "
                f"rejets={self._nb_rejets}")
