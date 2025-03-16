"""
Jour 16 — Load Balancing : Couche 4 vs Couche 7
=================================================
Problème : un seul serveur ne peut pas absorber 1M requêtes/seconde.
Solution : répartir le trafic sur N backends via un load balancer.

Couche 4 (Transport — TCP/UDP) :
  Le LB voit : IP source, IP destination, port source, port destination.
  Il ne peut PAS lire le contenu du paquet (pas de TLS termination,
  pas de headers HTTP, pas d'URL).
  → Décision basée uniquement sur les métadonnées réseau.
  → Ultra-rapide (pas de parsing), très faible latence.
  Exemple : HAProxy en mode TCP, AWS NLB, LVS/IPVS.

Couche 7 (Application — HTTP/HTTPS/gRPC) :
  Le LB voit TOUT : URL, headers, cookies, body, méthode HTTP.
  Il peut faire de la TLS termination, du routage par chemin,
  de l'authentification, du rate limiting, du circuit breaking.
  → Décision riche basée sur le contenu applicatif.
  → Plus lent que L4 (parsing HTTP), mais beaucoup plus puissant.
  Exemple : Nginx, HAProxy en mode HTTP, Envoy, AWS ALB.

Algorithmes de répartition :
  Round Robin      : tourniquet — simple, ignore la charge réelle
  Weighted RR      : pondéré par capacité des serveurs
  Least Connections: envoie au serveur avec le moins de connexions actives
  IP Hash          : hash(IP client) → même client = même serveur (sticky)
  Random           : aléatoire (surprenamment efficace à grande échelle)
  Power of Two     : choisit le moins chargé parmi 2 aléatoires (optimal)

Health checks :
  L4 : TCP connect() réussit ou non
  L7 : GET /health retourne 200 ou non
  → Un backend qui ne répond pas est retiré du pool automatiquement
"""

import time
import threading
import random
import hashlib
from dataclasses import dataclass, field
from typing import Optional, Callable
from enum import Enum
from collections import deque, defaultdict


# ─── BACKEND SERVER ──────────────────────────────────────────────────────────

@dataclass
class RequeteHTTP:
    methode:    str
    chemin:     str
    headers:    dict = field(default_factory=dict)
    body:       str = ""
    client_ip:  str = "127.0.0.1"
    request_id: str = ""


@dataclass
class ReponseHTTP:
    status:      int
    body:        str
    headers:     dict = field(default_factory=dict)
    backend_id:  str = ""
    latence_ms:  float = 0.0


class BackendServer:
    """
    Simule un serveur backend avec latence, capacité et health check.
    Peut simuler des surcharges, pannes et slowdowns.
    """

    def __init__(
        self,
        server_id:   str,
        latence_ms:  float = 20.0,
        capacite:    int   = 100,    # Max connexions simultanées
        poids:       float = 1.0,    # Pour Weighted RR
        version:     str   = "v1",   # Pour blue/green et canary
        region:      str   = "eu",
    ):
        self.id         = server_id
        self.latence    = latence_ms / 1000
        self.capacite   = capacite
        self.poids      = poids
        self.version    = version
        self.region     = region
        self._actif     = True
        self._sain      = True

        self._connexions_actives = 0
        self._lock               = threading.RLock()

        self.stats = {
            "requetes_traitees": 0,
            "erreurs":           0,
            "connexions_max":    0,
            "latence_totale_ms": 0.0,
        }

        # Simulation de pathologies
        self._surcharge    = False
        self._latence_extra = 0.0

    # ── Traitement d'une requête ──────────────────────────────────────────────

    def traiter(self, req: RequeteHTTP) -> ReponseHTTP:
        """Traite une requête HTTP (simulée)."""
        t0 = time.perf_counter()

        with self._lock:
            if not self._actif or not self._sain:
                return ReponseHTTP(503, "Service Unavailable",
                                   backend_id=self.id)
            if self._connexions_actives >= self.capacite:
                self.stats["erreurs"] += 1
                return ReponseHTTP(503, "Too Many Connections",
                                   backend_id=self.id)
            self._connexions_actives += 1
            self.stats["connexions_max"] = max(
                self.stats["connexions_max"], self._connexions_actives
            )

        try:
            latence = self.latence + self._latence_extra
            if self._surcharge:
                latence *= 5
            time.sleep(latence * random.uniform(0.7, 1.3))

            status = 200
            body   = f"OK from {self.id} ({self.version}) — {req.chemin}"

            with self._lock:
                self.stats["requetes_traitees"] += 1
                elapsed = (time.perf_counter() - t0) * 1000
                self.stats["latence_totale_ms"] += elapsed

            return ReponseHTTP(status, body, backend_id=self.id,
                               latence_ms=(time.perf_counter() - t0) * 1000)
        finally:
            with self._lock:
                self._connexions_actives -= 1

    # ── Health check ─────────────────────────────────────────────────────────

    def health_check_l4(self) -> bool:
        """L4 : juste vérifier que le serveur est up (TCP connect)."""
        return self._actif

    def health_check_l7(self) -> tuple[bool, int]:
        """L7 : vérifier le endpoint /health (retourne 200 si sain)."""
        if not self._actif:
            return False, 0
        status = 200 if self._sain else 503
        return self._sain, status

    # ── Contrôle ─────────────────────────────────────────────────────────────

    def tomber(self):
        self._actif = False

    def degrader(self):
        """Le serveur répond mais lentement (surcharge)."""
        self._surcharge = True

    def guerir(self):
        self._actif     = True
        self._sain      = True
        self._surcharge = False
        self._latence_extra = 0.0

    def connexions_actives(self) -> int:
        with self._lock:
            return self._connexions_actives

    def latence_moyenne_ms(self) -> float:
        with self._lock:
            if self.stats["requetes_traitees"] == 0:
                return 0.0
            return self.stats["latence_totale_ms"] / self.stats["requetes_traitees"]


# ─── ALGORITHMES DE RÉPARTITION ───────────────────────────────────────────────

class AlgoRepartition(Enum):
    ROUND_ROBIN       = "Round Robin"
    WEIGHTED_RR       = "Weighted Round Robin"
    LEAST_CONNECTIONS = "Least Connections"
    IP_HASH           = "IP Hash (Sticky)"
    RANDOM            = "Random"
    POWER_OF_TWO      = "Power of Two Choices"


# ─── LOAD BALANCER L4 ────────────────────────────────────────────────────────

class LoadBalancerL4:
    """
    Load Balancer couche 4 (TCP).
    Voit uniquement : IP source, IP destination, port.
    NE PEUT PAS inspecter le contenu HTTP.

    Algorithmes disponibles : Round Robin, Weighted RR, IP Hash, Random.
    Least Connections possible mais moins précis sans contexte applicatif.
    """

    def __init__(self, algo: AlgoRepartition = AlgoRepartition.ROUND_ROBIN):
        self.algo     = algo
        self.backends: list[BackendServer] = []
        self._rr_idx  = 0
        self._lock    = threading.RLock()
        self.stats    = defaultdict(int)

        # Health check L4 (TCP)
        self._actif = True
        threading.Thread(target=self._boucle_health_check, daemon=True).start()

    def ajouter_backend(self, backend: BackendServer):
        with self._lock:
            self.backends.append(backend)

    def _backends_sains(self) -> list[BackendServer]:
        with self._lock:
            return [b for b in self.backends if b.health_check_l4()]

    def selectionner(self, client_ip: str = "127.0.0.1") -> Optional[BackendServer]:
        """Sélectionne un backend selon l'algorithme configuré."""
        sains = self._backends_sains()
        if not sains:
            return None

        if self.algo == AlgoRepartition.ROUND_ROBIN:
            return self._round_robin(sains)
        elif self.algo == AlgoRepartition.WEIGHTED_RR:
            return self._weighted_rr(sains)
        elif self.algo == AlgoRepartition.IP_HASH:
            return self._ip_hash(sains, client_ip)
        elif self.algo == AlgoRepartition.RANDOM:
            return random.choice(sains)
        elif self.algo == AlgoRepartition.POWER_OF_TWO:
            return self._power_of_two(sains)
        elif self.algo == AlgoRepartition.LEAST_CONNECTIONS:
            return min(sains, key=lambda b: b.connexions_actives())
        return sains[0]

    def _round_robin(self, sains: list) -> BackendServer:
        with self._lock:
            backend = sains[self._rr_idx % len(sains)]
            self._rr_idx += 1
            return backend

    def _weighted_rr(self, sains: list) -> BackendServer:
        """Pondération : chaque backend a poids * 10 tickets."""
        tickets = []
        for b in sains:
            tickets.extend([b] * max(1, int(b.poids * 10)))
        return random.choice(tickets)

    def _ip_hash(self, sains: list, client_ip: str) -> BackendServer:
        """Hash de l'IP client → même client → même backend."""
        h = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        return sains[h % len(sains)]

    def _power_of_two(self, sains: list) -> BackendServer:
        """
        Power of Two Choices : choisir 2 backends aléatoires,
        retourner celui avec le moins de connexions actives.
        Équilibre quasi-optimal avec O(1) comparaisons.
        """
        if len(sains) < 2:
            return sains[0]
        a, b = random.sample(sains, 2)
        return a if a.connexions_actives() <= b.connexions_actives() else b

    def acheminer(self, client_ip: str = "127.0.0.1",
                  req: Optional[RequeteHTTP] = None) -> Optional[ReponseHTTP]:
        backend = self.selectionner(client_ip)
        if not backend:
            self.stats["no_backend"] += 1
            return ReponseHTTP(503, "No backend available")
        self.stats[f"backend_{backend.id}"] += 1
        if req:
            return backend.traiter(req)
        return ReponseHTTP(200, f"Routed to {backend.id}", backend_id=backend.id)

    def _boucle_health_check(self):
        while self._actif:
            time.sleep(0.5)
            with self._lock:
                for b in self.backends:
                    b.health_check_l4()


# ─── LOAD BALANCER L7 ────────────────────────────────────────────────────────

class RegleRoutage:
    """Une règle de routage L7."""
    def __init__(self, condition: Callable[[RequeteHTTP], bool],
                 pool: list[BackendServer], nom: str = ""):
        self.condition = condition
        self.pool      = pool
        self.nom       = nom
        self.hits      = 0


class LoadBalancerL7:
    """
    Load Balancer couche 7 (HTTP).
    Inspecte les headers, URL, méthode, cookies.
    Peut router différemment selon le contenu de la requête.

    Fonctionnalités :
      - Routage par chemin (path-based routing)
      - Routage par header (canary deployments)
      - Sticky sessions par cookie
      - Health checks applicatifs (GET /health → 200)
      - Rate limiting par IP
    """

    def __init__(self, algo: AlgoRepartition = AlgoRepartition.LEAST_CONNECTIONS):
        self.algo     = algo
        self._regles: list[RegleRoutage] = []
        self._default_pool: list[BackendServer] = []
        self._lock    = threading.RLock()
        self.stats    = defaultdict(int)

        # Rate limiting : IP → compteur de requêtes sur fenêtre glissante
        self._rate_limits: dict[str, deque] = defaultdict(deque)
        self._rate_limit_rps = None   # None = désactivé

        # Health checks L7
        self._actif = True
        threading.Thread(target=self._boucle_health_check, daemon=True).start()

    # ── Configuration ─────────────────────────────────────────────────────────

    def ajouter_backend(self, backend: BackendServer):
        with self._lock:
            self._default_pool.append(backend)

    def ajouter_regle(self, condition: Callable, pool: list[BackendServer], nom: str = ""):
        """Ajoute une règle de routage. Les règles sont évaluées dans l'ordre."""
        with self._lock:
            self._regles.append(RegleRoutage(condition, pool, nom))

    def configurer_rate_limit(self, requetes_par_seconde: int):
        self._rate_limit_rps = requetes_par_seconde

    # ── Routage ───────────────────────────────────────────────────────────────

    def acheminer(self, req: RequeteHTTP) -> ReponseHTTP:
        """
        Pipeline de traitement L7 :
          1. Rate limiting
          2. Matching des règles de routage
          3. Sélection du backend dans le pool retenu
          4. Forwarding
        """
        # 1. Rate limiting
        if self._rate_limit_rps and self._est_rate_limite(req.client_ip):
            self.stats["rate_limited"] += 1
            return ReponseHTTP(429, "Too Many Requests",
                               headers={"Retry-After": "1"})

        # 2. Trouver le pool selon les règles
        pool = self._trouver_pool(req)
        if not pool:
            self.stats["no_backend"] += 1
            return ReponseHTTP(503, "No backend available")

        # 3. Sélectionner le backend dans le pool
        backend = self._selectionner_dans_pool(pool, req)
        if not backend:
            self.stats["no_healthy_backend"] += 1
            return ReponseHTTP(503, "No healthy backend")

        # 4. Forwarder
        self.stats[f"backend_{backend.id}"] += 1
        self.stats[f"regle_{self._derniere_regle}"] += 1
        return backend.traiter(req)

    def _trouver_pool(self, req: RequeteHTTP) -> list[BackendServer]:
        """Évalue les règles dans l'ordre, retourne le premier pool qui matche."""
        with self._lock:
            for regle in self._regles:
                if regle.condition(req):
                    regle.hits += 1
                    self._derniere_regle = regle.nom
                    sains = [b for b in regle.pool if b.health_check_l7()[0]]
                    if sains:
                        return sains
            self._derniere_regle = "default"
            return [b for b in self._default_pool if b.health_check_l7()[0]]

    def _selectionner_dans_pool(
        self, pool: list[BackendServer], req: RequeteHTTP
    ) -> Optional[BackendServer]:
        if not pool:
            return None
        if self.algo == AlgoRepartition.LEAST_CONNECTIONS:
            return min(pool, key=lambda b: b.connexions_actives())
        elif self.algo == AlgoRepartition.ROUND_ROBIN:
            with self._lock:
                idx = getattr(self, '_rr_idx', 0) % len(pool)
                self._rr_idx = idx + 1
            return pool[idx]
        elif self.algo == AlgoRepartition.IP_HASH:
            h = int(hashlib.md5(req.client_ip.encode()).hexdigest(), 16)
            return pool[h % len(pool)]
        return random.choice(pool)

    def _est_rate_limite(self, client_ip: str) -> bool:
        """Fenêtre glissante d'1 seconde."""
        now = time.time()
        fenetre = self._rate_limits[client_ip]
        while fenetre and fenetre[0] < now - 1.0:
            fenetre.popleft()
        if len(fenetre) >= self._rate_limit_rps:
            return True
        fenetre.append(now)
        return False

    def _boucle_health_check(self):
        while self._actif:
            time.sleep(0.3)

    _derniere_regle = "default"
