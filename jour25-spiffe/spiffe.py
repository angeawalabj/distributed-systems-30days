"""
Jour 25 — "Le Sceau d'Identité" — SPIFFE / SPIRE
==================================================
SPIFFE : Secure Production Identity Framework For Everyone
SPIRE  : SPIFFE Runtime Environment (implémentation de référence)

Problème fondamental : comment un workload prouve-t-il son identité ?
  → Un humain : login + mot de passe + MFA
  → Un service : ??? (secret dans le code = danger, IP = insuffisant)

SPIFFE résout ça avec l'attestation de workload :
  "Je vais te prouver qui tu es en observant ton environnement d'exécution,
   sans que tu aies besoin de connaître un secret au démarrage."

Architecture SPIRE :
  ┌─────────────────────────────────────────────────────┐
  │  SPIRE SERVER (1 par cluster)                       │
  │    - Stocke les entries (qui peut avoir quelle id?) │
  │    - Signe les SVIDs (certificats X.509)            │
  │    - Gère la CA racine                              │
  └──────────────────┬──────────────────────────────────┘
                     │ TLS
  ┌──────────────────▼──────────────────────────────────┐
  │  SPIRE AGENT (1 par nœud)                           │
  │    - Atteste l'identité des workloads sur ce nœud   │
  │    - Cache les SVIDs                                │
  │    - Expose l'API Workload (socket Unix)            │
  └──────────────────┬──────────────────────────────────┘
                     │ Unix socket
  ┌──────────────────▼──────────────────────────────────┐
  │  WORKLOAD (ton service)                             │
  │    - Appelle l'API Workload                         │
  │    - Reçoit son SVID automatiquement                │
  │    - Zéro secret au démarrage                       │
  └─────────────────────────────────────────────────────┘

Attestation : comment le SPIRE Agent sait-il que c'est le bon workload ?
  Kubernetes : lire le ServiceAccount Token du pod (API k8s)
  Linux      : lire le PID, UID, path de l'exécutable (kernel)
  AWS EC2    : Instance Identity Document signé par AWS
  Docker     : inspecter le label du conteneur

SVID (SPIFFE Verifiable Identity Document) :
  Certificat X.509 avec SAN (Subject Alternative Name) = spiffe://domain/path
  Durée : 1h à 24h (configurable, souvent 1h en prod)
  Rotation : l'agent renouvelle proactivement à 2/3 de la durée restante

Fédération :
  Cluster A trust domain : spiffe://cluster-a.acme.com
  Cluster B trust domain : spiffe://cluster-b.acme.com
  → Échange de trust bundles → services A peuvent s'authentifier auprès de B
  → Multi-cloud, multi-cluster, partenaires externes
"""

from __future__ import annotations
import time
import hashlib
import hmac
import threading
from dataclasses import dataclass, field
from typing import Optional, Callable
from enum import Enum
from collections import defaultdict


# ─── SVID ────────────────────────────────────────────────────────────────────

@dataclass
class SVID:
    """
    SPIFFE Verifiable Identity Document.
    Certificat X.509 avec URI SPIFFE dans le champ SAN.
    """
    spiffe_id:    str       # spiffe://trust-domain/workload-path
    trust_domain: str
    workload:     str
    emis_a:       float
    expire_a:     float
    cle_privee:   str       # Simulé : hash de la clef
    bundle_ca:    str       # Hash du bundle CA ayant signé ce SVID
    revoque:      bool = False

    @property
    def est_valide(self) -> bool:
        return (not self.revoque and
                self.emis_a <= time.time() <= self.expire_a)

    @property
    def duree_restante(self) -> float:
        return max(0.0, self.expire_a - time.time())

    @property
    def pct_vie_restante(self) -> float:
        total = self.expire_a - self.emis_a
        return self.duree_restante / max(total, 1) * 100

    def __repr__(self):
        statut = "✅" if self.est_valide else "❌"
        return (f"SVID({self.spiffe_id}, "
                f"restant={self.duree_restante:.0f}s {statut})")


# ─── ENTRY (REGISTRATION) ────────────────────────────────────────────────────

@dataclass
class Entry:
    """
    Entrée dans le registre SPIRE Server.
    Mappe : sélecteur (qui?) → SPIFFE ID (quelle identité?)
    """
    entry_id:    str
    spiffe_id:   str
    selectors:   list[str]   # ex: ["k8s:ns:prod", "k8s:sa:payments"]
    ttl_s:       float = 3600
    parent_id:   Optional[str] = None   # Pour la délégation d'attestation


# ─── TRUST BUNDLE ────────────────────────────────────────────────────────────

@dataclass
class TrustBundle:
    """
    Bundle de confiance d'un trust domain.
    Contient le(s) certificat(s) racine de la CA du domaine.
    Partagé avec les domaines fédérés pour valider les SVIDs cross-domain.
    """
    trust_domain:  str
    cle_ca:        str      # Clef publique CA (simulée)
    version:       int = 1
    emis_a:        float = field(default_factory=time.time)

    def peut_valider(self, svid: SVID) -> bool:
        """Vérifier que ce bundle peut valider ce SVID."""
        return (svid.trust_domain == self.trust_domain and
                svid.bundle_ca == self.cle_ca)


# ─── WORKLOAD API ─────────────────────────────────────────────────────────────

class WorkloadAPI:
    """
    Interface entre le SPIRE Agent et le workload.
    En production : socket Unix /run/spire/sockets/agent.sock
    Protocole     : gRPC (WorkloadService)
    """

    def __init__(self, agent: "SPIREAgent"):
        self._agent = agent

    def fetch_x509_svids(self, pid: int,
                          metadata: dict) -> list[SVID]:
        """
        Appel principal du workload : "donne-moi mon SVID".
        L'agent atteste le workload et retourne son SVID.
        """
        return self._agent.attester_et_delivrer(pid, metadata)

    def fetch_trust_bundles(self) -> list[TrustBundle]:
        """Retourner les trust bundles connus (pour valider les pairs)."""
        return self._agent.trust_bundles()


# ─── SPIRE AGENT ─────────────────────────────────────────────────────────────

class SPIREAgent:
    """
    SPIRE Agent — tourne sur chaque nœud du cluster.
    Rôles :
      1. S'attester auprès du SPIRE Server (prouve qu'il est sur le bon nœud)
      2. Attester les workloads (prouve qu'un processus est le bon service)
      3. Mettre en cache les SVIDs et les renouveler proactivement
      4. Exposer l'API Workload (socket Unix)
    """

    SEUIL_RENOUVELLEMENT = 0.34   # Renouveler quand < 34% de vie restante

    def __init__(self, node_id: str, server: "SPIREServer"):
        self.node_id = node_id
        self._server = server
        self._cache: dict[str, SVID] = {}    # spiffe_id → SVID
        self._lock   = threading.Lock()
        self._stats  = defaultdict(int)
        self._bundles: list[TrustBundle] = []
        # L'agent s'atteste auprès du serveur au démarrage
        self._atteste = False
        self._attester_noeud()

    def _attester_noeud(self):
        """L'agent prouve son identité au SPIRE Server (node attestation)."""
        ok = self._server.attester_agent(self.node_id)
        self._atteste = ok
        if ok:
            self._bundles = self._server.trust_bundles()

    def attester_et_delivrer(self, pid: int, metadata: dict) -> list[SVID]:
        """
        Workload attestation :
          1. Collecter les sélecteurs du processus (namespace, SA, labels...)
          2. Interroger le SPIRE Server pour trouver les entries correspondantes
          3. Récupérer ou renouveler les SVIDs
          4. Retourner au workload
        """
        if not self._atteste:
            return []

        selectors = self._extraire_selectors(pid, metadata)
        entries   = self._server.trouver_entries(selectors)
        svids     = []

        for entry in entries:
            spiffe_id = entry.spiffe_id
            svid      = self._obtenir_svid(spiffe_id, entry.ttl_s)
            if svid:
                svids.append(svid)
                self._stats["delivres"] += 1

        return svids

    def _extraire_selectors(self, pid: int, metadata: dict) -> list[str]:
        """
        Extraire les sélecteurs du workload depuis le kernel / API k8s.
        En production :
          - Kubernetes : lit le ServiceAccount via l'API k8s
          - Linux      : lit /proc/<pid>/exe, UID, GID
          - Docker     : inspecte les labels du conteneur
        """
        selectors = []
        # Sélecteurs Kubernetes simulés
        if "namespace" in metadata:
            selectors.append(f"k8s:ns:{metadata['namespace']}")
        if "service_account" in metadata:
            selectors.append(f"k8s:sa:{metadata['service_account']}")
        if "pod_label" in metadata:
            for k, v in metadata["pod_label"].items():
                selectors.append(f"k8s:pod-label:{k}:{v}")
        # Sélecteur Unix
        if "uid" in metadata:
            selectors.append(f"unix:uid:{metadata['uid']}")
        return selectors

    def _obtenir_svid(self, spiffe_id: str, ttl_s: float) -> Optional[SVID]:
        """Obtenir un SVID depuis le cache ou le renouveler."""
        with self._lock:
            cached = self._cache.get(spiffe_id)
            if cached and cached.est_valide:
                # Renouveler si < seuil de vie restante
                if cached.pct_vie_restante < self.SEUIL_RENOUVELLEMENT * 100:
                    self._stats["renouvellements"] += 1
                    nouveau = self._server.emettre_svid(spiffe_id, ttl_s)
                    if nouveau:
                        self._cache[spiffe_id] = nouveau
                        return nouveau
                return cached
            # Pas en cache ou expiré → demander au serveur
            svid = self._server.emettre_svid(spiffe_id, ttl_s)
            if svid:
                self._cache[spiffe_id] = svid
                self._stats["emis_frais"] += 1
            return svid

    def trust_bundles(self) -> list[TrustBundle]:
        return list(self._bundles)

    def stats(self) -> dict:
        return dict(self._stats)


# ─── SPIRE SERVER ─────────────────────────────────────────────────────────────

class SPIREServer:
    """
    SPIRE Server — cœur du système.
    Unique par trust domain (ou pair HA en production).
    Rôles :
      1. Maintenir le registre des entries (qui → quelle identité)
      2. Attester les agents (vérifier qu'ils sont sur de vrais nœuds)
      3. Émettre les SVIDs signés par la CA interne
      4. Gérer la fédération avec d'autres trust domains
    """

    def __init__(self, trust_domain: str):
        self.trust_domain = trust_domain
        self._secret      = hashlib.sha256(trust_domain.encode()).hexdigest()
        self._entries: list[Entry] = []
        self._agents_attestes: set[str] = set()
        self._svids_emis: list[SVID] = []
        self._federes: dict[str, TrustBundle] = {}   # domain → bundle
        self._stats = defaultdict(int)

    @property
    def trust_bundle(self) -> TrustBundle:
        return TrustBundle(
            trust_domain = self.trust_domain,
            cle_ca       = self._secret[:32],
        )

    def trust_bundles(self) -> list[TrustBundle]:
        """Retourner notre bundle + ceux des domaines fédérés."""
        return [self.trust_bundle] + list(self._federes.values())

    def enregistrer_entry(self, entry: Entry):
        """Enregistrer une association sélecteurs → SPIFFE ID."""
        self._entries.append(entry)

    def attester_agent(self, node_id: str) -> bool:
        """
        Node attestation : vérifier que l'agent tourne sur un vrai nœud.
        En prod : TPM, AWS Instance Identity Document, k8s PSAT token...
        """
        # Simulé : tout nœud avec node_id valide est accepté
        self._agents_attestes.add(node_id)
        self._stats["agents_attestes"] += 1
        return True

    def trouver_entries(self, selectors: list[str]) -> list[Entry]:
        """
        Trouver les entries dont les sélecteurs sont tous présents
        dans les sélecteurs du workload (sous-ensemble).
        """
        sel_set = set(selectors)
        return [e for e in self._entries
                if set(e.selectors).issubset(sel_set)]

    def emettre_svid(self, spiffe_id: str, ttl_s: float = 3600) -> Optional[SVID]:
        """
        Émettre un SVID signé par la CA du trust domain.
        En production : génère une vraie clef X.509 avec openssl.
        """
        # Vérifier que le spiffe_id appartient bien à ce trust domain
        if not spiffe_id.startswith(f"spiffe://{self.trust_domain}/"):
            return None

        workload = spiffe_id.split(f"spiffe://{self.trust_domain}/", 1)[1]
        now      = time.time()
        # Clef privée simulée : HMAC(secret_CA, spiffe_id + timestamp)
        msg      = f"{spiffe_id}:{now}".encode()
        cle      = hmac.new(self._secret.encode(), msg, hashlib.sha256).hexdigest()

        svid = SVID(
            spiffe_id    = spiffe_id,
            trust_domain = self.trust_domain,
            workload     = workload,
            emis_a       = now,
            expire_a     = now + ttl_s,
            cle_privee   = cle,
            bundle_ca    = self._secret[:32],
        )
        self._svids_emis.append(svid)
        self._stats["svids_emis"] += 1
        return svid

    def federer(self, autre_server: "SPIREServer"):
        """
        Établir une fédération bidirectionnelle avec un autre trust domain.
        Échange les trust bundles → services cross-domain peuvent se valider.
        """
        self._federes[autre_server.trust_domain] = autre_server.trust_bundle
        autre_server._federes[self.trust_domain] = self.trust_bundle
        self._stats["federations"] += 1

    def valider_svid(self, svid: SVID) -> tuple[bool, str]:
        """Valider un SVID (local ou fédéré)."""
        if not svid.est_valide:
            return False, "SVID expiré ou révoqué"

        # SVID local
        if svid.trust_domain == self.trust_domain:
            if svid.bundle_ca != self._secret[:32]:
                return False, "Bundle CA invalide"
            return True, "SVID local valide"

        # SVID fédéré
        bundle = self._federes.get(svid.trust_domain)
        if not bundle:
            return False, f"Trust domain '{svid.trust_domain}' non fédéré"
        if not bundle.peut_valider(svid):
            return False, "Bundle CA fédéré invalide"
        return True, f"SVID fédéré valide ({svid.trust_domain})"

    def stats(self) -> dict:
        return dict(self._stats)
