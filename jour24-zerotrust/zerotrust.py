"""
Jour 24 — "Confiance Zéro" — Zero-Trust Networking
=====================================================
Principe : "Never trust, always verify"

Modèle traditionnel (périmètre) :
  Réseau interne = zone de confiance
  Pare-feu à la frontière → tout ce qui est à l'intérieur = sûr
  Problème : un attaquant qui entre une fois a accès à TOUT
  Problème : un employé malveillant sur le réseau interne = danger

Zero-Trust :
  Pas de zone de confiance par défaut — même le réseau interne
  Chaque connexion doit être authentifiée + autorisée
  Moindre privilège : accès minimal nécessaire
  Inspection continue : re-vérifier à chaque requête

Les 4 piliers de Zero-Trust :
  1. IDENTITÉ   : qui es-tu ? (certificat X.509, JWT, SPIFFE SVID)
  2. APPAREIL   : ton appareil est-il sain ? (EDR, patch level)
  3. RÉSEAU     : chiffre tout le trafic (mTLS = mutual TLS)
  4. POLITIQUE  : qu'as-tu le droit de faire ? (OPA, RBAC)

mTLS (mutual TLS) :
  TLS normal    : client vérifie le certificat du SERVEUR
  mTLS          : client ET serveur vérifient mutuellement leurs certificats
  → L'identité du service appelant est prouvée cryptographiquement

SPIFFE (Secure Production Identity Framework For Everyone) :
  Standard open-source pour les identités de workloads
  SVID (SPIFFE Verifiable Identity Document) = certificat X.509 + URI
  Format : spiffe://trust-domain/path
  Ex     : spiffe://acme.com/payments-service
  Rotation automatique (24h par défaut) → clefs compromises expirent vite

Policy Engine (OPA — Open Policy Agent) :
  Décide si une requête est autorisée selon des règles déclaratives (Rego)
  Entrée  : {subject, action, resource, context}
  Sortie  : allow=true/false + raison
  Découplé de l'application → politiques modifiables sans redéploiement
"""

from __future__ import annotations
import time
import hashlib
import hmac
import base64
import json
import re
from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum
from collections import defaultdict


# ─── CERTIFICAT X.509 SIMPLIFIÉ ──────────────────────────────────────────────

@dataclass
class Certificat:
    """
    Représentation simplifiée d'un certificat X.509 / SPIFFE SVID.
    En production : openssl ou cryptography lib génèrent de vrais certificats.
    """
    sujet:        str           # CN du service (ex: payments-service)
    spiffe_uri:   str           # spiffe://trust-domain/service
    emetteur:     str           # CA qui a signé (ex: acme-ca)
    valide_depuis: float        # timestamp
    expire_a:     float         # timestamp
    empreinte:    str           # hash du cert (simule la clef publique)
    revoque:      bool = False

    @property
    def est_valide(self) -> bool:
        now = time.time()
        return (not self.revoque and
                self.valide_depuis <= now <= self.expire_a)

    @property
    def duree_restante(self) -> float:
        return max(0.0, self.expire_a - time.time())

    def __repr__(self):
        statut = "✅ valide" if self.est_valide else "❌ expiré/révoqué"
        return f"Cert({self.sujet}, uri={self.spiffe_uri}, {statut})"


# ─── AUTORITÉ DE CERTIFICATION (CA) ──────────────────────────────────────────

class AutoriteCertification:
    """
    CA racine du cluster — émet et révoque les certificats des services.
    En prod : HashiCorp Vault PKI, cert-manager (k8s), SPIRE.
    Rotation automatique = les certs sont courts (24h) et renouvelés proactivement.
    """

    def __init__(self, nom: str, trust_domain: str):
        self.nom          = nom
        self.trust_domain = trust_domain
        self._secret      = hashlib.sha256(nom.encode()).hexdigest()
        self._emis: dict[str, Certificat] = {}
        self._revokes: set[str] = set()
        self._stats = defaultdict(int)

    def emettre(self, service: str, duree_s: float = 86400) -> Certificat:
        """Émettre un certificat pour un service (SVID)."""
        maintenant = time.time()
        spiffe_uri = f"spiffe://{self.trust_domain}/{service}"
        # Empreinte = HMAC(CA_secret, service + timestamp) → simule signature
        msg        = f"{service}:{maintenant}".encode()
        empreinte  = hmac.new(self._secret.encode(), msg, hashlib.sha256).hexdigest()
        cert = Certificat(
            sujet         = service,
            spiffe_uri    = spiffe_uri,
            emetteur      = self.nom,
            valide_depuis = maintenant,
            expire_a      = maintenant + duree_s,
            empreinte     = empreinte,
        )
        self._emis[spiffe_uri] = cert
        self._stats["emis"] += 1
        return cert

    def revoquer(self, spiffe_uri: str):
        """Révoquer un certificat (ex: service compromis)."""
        if spiffe_uri in self._emis:
            self._emis[spiffe_uri].revoque = True
            self._revokes.add(spiffe_uri)
            self._stats["revoques"] += 1

    def verifier(self, cert: Certificat) -> tuple[bool, str]:
        """Vérifier qu'un certificat a bien été émis par cette CA."""
        if cert.emetteur != self.nom:
            return False, f"Émetteur inconnu : {cert.emetteur}"
        if cert.spiffe_uri not in self._emis:
            return False, "Certificat non enregistré dans cette CA"
        if cert.spiffe_uri in self._revokes:
            return False, "Certificat révoqué"
        if not cert.est_valide:
            return False, f"Certificat expiré ou pas encore valide"
        connu = self._emis[cert.spiffe_uri]
        if connu.empreinte != cert.empreinte:
            return False, "Empreinte invalide (falsification ?)"
        return True, "OK"

    def stats(self) -> dict:
        return dict(self._stats)


# ─── HANDSHAKE mTLS ──────────────────────────────────────────────────────────

@dataclass
class ResultatMTLS:
    succes:         bool
    identite_pair:  Optional[str]    # SPIFFE URI du pair si succès
    raison:         str
    latence_ms:     float = 0.0


class HandshakeMTLS:
    """
    Simule le handshake mTLS entre deux services.

    TLS normal :
      Client → Server : "voici mon ClientHello"
      Server → Client : "voici mon certificat"
      Client vérifie   : cert signé par CA connue ? ✅
      → Chiffrement établi

    mTLS (ajout) :
      Server → Client : "envoie TON certificat aussi"
      Client → Server : "voici MON certificat"
      Server vérifie   : cert signé par CA connue ? ✅
      → Les deux identités sont prouvées cryptographiquement
    """

    def __init__(self, ca: AutoriteCertification):
        self._ca = ca

    def connecter(self, cert_client: Certificat,
                  cert_serveur: Certificat) -> ResultatMTLS:
        t0 = time.perf_counter()

        # Étape 1 : le client vérifie le serveur
        ok_s, raison_s = self._ca.verifier(cert_serveur)
        if not ok_s:
            return ResultatMTLS(False, None,
                                f"Client rejette le serveur : {raison_s}",
                                (time.perf_counter() - t0) * 1000)

        # Étape 2 : le serveur vérifie le client
        ok_c, raison_c = self._ca.verifier(cert_client)
        if not ok_c:
            return ResultatMTLS(False, None,
                                f"Serveur rejette le client : {raison_c}",
                                (time.perf_counter() - t0) * 1000)

        # Étape 3 : tunnel chiffré établi (simulé)
        return ResultatMTLS(
            succes        = True,
            identite_pair = cert_client.spiffe_uri,
            raison        = "mTLS établi",
            latence_ms    = (time.perf_counter() - t0) * 1000,
        )


# ─── POLICY ENGINE (OPA simplifié) ────────────────────────────────────────────

@dataclass
class RequeteAutorisation:
    sujet:    str    # SPIFFE URI du service demandeur
    action:   str    # GET, POST, DELETE, PUBLISH, CONSUME...
    ressource: str   # /api/payments, kafka://orders, hdfs:///data/users
    context:  dict = field(default_factory=dict)


@dataclass
class ResultatAutorisation:
    autorise: bool
    raison:   str
    politique: str   # Nom de la règle qui a décidé


class MoteurPolitique:
    """
    Policy Engine inspiré d'OPA (Open Policy Agent).
    Les politiques sont déclarées en Python (en prod : langage Rego d'OPA).

    Chaque politique est une fonction : RequeteAutorisation → (bool, str)
    Le moteur les évalue dans l'ordre, première décision gagne.
    """

    def __init__(self):
        self._politiques: list[tuple[str, callable]] = []
        self._stats = defaultdict(int)
        self._log: list[dict] = []

    def ajouter_politique(self, nom: str, fn: callable):
        self._politiques.append((nom, fn))

    def evaluer(self, req: RequeteAutorisation) -> ResultatAutorisation:
        for nom, fn in self._politiques:
            resultat = fn(req)
            if resultat is not None:
                autorise, raison = resultat
                cle = "autorise" if autorise else "refuse"
                self._stats[cle] += 1
                self._log.append({
                    "sujet":    req.sujet,
                    "action":   req.action,
                    "ressource": req.ressource,
                    "decision": cle,
                    "politique": nom,
                    "ts":       time.time(),
                })
                return ResultatAutorisation(autorise, raison, nom)

        # Deny by default — comportement Zero-Trust
        self._stats["refuse"] += 1
        return ResultatAutorisation(False, "Aucune règle n'autorise cette requête",
                                    "default-deny")

    def stats(self) -> dict:
        return dict(self._stats)

    def derniers_logs(self, n: int = 10) -> list[dict]:
        return self._log[-n:]


# ─── AGENT ZERO-TRUST ─────────────────────────────────────────────────────────

class AgentZeroTrust:
    """
    Sidecar / proxy Zero-Trust.
    En production : Envoy proxy avec SPIRE pour l'identité.
    Chaque service a son agent qui gère :
      - Le renouvellement automatique du certificat
      - Le handshake mTLS pour chaque connexion
      - La vérification des politiques pour chaque requête
    """

    def __init__(self, service: str, ca: AutoriteCertification,
                 moteur: MoteurPolitique):
        self.service = service
        self._ca     = ca
        self._moteur = moteur
        self._mtls   = HandshakeMTLS(ca)
        self._cert   = ca.emettre(service, duree_s=3600)   # cert 1h
        self._connexions: dict[str, float] = {}   # pairs connectés

    def renouveler_cert(self, duree_s: float = 3600):
        """Rotation automatique du certificat avant expiration."""
        ancien = self._cert
        self._cert = self._ca.emettre(self.service, duree_s=duree_s)
        return ancien, self._cert

    def appeler(self, cible: "AgentZeroTrust",
                action: str, ressource: str,
                context: dict = None) -> dict:
        """
        Appel Zero-Trust complet :
          1. mTLS handshake (identité mutuelle)
          2. Vérification de politique (autorisation)
          3. Exécution (si autorisé)
        """
        t0 = time.perf_counter()

        # Étape 1 : mTLS
        handshake = self._mtls.connecter(self._cert, cible._cert)
        if not handshake.succes:
            return {"ok": False, "etape": "mTLS", "raison": handshake.raison}

        # Étape 2 : autorisation
        req    = RequeteAutorisation(
            sujet     = self._cert.spiffe_uri,
            action    = action,
            ressource = ressource,
            context   = context or {},
        )
        decision = self._moteur.evaluer(req)
        if not decision.autorise:
            return {"ok": False, "etape": "policy",
                    "raison": decision.raison,
                    "politique": decision.politique}

        duree = (time.perf_counter() - t0) * 1000
        return {
            "ok":         True,
            "identite":   handshake.identite_pair,
            "politique":  decision.politique,
            "latence_ms": round(duree, 2),
        }
