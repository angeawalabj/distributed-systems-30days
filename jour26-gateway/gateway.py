"""
Jour 26 — "Le Gardien des Portes" — API Gateway + OPA
=======================================================
API Gateway : point d'entrée unique pour tous les clients externes.
OPA         : moteur de décision pour les politiques d'accès.

Problème :
  500 microservices, chacun implémente son propre :
    - Authentification (JWT, API key, mTLS)
    - Rate limiting (chacun avec sa propre logique)
    - Logging (formats différents, incomplets)
    - CORS, transformation de headers...
  → Duplication massive, incohérences, failles de sécurité oubliées

API Gateway résout ça en centralisant :
  Authentification   → une seule implémentation, partagée
  Rate limiting      → compteurs centralisés (pas de bypass)
  Routing            → /api/payments/* → payments-service
  Transformation     → enrichir les headers, masquer les détails internes
  Observabilité      → traces distribuées, métriques, logs unifiés
  TLS termination    → un seul certificat externe

OPA (Open Policy Agent) :
  Policy-as-Code : les règles d'accès = fichiers Rego dans Git
  Découplé       : pas besoin de redeployer les services pour changer les règles
  Testable       : les politiques ont des tests unitaires
  Audit          : chaque décision loggée avec le contexte complet

  Flux OPA :
    Gateway reçoit une requête
    → construit un "input" JSON {method, path, token, headers...}
    → envoie à OPA : POST /v1/data/authz/allow
    → OPA évalue les règles Rego
    → répond {result: true/false, reason: "..."}
    → Gateway autorise ou rejette

Architecture complète :
  Client
    ↓ HTTPS
  API Gateway  ←→  OPA (policy engine)
    ↓ mTLS (SPIFFE)
  Microservices (payments, analytics, fraud...)
    ↓
  Backends (Kafka, HDFS, DB)
"""

from __future__ import annotations
import time
import hashlib
import hmac
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from collections import defaultdict
from enum import Enum


# ─── TOKEN JWT SIMPLIFIÉ ──────────────────────────────────────────────────────

@dataclass
class Token:
    """JWT simplifié : header.payload.signature (simulé en base64)."""
    subject:  str           # user_id ou service_id
    roles:    list[str]     # ["admin", "read:payments", ...]
    scope:    list[str]     # ["payments:read", "analytics:write"]
    exp:      float         # expiration timestamp
    issued_by: str          # émetteur (IdP)
    _secret: str = field(default="secret", repr=False)

    @property
    def est_valide(self) -> bool:
        return time.time() < self.exp

    def signer(self) -> str:
        """Générer une signature (simulée)."""
        payload = f"{self.subject}:{self.roles}:{self.exp}"
        return hmac.new(self._secret.encode(),
                         payload.encode(), hashlib.sha256).hexdigest()[:16]

    @classmethod
    def creer(cls, subject: str, roles: list[str], scope: list[str],
              duree_s: float = 3600, secret: str = "secret") -> "Token":
        return cls(subject=subject, roles=roles, scope=scope,
                   exp=time.time() + duree_s, issued_by="acme-idp",
                   _secret=secret)


# ─── REQUÊTE / RÉPONSE ────────────────────────────────────────────────────────

@dataclass
class Requete:
    method:   str            # GET, POST, DELETE...
    path:     str            # /api/v1/payments/123
    headers:  dict           # Authorization, X-Request-ID...
    body:     dict = field(default_factory=dict)
    client_ip: str = "0.0.0.0"
    timestamp: float = field(default_factory=time.time)

    @property
    def token(self) -> Optional[Token]:
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return self.headers.get("_token_obj")   # Token parsé
        return None


@dataclass
class Reponse:
    status:  int
    body:    dict
    headers: dict = field(default_factory=dict)
    latence_ms: float = 0.0

    @classmethod
    def ok(cls, body: dict = None) -> "Reponse":
        return cls(200, body or {"status": "ok"})

    @classmethod
    def erreur(cls, status: int, message: str) -> "Reponse":
        return cls(status, {"error": message})


# ─── OPA : MOTEUR DE POLITIQUES ───────────────────────────────────────────────

@dataclass
class InputOPA:
    """Structure d'entrée envoyée à OPA pour évaluation."""
    method:    str
    path:      str
    subject:   str           # user ou service
    roles:     list[str]
    scope:     list[str]
    client_ip: str
    context:   dict = field(default_factory=dict)


@dataclass
class DecisionOPA:
    autorise:  bool
    raison:    str
    politique: str
    latence_ms: float = 0.0


class MoteurOPA:
    """
    Moteur OPA simplifié.
    En production : processus OPA séparé, politiques en langage Rego.
    Ici : politiques Python pour la lisibilité.

    Chaque politique est une fonction Input → (bool, str) | None
    None = "je ne suis pas concerné par cette requête"
    """

    def __init__(self):
        self._politiques: list[tuple[str, Callable]] = []
        self._stats = defaultdict(int)
        self._decisions: list[dict] = []

    def politique(self, nom: str):
        """Décorateur pour enregistrer une politique."""
        def decorator(fn: Callable):
            self._politiques.append((nom, fn))
            return fn
        return decorator

    def evaluer(self, inp: InputOPA) -> DecisionOPA:
        t0 = time.perf_counter()
        for nom, fn in self._politiques:
            res = fn(inp)
            if res is not None:
                autorise, raison = res
                self._stats["autorise" if autorise else "refuse"] += 1
                decision = DecisionOPA(
                    autorise   = autorise,
                    raison     = raison,
                    politique  = nom,
                    latence_ms = (time.perf_counter() - t0) * 1000,
                )
                self._decisions.append({
                    "ts":       time.time(),
                    "subject":  inp.subject,
                    "method":   inp.method,
                    "path":     inp.path,
                    "decision": "ALLOW" if autorise else "DENY",
                    "politique": nom,
                    "raison":   raison,
                })
                return decision

        # Default deny (Zero-Trust)
        self._stats["refuse"] += 1
        return DecisionOPA(False, "Aucune règle n'autorise", "default-deny",
                           (time.perf_counter() - t0) * 1000)

    def stats(self) -> dict:
        return dict(self._stats)

    def audit_log(self, n: int = 20) -> list[dict]:
        return self._decisions[-n:]


def creer_politiques_acme() -> MoteurOPA:
    """
    Politiques d'accès pour un cluster microservices ACME Corp.
    En production : fichiers .rego dans un repo Git, déployés via CI/CD.
    """
    opa = MoteurOPA()

    @opa.politique("health-public")
    def health_check(inp: InputOPA):
        """Les health checks sont toujours accessibles."""
        if inp.path.endswith("/health") and inp.method == "GET":
            return True, "Health check public"
        return None

    @opa.politique("admin-full-access")
    def admin(inp: InputOPA):
        """Les admins ont accès à tout (avec MFA requis pour DELETE)."""
        if "admin" not in inp.roles:
            return None
        if inp.method == "DELETE":
            if not inp.context.get("mfa"):
                return False, "Admin : MFA requis pour DELETE"
        return True, "Admin autorisé"

    @opa.politique("payments-read")
    def payments_lecture(inp: InputOPA):
        if not inp.path.startswith("/api/v1/payments"):
            return None
        if inp.method != "GET":
            return None
        if "payments:read" in inp.scope or "read:payments" in inp.roles:
            return True, "Lecture paiements autorisée"
        return False, "Scope 'payments:read' requis pour lire les paiements"

    @opa.politique("payments-write")
    def payments_ecriture(inp: InputOPA):
        if not inp.path.startswith("/api/v1/payments"):
            return None
        if inp.method not in ("POST", "PUT", "PATCH"):
            return None
        if "payments:write" in inp.scope:
            return True, "Écriture paiements autorisée"
        return False, "Scope 'payments:write' requis"

    @opa.politique("analytics-read")
    def analytics(inp: InputOPA):
        if not inp.path.startswith("/api/v1/analytics"):
            return None
        if inp.method == "GET":
            if "analytics:read" in inp.scope or "analyst" in inp.roles:
                return True, "Lecture analytics autorisée"
            return False, "Rôle 'analyst' ou scope 'analytics:read' requis"
        return None

    @opa.politique("ip-allowlist")
    def ip_allowlist(inp: InputOPA):
        """Certaines routes sont restreintes à des IPs internes."""
        routes_internes = ["/api/internal/", "/api/admin/"]
        if any(inp.path.startswith(r) for r in routes_internes):
            plages_autorisees = ["10.", "172.16.", "192.168."]
            if any(inp.client_ip.startswith(p) for p in plages_autorisees):
                return True, f"IP interne autorisée sur route interne"
            return False, f"IP {inp.client_ip} non autorisée pour les routes internes"
        return None

    @opa.politique("rate-limit-guard")
    def rate_limit_guard(inp: InputOPA):
        """Bloquer les requêtes déjà rate-limitées."""
        if inp.context.get("rate_limited"):
            return False, "Rate limit dépassé"
        return None

    return opa


# ─── RATE LIMITER ────────────────────────────────────────────────────────────

class RateLimiter:
    """
    Token bucket par (subject, path_prefix).
    En production : Redis avec INCR + EXPIRE pour la coordination.
    """

    def __init__(self, requetes_par_minute: int = 60):
        self.rpm   = requetes_par_minute
        self._seaux: dict[str, dict] = defaultdict(
            lambda: {"tokens": requetes_par_minute, "dernier": time.time()}
        )
        self._lock = threading.Lock()

    def autoriser(self, cle: str) -> tuple[bool, int]:
        """
        Retourne (autorisé, tokens_restants).
        Recharge les tokens selon le temps écoulé.
        """
        with self._lock:
            seau    = self._seaux[cle]
            now     = time.time()
            ecoule  = now - seau["dernier"]
            # Recharger proportionnellement au temps
            recharge = ecoule * (self.rpm / 60.0)
            seau["tokens"] = min(self.rpm, seau["tokens"] + recharge)
            seau["dernier"] = now

            if seau["tokens"] >= 1:
                seau["tokens"] -= 1
                return True, int(seau["tokens"])
            return False, 0


# ─── ROUTEUR ─────────────────────────────────────────────────────────────────

@dataclass
class Route:
    prefix:   str
    service:  str
    methodes: list[str] = field(default_factory=lambda: ["GET","POST","PUT","DELETE"])
    strip_prefix: bool = True

    def correspond(self, method: str, path: str) -> bool:
        return (method in self.methodes and path.startswith(self.prefix))

    def transformer(self, path: str) -> str:
        if self.strip_prefix:
            return path[len(self.prefix):] or "/"
        return path


class Routeur:
    def __init__(self):
        self._routes: list[Route] = []

    def ajouter(self, route: Route):
        self._routes.append(route)

    def resoudre(self, method: str, path: str) -> Optional[Route]:
        for route in self._routes:
            if route.correspond(method, path):
                return route
        return None


# ─── API GATEWAY ─────────────────────────────────────────────────────────────

class APIGateway:
    """
    API Gateway : point d'entrée unique.

    Pipeline de traitement pour chaque requête :
      1. TLS termination (simulé)
      2. Validation du token (JWT)
      3. Rate limiting
      4. Décision OPA (autorisation)
      5. Routage vers le service backend
      6. Transformation de la réponse
      7. Logging + métriques
    """

    def __init__(self, opa: MoteurOPA, rate_limiter: RateLimiter,
                 routeur: Routeur):
        self._opa      = opa
        self._rl       = rate_limiter
        self._routeur  = routeur
        self._stats    = defaultdict(int)
        self._latences: list[float] = []

    def traiter(self, req: Requete) -> Reponse:
        t0 = time.perf_counter()
        self._stats["total"] += 1

        # ── Étape 1 : Validation du token ───────────────────────────────
        token = req.token
        if token is None or not token.est_valide:
            self._stats["auth_echec"] += 1
            return Reponse.erreur(401, "Token absent ou expiré")

        # ── Étape 2 : Rate limiting ──────────────────────────────────────
        cle_rl = f"{token.subject}:{req.path.split('/')[2] if len(req.path.split('/')) > 2 else 'root'}"
        rl_ok, tokens_restants = self._rl.autoriser(cle_rl)
        if not rl_ok:
            self._stats["rate_limited"] += 1
            return Reponse.erreur(429, "Too Many Requests")

        # ── Étape 3 : Décision OPA ───────────────────────────────────────
        inp = InputOPA(
            method    = req.method,
            path      = req.path,
            subject   = token.subject,
            roles     = token.roles,
            scope     = token.scope,
            client_ip = req.client_ip,
            context   = {
                "rate_limited": not rl_ok,
                "tokens_restants": tokens_restants,
                **req.headers.get("_context", {}),
            },
        )
        decision = self._opa.evaluer(inp)
        if not decision.autorise:
            self._stats["opa_refuse"] += 1
            return Reponse.erreur(403, decision.raison)

        # ── Étape 4 : Routage ─────────────────────────────────────────────
        route = self._routeur.resoudre(req.method, req.path)
        if not route:
            self._stats["not_found"] += 1
            return Reponse.erreur(404, f"Route {req.method} {req.path} introuvable")

        # ── Étape 5 : Réponse simulée du backend ─────────────────────────
        path_backend = route.transformer(req.path)
        reponse = Reponse.ok({
            "service":  route.service,
            "path":     path_backend,
            "method":   req.method,
            "subject":  token.subject,
        })

        # ── Enrichir les headers de réponse ──────────────────────────────
        latence = (time.perf_counter() - t0) * 1000
        reponse.headers = {
            "X-Request-ID":       req.headers.get("X-Request-ID", ""),
            "X-Rate-Limit-Remaining": tokens_restants,
            "X-Policy":           decision.politique,
            "X-Latency-Ms":       f"{latence:.2f}",
        }
        reponse.latence_ms = latence
        self._stats["ok"] += 1
        self._latences.append(latence)
        return reponse

    def stats(self) -> dict:
        s = dict(self._stats)
        if self._latences:
            s["latence_moy_ms"] = round(sum(self._latences) / len(self._latences), 2)
            s["latence_p99_ms"] = round(sorted(self._latences)[int(len(self._latences)*0.99)], 2)
        return s
