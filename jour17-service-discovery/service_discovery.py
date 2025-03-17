"""
Jour 17 — Service Discovery (Consul/Etcd)
==========================================
Problème : dans un cluster dynamique, les IPs changent constamment.
  → Un Pod Kubernetes redémarre → nouvelle IP
  → Un service scale up → nouvelles instances inconnues des clients
  → Configuration statique impossible à maintenir

Solution : Service Discovery
  Chaque service s'ENREGISTRE au démarrage dans un registre central.
  Les clients RÉSOLVENT le nom du service pour obtenir les IPs actuelles.
  Des HEALTH CHECKS retirent automatiquement les instances défaillantes.

Deux modèles :

  1. Client-side discovery (Netflix Eureka, Consul) :
     Le client interroge le registre lui-même et choisit l'instance.
     ✅ Client contrôle le load balancing
     ❌ Chaque client doit implémenter la logique de découverte

  2. Server-side discovery (AWS ALB + ECS, Kubernetes Services) :
     Un routeur intermédiaire interroge le registre et forwarde.
     ✅ Clients simplifiés (ils parlent toujours à la même adresse)
     ❌ Routeur est un SPOF potentiel (mais souvent répliqué)

Architecture d'un registre (Consul/etcd) :
  - Stockage clé-valeur distribué (Raft en dessous — Jour 7)
  - API d'enregistrement : PUT /v1/agent/service/register
  - API de résolution  : GET /v1/health/service/{name}?passing=true
  - Health checks : TCP, HTTP, gRPC, script, TTL
  - TTL sur les enregistrements : service mort → auto-désenregistrement
  - Watch/Subscribe : notification en cas de changement du catalogue

Lien avec les autres jours :
  Jour 7  (Raft)        : le registre est lui-même un système CP
  Jour 15 (Redlock)     : etcd aussi utilisé pour les verrous distribués
  Jour 16 (LB)          : le LB utilise le registre pour ses backends
"""

import time
import uuid
import threading
import random
import hashlib
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
from enum import Enum
from collections import defaultdict


# ─── ÉTAT DE SANTÉ ───────────────────────────────────────────────────────────

class EtatSante(Enum):
    PASSING  = "passing"    # Sain, accepte du trafic
    WARNING  = "warning"    # Dégradé, encore utilisable
    CRITICAL = "critical"   # Défaillant, retiré du pool


# ─── INSTANCE DE SERVICE ─────────────────────────────────────────────────────

@dataclass
class InstanceService:
    """
    Une instance d'un service enregistrée dans le catalogue.
    Équivalent d'un ServiceEntry dans Consul.
    """
    service_id:  str          # ID unique de cette instance (ex: "api-eu-1a-abc123")
    service_nom: str          # Nom logique du service (ex: "api-utilisateurs")
    adresse:     str          # IP ou hostname
    port:        int
    tags:        list[str]    = field(default_factory=list)  # version, région, env…
    meta:        dict         = field(default_factory=dict)  # métadonnées arbitraires
    enregistre_a: float       = field(default_factory=time.time)
    derniere_vue: float       = field(default_factory=time.time)
    etat:        EtatSante    = EtatSante.PASSING
    ttl_s:       int          = 30    # TTL en secondes (si pas de heartbeat → CRITICAL)

    def adresse_complete(self) -> str:
        return f"{self.adresse}:{self.port}"

    def est_saine(self) -> bool:
        return self.etat == EtatSante.PASSING

    def age_s(self) -> float:
        return time.time() - self.derniere_vue


# ─── HEALTH CHECK ────────────────────────────────────────────────────────────

@dataclass
class HealthCheck:
    """
    Définit comment vérifier la santé d'une instance.
    Le registre exécute ces checks périodiquement.
    """
    check_id:     str
    instance_id:  str
    type:         str          # "http", "tcp", "ttl", "script"
    intervalle_s: float = 5.0
    timeout_s:    float = 2.0
    # Pour HTTP
    url:          str = ""
    # Pour TTL : l'instance doit appeler /agent/check/pass/<check_id> périodiquement
    # Pour TCP : on simule juste la connectivité

    # Fonction de vérification (simulée)
    fn_check:     Optional[Callable[[], bool]] = field(default=None, repr=False)
    dernier_etat: EtatSante = EtatSante.PASSING
    derniere_exec: float    = 0.0


# ─── REGISTRE DE SERVICES ────────────────────────────────────────────────────

class RegistreServices:
    """
    Registre central de services, inspiré de Consul.

    En production, ce registre est lui-même répliqué via Raft
    sur 3 ou 5 nœuds pour la haute disponibilité.
    Ici on simule un registre unique pour la clarté.

    API principale :
      enregistrer(instance, health_check)  → registration
      desenregistrer(service_id)           → deregistration
      resoudre(service_nom, tags=[])        → list[InstanceService]
      heartbeat(check_id)                  → renouvelle le TTL
      observer(service_nom, callback)      → notifications de changement
    """

    def __init__(self, nom: str = "registre-principal"):
        self.nom      = nom
        self._catalog: dict[str, InstanceService] = {}   # service_id → instance
        self._checks:  dict[str, HealthCheck]     = {}   # check_id → check
        self._watchers: dict[str, list[Callable]] = defaultdict(list)
        self._lock     = threading.RLock()
        self._actif    = True

        self.stats = {
            "enregistrements":    0,
            "desenregistrements": 0,
            "resolutions":        0,
            "checks_executes":    0,
            "changements_etat":   0,
            "notifications":      0,
        }
        self.journal: list[dict] = []

        # Lancer les boucles de background
        threading.Thread(target=self._boucle_health_checks, daemon=True).start()
        threading.Thread(target=self._boucle_ttl_expiration, daemon=True).start()

    # ── Enregistrement ────────────────────────────────────────────────────────

    def enregistrer(
        self,
        instance: InstanceService,
        check:    Optional[HealthCheck] = None,
    ) -> bool:
        with self._lock:
            self._catalog[instance.service_id] = instance
            if check:
                check.instance_id = instance.service_id
                self._checks[check.check_id] = check
            self.stats["enregistrements"] += 1
            self._log("REGISTER", instance.service_id,
                      f"{instance.service_nom} @ {instance.adresse_complete()}")
            self._notifier(instance.service_nom)
        return True

    def desenregistrer(self, service_id: str) -> bool:
        with self._lock:
            instance = self._catalog.pop(service_id, None)
            if not instance:
                return False
            # Supprimer les checks associés
            to_del = [cid for cid, c in self._checks.items()
                      if c.instance_id == service_id]
            for cid in to_del:
                del self._checks[cid]
            self.stats["desenregistrements"] += 1
            self._log("DEREGISTER", service_id, instance.service_nom)
            self._notifier(instance.service_nom)
        return True

    # ── Résolution ────────────────────────────────────────────────────────────

    def resoudre(
        self,
        service_nom: str,
        tags:        list[str] = None,
        etat:        EtatSante = EtatSante.PASSING,
    ) -> list[InstanceService]:
        """
        Résout un nom de service en liste d'instances saines.
        Filtre optionnel par tags (version, région, environnement…).
        """
        with self._lock:
            self.stats["resolutions"] += 1
            instances = [
                inst for inst in self._catalog.values()
                if inst.service_nom == service_nom
                and inst.etat == etat
                and (not tags or all(t in inst.tags for t in tags))
            ]
            return list(instances)

    def resoudre_une(
        self,
        service_nom: str,
        tags:        list[str] = None,
        strategie:   str = "random",
    ) -> Optional[InstanceService]:
        """Résout et retourne une instance selon la stratégie."""
        instances = self.resoudre(service_nom, tags)
        if not instances:
            return None
        if strategie == "random":
            return random.choice(instances)
        elif strategie == "round_robin":
            # Utilise un hash du temps pour simuler le round-robin
            idx = int(time.time() * 1000) % len(instances)
            return instances[idx]
        return instances[0]

    # ── Heartbeat (TTL check) ─────────────────────────────────────────────────

    def heartbeat(self, check_id: str) -> bool:
        """
        L'instance signale qu'elle est vivante.
        Renouvelle le TTL du check correspondant.
        Analogue au PUT /agent/check/pass/<check_id> de Consul.
        """
        with self._lock:
            check = self._checks.get(check_id)
            if not check:
                return False
            instance = self._catalog.get(check.instance_id)
            if not instance:
                return False
            instance.derniere_vue = time.time()
            ancien_etat = instance.etat
            instance.etat = EtatSante.PASSING
            if ancien_etat != EtatSante.PASSING:
                self.stats["changements_etat"] += 1
                self._log("RECOVER", instance.service_id, instance.service_nom)
                self._notifier(instance.service_nom)
            return True

    # ── Observation (Watch) ───────────────────────────────────────────────────

    def observer(self, service_nom: str, callback: Callable[[list[InstanceService]], None]):
        """
        Souscrit aux changements du catalogue pour un service.
        Le callback est appelé à chaque changement d'état ou d'instances.
        Analogue au blocking query de Consul ou au watch etcd.
        """
        with self._lock:
            self._watchers[service_nom].append(callback)

    def _notifier(self, service_nom: str):
        """Notifie les watchers d'un service (appelé sous lock)."""
        instances = [i for i in self._catalog.values()
                     if i.service_nom == service_nom]
        callbacks = self._watchers.get(service_nom, [])
        self.stats["notifications"] += len(callbacks)
        for cb in callbacks:
            threading.Thread(target=cb, args=(instances,), daemon=True).start()

    # ── Boucles de background ─────────────────────────────────────────────────

    def _boucle_health_checks(self):
        """Exécute les health checks actifs périodiquement."""
        while self._actif:
            time.sleep(0.5)
            with self._lock:
                checks = list(self._checks.values())
            for check in checks:
                if check.fn_check is None:
                    continue
                if time.time() - check.derniere_exec < check.intervalle_s:
                    continue
                check.derniere_exec = time.time()
                try:
                    sain = check.fn_check()
                except Exception:
                    sain = False
                nouvel_etat = EtatSante.PASSING if sain else EtatSante.CRITICAL
                with self._lock:
                    check.dernier_etat = nouvel_etat
                    instance = self._catalog.get(check.instance_id)
                    if instance and instance.etat != nouvel_etat:
                        instance.etat = nouvel_etat
                        self.stats["checks_executes"] += 1
                        self.stats["changements_etat"] += 1
                        self._log(
                            "HEALTH_CHANGE", instance.service_id,
                            f"{instance.service_nom} → {nouvel_etat.value}"
                        )
                        self._notifier(instance.service_nom)

    def _boucle_ttl_expiration(self):
        """Marque CRITICAL les instances dont le TTL a expiré."""
        while self._actif:
            time.sleep(1.0)
            with self._lock:
                for inst in list(self._catalog.values()):
                    if inst.age_s() > inst.ttl_s and inst.etat != EtatSante.CRITICAL:
                        inst.etat = EtatSante.CRITICAL
                        self.stats["changements_etat"] += 1
                        self._log("TTL_EXPIRED", inst.service_id, inst.service_nom)
                        self._notifier(inst.service_nom)

    def _log(self, action: str, service_id: str, detail: str = ""):
        self.journal.append({
            "ts":         time.time(),
            "action":     action,
            "service_id": service_id,
            "detail":     detail,
        })

    # ── Catalogue complet ─────────────────────────────────────────────────────

    def catalogue(self) -> dict[str, list[InstanceService]]:
        """Retourne le catalogue complet groupé par nom de service."""
        with self._lock:
            par_nom = defaultdict(list)
            for inst in self._catalog.values():
                par_nom[inst.service_nom].append(inst)
            return dict(par_nom)

    def afficher_catalogue(self):
        cat = self.catalogue()
        if not cat:
            print("  (catalogue vide)")
            return
        for nom, instances in sorted(cat.items()):
            print(f"  {nom} ({len(instances)} instance(s)) :")
            for inst in instances:
                icone = {"passing": "✅", "warning": "⚠️ ", "critical": "❌"}.get(
                    inst.etat.value, "❓"
                )
                tags_str = f"  [{', '.join(inst.tags)}]" if inst.tags else ""
                print(f"    {icone} {inst.service_id:<28} "
                      f"{inst.adresse_complete():<22}{tags_str}")


# ─── CLIENT DE DÉCOUVERTE ────────────────────────────────────────────────────

class ClientDecouverte:
    """
    Client qui utilise le registre pour découvrir les services.
    Implémente un cache local avec invalidation par watch.

    En production : le client maintient un cache local et
    souscrit aux changements pour éviter une requête au registre
    à chaque appel (trop lent et trop de charge sur le registre).
    """

    def __init__(self, client_id: str, registre: RegistreServices,
                 ttl_cache_s: float = 10.0):
        self.id       = client_id
        self.registre = registre
        self.ttl_cache = ttl_cache_s
        self._cache: dict[str, tuple[list[InstanceService], float]] = {}
        self._lock    = threading.Lock()
        self.stats    = {"cache_hits": 0, "cache_misses": 0, "appels": 0}

        # Souscrire aux changements pour invalider le cache
        # (En prod : blocking queries Consul ou watch etcd)

    def obtenir(self, service_nom: str, tags: list[str] = None,
                forcer_refresh: bool = False) -> list[InstanceService]:
        """Retourne les instances saines, depuis le cache si possible."""
        cle = f"{service_nom}:{','.join(sorted(tags or []))}"
        with self._lock:
            self.stats["appels"] += 1
            if not forcer_refresh and cle in self._cache:
                instances, expires = self._cache[cle]
                if time.time() < expires:
                    self.stats["cache_hits"] += 1
                    return instances
            self.stats["cache_misses"] += 1

        instances = self.registre.resoudre(service_nom, tags)
        with self._lock:
            self._cache[cle] = (instances, time.time() + self.ttl_cache)
        return instances

    def appeler(self, service_nom: str, chemin: str = "/",
                tags: list[str] = None) -> dict:
        """Découvre une instance et simule un appel HTTP."""
        instances = self.obtenir(service_nom, tags)
        if not instances:
            return {"status": 503, "erreur": f"Aucune instance saine pour {service_nom!r}"}
        instance = random.choice(instances)
        # Simuler l'appel
        time.sleep(random.uniform(0.005, 0.020))
        return {
            "status": 200,
            "instance": instance.service_id,
            "adresse": instance.adresse_complete(),
            "chemin": chemin,
        }

    def invalider_cache(self, service_nom: str):
        with self._lock:
            cles = [k for k in self._cache if k.startswith(service_nom + ":")]
            for k in cles:
                del self._cache[k]
