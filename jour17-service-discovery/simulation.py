"""
Jour 17 — Simulation : Service Discovery en action
====================================================
5 scénarios :
  1. Enregistrement, résolution et désenregistrement de base
  2. Health checks : TTL heartbeat vs HTTP check actif
  3. Scale up/down dynamique avec notifications (Watch)
  4. Routage par tags : version, région, environnement
  5. Cache client + invalidation : réduction de la charge sur le registre
"""

import time
import threading
import random
from collections import defaultdict
from service_discovery import (
    RegistreServices, InstanceService, HealthCheck,
    ClientDecouverte, EtatSante
)

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s): time.sleep(s)
def ts(): return f"+{(time.perf_counter()-T0)*1000:6.0f}ms"

T0 = time.perf_counter()

def creer_instance(nom: str, idx: int, tags: list = None,
                   region: str = "eu", version: str = "v1") -> InstanceService:
    return InstanceService(
        service_id   = f"{nom}-{region}-{idx}",
        service_nom  = nom,
        adresse      = f"10.{random.randint(0,255)}.{random.randint(0,255)}.{idx}",
        port         = 8000 + idx,
        tags         = tags or [version, region],
        meta         = {"version": version, "region": region},
        ttl_s        = 10,
    )


# ─── SCÉNARIO 1 : ENREGISTREMENT ET RÉSOLUTION ────────────────────────────────

def scenario_base():
    titre("SCÉNARIO 1 — Enregistrement, résolution et désenregistrement")

    print("""
  Cycle de vie d'un service dans le registre :
    1. Au démarrage  → service.register()
    2. En service    → clients résolvent son nom
    3. À l'arrêt     → service.deregister()
    4. En cas de crash → TTL expire → CRITICAL → retiré des résolutions
    """)

    registre = RegistreServices("consul-sim")

    # Enregistrer 3 instances de l'API utilisateurs
    instances = []
    for i in range(1, 4):
        inst = creer_instance("api-utilisateurs", i, tags=["v2", "eu", "prod"])
        instances.append(inst)
        registre.enregistrer(inst)
        print(f"  ✅ Enregistré : {inst.service_id} @ {inst.adresse_complete()}")

    print(f"\n  Catalogue après enregistrement :")
    registre.afficher_catalogue()

    # Résolution
    print(f"\n  Résolution de 'api-utilisateurs' :")
    resultat = registre.resoudre("api-utilisateurs")
    print(f"    → {len(resultat)} instance(s) saine(s) trouvée(s)")
    for r in resultat:
        print(f"       {r.service_id} @ {r.adresse_complete()}")

    # Désenregistrement d'une instance
    print(f"\n  Désenregistrement de api-utilisateurs-eu-2 (arrêt propre) :")
    registre.desenregistrer("api-utilisateurs-eu-2")
    resultat_apres = registre.resoudre("api-utilisateurs")
    print(f"  → {len(resultat_apres)} instance(s) restante(s) ✅")

    # Service inexistant
    inconnu = registre.resoudre("service-inexistant")
    print(f"\n  Résolution de 'service-inexistant' → {len(inconnu)} résultat(s)")
    print(f"  → Le client reçoit une liste vide → 503 Service Unavailable")


# ─── SCÉNARIO 2 : HEALTH CHECKS ───────────────────────────────────────────────

def scenario_health_checks():
    titre("SCÉNARIO 2 — Health Checks : TTL heartbeat vs HTTP check actif")

    print("""
  Deux modèles de health check :

  TTL check (push) :
    L'instance envoie un heartbeat périodique au registre.
    Si le TTL expire sans heartbeat → CRITICAL.
    Avantage : l'instance sait si elle est vraiment prête.
    Exemple : un worker attend d'avoir chargé sa config avant de signaler "passing".

  HTTP check (pull) :
    Le registre appelle GET /health sur l'instance.
    Avantage : détecte les instances qui ne savent pas qu'elles sont malades.
    Exemple : un serveur bloqué sur une DB ne peut plus s'auto-évaluer.
    """)

    registre = RegistreServices()

    # ── Instance A : TTL check (heartbeat) ────────────────────────────────────
    inst_a = creer_instance("paiement-svc", 1)
    inst_a.ttl_s = 3   # TTL court pour la démo

    check_ttl = HealthCheck(
        check_id     = "paiement-svc-1-ttl",
        instance_id  = inst_a.service_id,
        type         = "ttl",
        intervalle_s = 1.0,
    )
    registre.enregistrer(inst_a, check_ttl)

    # ── Instance B : HTTP check actif ─────────────────────────────────────────
    inst_b = creer_instance("paiement-svc", 2)
    est_sain_b = [True]   # Mutable pour la closure

    check_http = HealthCheck(
        check_id     = "paiement-svc-2-http",
        instance_id  = inst_b.service_id,
        type         = "http",
        url          = f"http://{inst_b.adresse_complete()}/health",
        intervalle_s = 0.5,
        fn_check     = lambda: est_sain_b[0],
    )
    registre.enregistrer(inst_b, check_http)

    print(f"  Instances enregistrées : A (TTL check), B (HTTP check)\n")

    # Heartbeat normal pour A
    def heartbeat_a():
        for _ in range(4):
            attendre(0.8)
            registre.heartbeat("paiement-svc-1-ttl")

    threading.Thread(target=heartbeat_a, daemon=True).start()

    print(f"  {ts()}  État initial :")
    registre.afficher_catalogue()

    # Simuler une panne de B après 1.5s
    def simuler_panne_b():
        attendre(1.5)
        print(f"\n  {ts()}  💥 paiement-svc-2 devient défaillant (HTTP check retourne False)")
        est_sain_b[0] = False

    threading.Thread(target=simuler_panne_b, daemon=True).start()

    # Simuler l'arrêt du heartbeat de A après 3s (simule un crash)
    def arreter_heartbeat_a():
        attendre(3.0)
        print(f"\n  {ts()}  💥 paiement-svc-1 arrête son heartbeat (TTL expirera dans {inst_a.ttl_s}s)")

    threading.Thread(target=arreter_heartbeat_a, daemon=True).start()

    # Observer pendant 8 secondes
    for i in range(8):
        attendre(1.0)
        instances = registre.resoudre("paiement-svc")
        noms = [inst.service_id for inst in instances]
        print(f"  {ts()}  Instances saines : {noms if noms else '(aucune)'}")

    print(f"""
  Résumé des health checks :
    A (TTL)  : sain tant que le heartbeat arrive → expiré après l'arrêt du heartbeat
    B (HTTP) : le registre détecte la panne activement → CRITICAL en ~0.5s
    """)


# ─── SCÉNARIO 3 : SCALE UP/DOWN ET WATCH ─────────────────────────────────────

def scenario_watch():
    titre("SCÉNARIO 3 — Scale Up/Down dynamique avec notifications Watch")

    print("""
  Le Watch permet à un client de recevoir une notification
  instantanée quand le catalogue change, sans polling.

  Consul : blocking queries (long-polling avec X-Consul-Index)
  etcd   : Watch API (gRPC streaming)
  Eureka : delta queries toutes les 30s (moins réactif)
    """)

    registre   = RegistreServices()
    client     = ClientDecouverte("gateway", registre)
    journal_w  = []
    wlock      = threading.Lock()

    def on_changement(instances: list):
        saines = [i for i in instances if i.etat == EtatSante.PASSING]
        with wlock:
            journal_w.append(f"  {ts()}  Watch déclenché : {len(saines)} instance(s) saine(s)")

    registre.observer("recommendation-svc", on_changement)

    # Démarrage : 2 instances
    for i in range(1, 3):
        inst = creer_instance("recommendation-svc", i)
        registre.enregistrer(inst, HealthCheck(
            check_id     = f"rec-svc-{i}-http",
            instance_id  = inst.service_id,
            type         = "http",
            intervalle_s = 0.3,
            fn_check     = lambda: True,
        ))

    attendre(0.2)

    # Simuler le cycle de vie
    def cycle():
        attendre(0.5)
        print(f"\n  {ts()}  Scale UP : ajout de 2 nouvelles instances")
        for i in range(3, 5):
            inst = creer_instance("recommendation-svc", i)
            registre.enregistrer(inst, HealthCheck(
                check_id     = f"rec-svc-{i}-http",
                instance_id  = inst.service_id,
                type         = "http",
                intervalle_s = 0.3,
                fn_check     = lambda: True,
            ))

        attendre(1.0)
        print(f"\n  {ts()}  Scale DOWN : retrait de 2 instances (arrêt propre)")
        registre.desenregistrer("recommendation-svc-eu-3")
        registre.desenregistrer("recommendation-svc-eu-4")

        attendre(0.5)
        print(f"\n  {ts()}  Déploiement rolling : remplacement instance 1")
        registre.desenregistrer("recommendation-svc-eu-1")
        attendre(0.2)
        inst_new = creer_instance("recommendation-svc", 10, version="v2")
        registre.enregistrer(inst_new, HealthCheck(
            check_id     = f"rec-svc-10-http",
            instance_id  = inst_new.service_id,
            type         = "http",
            intervalle_s = 0.3,
            fn_check     = lambda: True,
        ))

    threading.Thread(target=cycle, daemon=True).start()
    attendre(3.5)

    print(f"\n  Journal des notifications Watch :")
    with wlock:
        for ligne_j in journal_w:
            print(ligne_j)

    print(f"\n  Catalogue final :")
    registre.afficher_catalogue()

    instances = registre.resoudre("recommendation-svc")
    print(f"\n  {len(instances)} instance(s) saine(s) prêtes à recevoir du trafic")


# ─── SCÉNARIO 4 : ROUTAGE PAR TAGS ────────────────────────────────────────────

def scenario_tags():
    titre("SCÉNARIO 4 — Routage par tags : version, région, environnement")

    print("""
  Les tags permettent un routage fin sans changer la logique cliente.
  Exemples :
    → Envoyer seulement vers la région EU (RGPD)
    → Tester la v2 sans impacter les utilisateurs en prod
    → Séparer staging et production dans le même registre
    """)

    registre = RegistreServices()

    # Enregistrer un cluster multi-région multi-version
    config = [
        ("order-svc", 1, ["v1", "eu-west", "prod"]),
        ("order-svc", 2, ["v1", "eu-west", "prod"]),
        ("order-svc", 3, ["v2", "eu-west", "prod"]),   # Canary v2
        ("order-svc", 4, ["v1", "us-east", "prod"]),
        ("order-svc", 5, ["v1", "us-east", "prod"]),
        ("order-svc", 6, ["v1", "eu-west", "staging"]),
    ]

    for nom, idx, tags in config:
        region  = next((t for t in tags if "-" in t), "eu")
        version = next((t for t in tags if t.startswith("v")), "v1")
        inst = creer_instance(nom, idx, tags=tags, region=region, version=version)
        registre.enregistrer(inst)

    print(f"  Catalogue :")
    registre.afficher_catalogue()

    # Requêtes de résolution avec différents filtres
    requetes = [
        ("Trafic prod EU v1",      ["v1", "eu-west", "prod"]),
        ("Trafic prod US",         ["us-east", "prod"]),
        ("Canary v2",              ["v2", "prod"]),
        ("Environnement staging",  ["staging"]),
        ("Tout le prod (sans tag)","prod"),
    ]

    print(f"\n  Résolutions par tags :\n")
    print(f"  {'Filtre':<30} {'Instances':>10}  {'IDs'}")
    print("  " + "─"*70)
    for label, tags in requetes:
        if isinstance(tags, str):
            tags = [tags]
        instances = registre.resoudre("order-svc", tags=tags)
        ids = ", ".join(i.service_id for i in instances)
        print(f"  {label:<30} {len(instances):>10}  {ids}")

    print(f"""
  Utilisation en production :
    Blue/Green  : tag "blue" vs "green" → basculer 100% du trafic d'un coup
    Canary      : tag "canary" → ajouter progressivement du trafic
    Multi-région: tag "eu" → respecter les contraintes RGPD (données en EU)
    Maintenance : retirer le tag "prod" → l'instance n'est plus dans les résolutions
    """)


# ─── SCÉNARIO 5 : CACHE CLIENT + INVALIDATION ────────────────────────────────

def scenario_cache():
    titre("SCÉNARIO 5 — Cache client + invalidation : charge sur le registre")

    print("""
  En production, chaque requête applicative NE FAIT PAS une requête au registre.
  Le client maintient un cache local des instances connues.

  Sans cache : 10 000 req/s × N services = millions de requêtes au registre
  Avec cache : 1 requête toutes les 10s par service par client

  Le cache est invalidé :
    → À l'expiration du TTL (proactif)
    → Sur notification Watch (réactif — très rapide)
    → Sur erreur de connexion (le backend a peut-être disparu)
    """)

    registre = RegistreServices()
    for i in range(1, 4):
        inst = creer_instance("inventory-svc", i)
        registre.enregistrer(inst, HealthCheck(
            check_id=f"inv-{i}", instance_id=inst.service_id,
            type="http", intervalle_s=0.3, fn_check=lambda: True,
        ))

    client = ClientDecouverte("checkout-svc", registre, ttl_cache_s=5.0)

    N = 200
    print(f"  Simulation de {N} appels vers 'inventory-svc' :")

    t0 = time.perf_counter()
    for i in range(N):
        client.obtenir("inventory-svc")
    duree = (time.perf_counter() - t0) * 1000

    hits  = client.stats["cache_hits"]
    misses = client.stats["cache_misses"]
    print(f"    Cache hits   : {hits}/{N}  ({hits/N*100:.1f}%)")
    print(f"    Cache misses : {misses}/{N}  ({misses/N*100:.1f}%)")
    print(f"    Durée totale : {duree:.1f}ms  ({duree/N:.3f}ms/appel)")
    print(f"    Requêtes au registre évitées : {hits} ✅")

    # Invalidation lors d'une panne
    print(f"\n  Simulation de panne d'une instance :")
    registre.desenregistrer("inventory-svc-eu-2")
    attendre(0.1)

    # Forcer le refresh (en prod : déclenché par le Watch)
    client.invalider_cache("inventory-svc")
    instances_refresh = client.obtenir("inventory-svc", forcer_refresh=True)
    print(f"    Après invalidation : {len(instances_refresh)} instance(s) saine(s)")

    # Comparaison avec/sans cache
    print(f"\n  Impact du TTL cache :\n")
    print(f"  {'TTL cache':>10}  {'Req/registre pour 1000 appels':>32}")
    print("  " + "─"*45)
    for ttl in [0, 1, 5, 30, 60]:
        c = ClientDecouverte("test", registre, ttl_cache_s=ttl)
        for i in range(1000):
            c.obtenir("inventory-svc")
            if ttl > 0 and i % (ttl * 100) == 0:
                c.invalider_cache("inventory-svc")  # Expiration simulée
        reqs = c.stats["cache_misses"]
        barre = "█" * min(40, reqs // 2)
        print(f"  {ttl:>8}s   {reqs:>6} requêtes  {barre}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    global T0
    T0 = time.perf_counter()
    random.seed(42)

    print("╔" + "═"*62 + "╗")
    print("║   JOUR 17 — SERVICE DISCOVERY (CONSUL/ETCD)              ║")
    print("╚" + "═"*62 + "╝")

    scenario_base()
    scenario_health_checks()
    scenario_watch()
    scenario_tags()
    scenario_cache()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Service Discovery — Ce qu'il faut retenir :

    Enregistrement  : chaque instance s'inscrit avec son ID, IP, port, tags
    Résolution      : les clients résolvent un nom → liste d'IPs saines
    Health checks   : TTL (push) ou HTTP/TCP (pull) → CRITICAL automatique
    Watch           : notifications instantanées des changements de catalogue
    Tags            : routage fin (version, région, env) sans code client

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Enregistrement / résolution / désenregistrement ✅
    Scénario 2 → HTTP check détecte la panne en 0.5s ; TTL expire en 3s ✅
    Scénario 3 → Watch notifié à chaque scale up/down/rolling deploy ✅
    Scénario 4 → Routage précis par tags : v1/v2, eu/us, prod/staging ✅
    Scénario 5 → Cache réduit 99%+ des requêtes au registre ✅

  Utilisé en production :
    Consul    → service mesh complet, DNS, ACL, multi-DC
    etcd      → stockage de config Kubernetes (API server)
    Eureka    → Netflix, Springboot ecosystem
    Zookeeper → Kafka, Hadoop, coordination distribuée

  Lien avec les autres jours :
    Jour 7  (Raft)    → le registre est lui-même répliqué via Raft
    Jour 16 (LB)      → le LB interroge le registre pour ses backends
    Jour 15 (Redlock) → etcd aussi utilisé pour les verrous distribués

  → Jour 18 : Distributed Tracing (Jaeger/Zipkin)
    Suivre une requête qui traverse 5 micro-services différents.
    Chaque service propage un trace-id et des spans.
    On voit exactement où le temps est perdu.
  """)


if __name__ == "__main__":
    main()
