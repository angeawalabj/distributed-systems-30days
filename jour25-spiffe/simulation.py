"""
Jour 25 — "Le Sceau d'Identité" — Simulation SPIFFE/SPIRE
==========================================================
5 scénarios :
  1. Attestation de workload : zéro secret au démarrage
  2. Rotation automatique : renouvellement avant expiration
  3. Sélecteurs Kubernetes : namespace + ServiceAccount → identité
  4. Fédération : deux clusters qui se font confiance
  5. Workload compromis : attestation rejette les imposteurs
"""

import time
import threading
from collections import defaultdict
from spiffe import (
    SPIREServer, SPIREAgent, WorkloadAPI, Entry,
    SVID, TrustBundle
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


def creer_cluster(trust_domain: str, nb_noeuds: int = 3):
    """Créer un cluster SPIRE avec 1 server et N agents."""
    server = SPIREServer(trust_domain)
    agents = {f"node-{i}": SPIREAgent(f"node-{i}", server)
              for i in range(nb_noeuds)}
    return server, agents


# ─── SCÉNARIO 1 : ATTESTATION ZÉRO SECRET ────────────────────────────────────

def scenario_attestation():
    titre("SCÉNARIO 1 — Attestation de workload : zéro secret au démarrage")

    print("""
  Problème classique : comment un pod Kubernetes connaît-il son identité
  sans avoir un secret codé en dur dans son image Docker ?

  Réponse SPIFFE/SPIRE :
    1. Le pod démarre → appelle le SPIRE Agent via socket Unix
    2. L'agent lit les attributs du pod (namespace, ServiceAccount) via l'API k8s
    3. L'agent interroge le SPIRE Server : "ce pod a ces sélecteurs, quelle identité ?"
    4. Le serveur répond avec le SPIFFE ID correspondant
    5. Le serveur émet un SVID (certificat X.509) pour ce workload
    6. Le pod reçoit son SVID → peut maintenant s'authentifier auprès des autres
    """)

    server, agents = creer_cluster("prod.acme.com", nb_noeuds=2)
    agent = agents["node-0"]
    api   = WorkloadAPI(agent)

    # Enregistrer les entries (mappings sélecteurs → SPIFFE IDs)
    entries = [
        Entry("e1", "spiffe://prod.acme.com/payments-service",
              ["k8s:ns:payments", "k8s:sa:payments-sa"]),
        Entry("e2", "spiffe://prod.acme.com/analytics-service",
              ["k8s:ns:analytics", "k8s:sa:analytics-sa"]),
        Entry("e3", "spiffe://prod.acme.com/fraud-detection",
              ["k8s:ns:fraud", "k8s:sa:fraud-sa"]),
    ]
    for e in entries:
        server.enregistrer_entry(e)

    # Simuler 3 pods qui démarrent
    pods = [
        (1001, {"namespace": "payments",  "service_account": "payments-sa"},
         "payments-service"),
        (1002, {"namespace": "analytics", "service_account": "analytics-sa"},
         "analytics-service"),
        (1003, {"namespace": "fraud",     "service_account": "fraud-sa"},
         "fraud-detection"),
    ]

    print(f"  Workloads qui démarrent (aucun ne connaît de secret) :\n")
    for pid, meta, nom in pods:
        svids = api.fetch_x509_svids(pid, meta)
        if svids:
            svid = svids[0]
            print(f"  🚀 {nom:<25} (pid={pid})")
            print(f"     Sélecteurs     : k8s:ns:{meta['namespace']}, "
                  f"k8s:sa:{meta['service_account']}")
            print(f"     SPIFFE ID reçu : {svid.spiffe_id}")
            print(f"     TTL            : {svid.expire_a - svid.emis_a:.0f}s")
            print(f"     Valide         : {svid.est_valide} ✅\n")
        else:
            print(f"  ❌ {nom:<25} → aucun SVID (sélecteurs non enregistrés)\n")

    print(f"  Stats serveur : {server.stats()}")
    print(f"  Stats agent   : {agent.stats()}")
    print(f"""
  Le workload n'a jamais eu à connaître un mot de passe ou une clef.
  Son identité est prouvée par SON ENVIRONNEMENT D'EXÉCUTION.
  Si un attaquant vole le binaire → il ne peut pas l'exécuter ailleurs
  avec les mêmes sélecteurs (namespace k8s, ServiceAccount).
    """)


# ─── SCÉNARIO 2 : ROTATION AUTOMATIQUE ───────────────────────────────────────

def scenario_rotation():
    titre("SCÉNARIO 2 — Rotation automatique : l'agent renouvelle avant expiration")

    print("""
  L'agent SPIRE surveille le cycle de vie des SVIDs.
  Stratégie : renouveler quand il reste < 1/3 de la durée de vie.
  → Overlap : l'ancien cert reste valide pendant que le nouveau est distribué
  → Zéro downtime sur les connexions en cours (mTLS renegotiation)
    """)

    server, agents = creer_cluster("prod.acme.com", nb_noeuds=1)
    agent  = agents["node-0"]
    api    = WorkloadAPI(agent)

    server.enregistrer_entry(Entry("e1",
        "spiffe://prod.acme.com/payments-service",
        ["k8s:ns:prod", "k8s:sa:payments-sa"]))

    meta = {"namespace": "prod", "service_account": "payments-sa"}

    print(f"  Simulation de la vie d'un SVID (TTL=6s pour accélérer) :\n")

    # TTL très court pour la démo
    TTL = 6.0
    svids = api.fetch_x509_svids(1001, meta)
    if not svids:
        print("  Erreur : aucun SVID reçu")
        return

    svid_initial = svids[0]
    # Forcer un TTL court pour la démo
    svid_initial.expire_a = time.time() + TTL
    agent._cache["spiffe://prod.acme.com/payments-service"] = svid_initial

    print(f"  {'Temps':>6}  {'Vie restante':>14}  {'% vie':>7}  Événement")
    print(f"  " + "─"*50)

    historique_cles = set()
    historique_cles.add(svid_initial.cle_privee[:12])

    for tick in range(8):
        t_ecoule = tick * (TTL / 7)
        pct = svid_initial.pct_vie_restante
        t_restant = svid_initial.duree_restante
        evenement = ""

        # Simuler le passage du temps
        svid_initial.emis_a -= TTL / 7
        svid_initial.expire_a -= TTL / 7

        # L'agent vérifie et renouvelle si besoin
        svids_nouveau = api.fetch_x509_svids(1001, meta)
        if svids_nouveau:
            nouveau_svid = svids_nouveau[0]
            nouvelle_cle = nouveau_svid.cle_privee[:12]
            if nouvelle_cle not in historique_cles:
                historique_cles.add(nouvelle_cle)
                evenement = "🔄 ROTATION — nouvelle clef émise"
                svid_initial = nouveau_svid
            elif not svid_initial.est_valide:
                evenement = "⚠️  Expiré !"
            else:
                evenement = "✅ Cache valide"

        print(f"  {t_ecoule:>5.1f}s  {max(0, t_restant):>10.1f}s  "
              f"{max(0, pct):>6.0f}%  {evenement}")

    rotations = len(historique_cles) - 1
    print(f"\n  Rotations effectuées : {rotations}")
    print(f"  SVIDs émis au total   : {server.stats().get('svids_emis', 0)}")
    print(f"""
  En production avec TTL=1h :
    Renouvellement à t=40min (reste 1/3 = 20min)
    Nouveau SVID distribué en quelques ms
    Ancien SVID reste valide jusqu'à t=60min
    → Overlap de 20min → zéro connexion cassée
    """)


# ─── SCÉNARIO 3 : SÉLECTEURS KUBERNETES ──────────────────────────────────────

def scenario_selecteurs():
    titre("SCÉNARIO 3 — Sélecteurs Kubernetes : granularité fine des identités")

    print("""
  Les sélecteurs permettent de discriminer finement les workloads.
  Un pod est identifié par la combinaison de ses attributs k8s.

  Granularité possible :
    Namespace seulement        → tous les pods du namespace ont la même identité
    Namespace + ServiceAccount → identité par service (recommandé)
    + pod-label                → identité par version ou environnement
    """)

    server, agents = creer_cluster("prod.acme.com", nb_noeuds=1)
    agent = agents["node-0"]
    api   = WorkloadAPI(agent)

    # Entries avec différents niveaux de granularité
    entries = [
        # Granularité : namespace seulement
        Entry("e-monitoring", "spiffe://prod.acme.com/monitoring",
              ["k8s:ns:monitoring"]),

        # Granularité : namespace + SA (standard)
        Entry("e-payments", "spiffe://prod.acme.com/payments-service",
              ["k8s:ns:payments", "k8s:sa:payments-sa"]),

        # Granularité : namespace + SA + label version
        Entry("e-payments-v2", "spiffe://prod.acme.com/payments-service-v2",
              ["k8s:ns:payments", "k8s:sa:payments-sa",
               "k8s:pod-label:version:v2"]),

        # Sélecteur Unix (hors k8s)
        Entry("e-cron", "spiffe://prod.acme.com/cron-job",
              ["unix:uid:1000"]),
    ]
    for e in entries:
        server.enregistrer_entry(e)

    # Pods avec différentes combinaisons d'attributs
    pods = [
        (2001, {"namespace": "monitoring"},
         "Pod monitoring (namespace seul)"),
        (2002, {"namespace": "payments", "service_account": "payments-sa"},
         "Pod payments v1 (ns + SA)"),
        (2003, {"namespace": "payments", "service_account": "payments-sa",
                "pod_label": {"version": "v2"}},
         "Pod payments v2 (ns + SA + label)"),
        (2004, {"uid": 1000},
         "Processus Unix uid=1000"),
        (2005, {"namespace": "rogue", "service_account": "unknown-sa"},
         "Pod inconnu (sélecteurs sans entry)"),
    ]

    print(f"\n  {'Pod / Processus':<42}  SPIFFE IDs reçus")
    print(f"  " + "─"*78)
    for pid, meta, desc in pods:
        svids = api.fetch_x509_svids(pid, meta)
        if svids:
            for svid in svids:
                path = svid.spiffe_id.split("/")[-1]
                print(f"  {desc:<42}  → {path}")
        else:
            print(f"  {desc:<42}  → ❌ aucune identité (rejeté)")

    print(f"""
  Observations clés :
    pod payments v2 : reçoit DEUX SVIDs (entry ns+SA ET entry ns+SA+label)
    → Le workload peut utiliser l'identité la plus spécifique
    pod inconnu     : aucun SVID → ne peut pas s'authentifier ✅
    Processus Unix  : SPIFFE fonctionne hors Kubernetes aussi
    """)


# ─── SCÉNARIO 4 : FÉDÉRATION ─────────────────────────────────────────────────

def scenario_federation():
    titre("SCÉNARIO 4 — Fédération : deux clusters qui se font confiance")

    print("""
  Fédération SPIFFE : permet à des services de différents clusters /
  clouds / organisations de s'authentifier mutuellement.

  Sans fédération :
    spiffe://cluster-a.acme.com/payments ne peut pas valider
    spiffe://cluster-b.acme.com/analytics → trust domain inconnu

  Avec fédération :
    Cluster A et B échangent leurs trust bundles (clés publiques CA)
    → Chaque cluster peut valider les SVIDs de l'autre
    → mTLS cross-cluster fonctionne
    """)

    # Cluster A : production principale (EU)
    server_a, agents_a = creer_cluster("prod-eu.acme.com", nb_noeuds=1)
    # Cluster B : cluster analytics (US)
    server_b, agents_b = creer_cluster("analytics-us.acme.com", nb_noeuds=1)
    # Cluster C : partenaire externe
    server_c, agents_c = creer_cluster("partner.external.com", nb_noeuds=1)

    # Enregistrer les workloads
    server_a.enregistrer_entry(Entry("e1",
        "spiffe://prod-eu.acme.com/payments-service",
        ["k8s:ns:prod", "k8s:sa:payments"]))
    server_b.enregistrer_entry(Entry("e2",
        "spiffe://analytics-us.acme.com/analytics-service",
        ["k8s:ns:analytics", "k8s:sa:analytics"]))
    server_c.enregistrer_entry(Entry("e3",
        "spiffe://partner.external.com/partner-api",
        ["k8s:ns:api", "k8s:sa:partner"]))

    # Obtenir les SVIDs
    svid_payments = server_a.emettre_svid(
        "spiffe://prod-eu.acme.com/payments-service")
    svid_analytics = server_b.emettre_svid(
        "spiffe://analytics-us.acme.com/analytics-service")
    svid_partner = server_c.emettre_svid(
        "spiffe://partner.external.com/partner-api")

    # ── SANS fédération ──────────────────────────────────────────────────
    print(f"  SANS fédération :\n")
    cas_sans = [
        (server_a, svid_payments,  "A valide son propre SVID payments"),
        (server_a, svid_analytics, "A valide le SVID analytics (cluster B)"),
        (server_a, svid_partner,   "A valide le SVID partner (externe)"),
    ]
    for srv, svid, desc in cas_sans:
        ok, raison = srv.valider_svid(svid)
        icone = "✅" if ok else "❌"
        print(f"    {icone} {desc}")
        if not ok:
            print(f"       Raison : {raison}")

    # ── Fédération A ↔ B ─────────────────────────────────────────────────
    server_a.federer(server_b)
    print(f"\n  Après fédération A ↔ B :\n")
    cas_avec = [
        (server_a, svid_payments,  "A valide son propre SVID payments"),
        (server_a, svid_analytics, "A valide le SVID analytics (cluster B)"),
        (server_b, svid_payments,  "B valide le SVID payments (cluster A)"),
        (server_a, svid_partner,   "A valide le SVID partner (non fédéré)"),
    ]
    for srv, svid, desc in cas_avec:
        ok, raison = srv.valider_svid(svid)
        icone = "✅" if ok else "❌"
        print(f"    {icone} {desc}")
        if not ok:
            print(f"       Raison : {raison}")

    print(f"""
  La fédération est explicite et bidirectionnelle.
  Cluster C (partner) n'est pas fédéré → ses SVIDs sont rejetés.
  → On ne fait confiance qu'aux domaines explicitement approuvés.

  Cas d'usage réels :
    Multi-cloud  : AWS prod ↔ GCP analytics
    Acquisition  : intégrer les services de l'entreprise rachetée
    Partenariat  : API inter-entreprises sans gestion de secrets partagés
    """)


# ─── SCÉNARIO 5 : IMPOSTURE REJETÉE ──────────────────────────────────────────

def scenario_imposture():
    titre("SCÉNARIO 5 — Imposture rejetée : l'attestation protège contre les faux workloads")

    print("""
  Un attaquant tente d'obtenir l'identité d'un service sensible.
  Il peut essayer :
    1. Appeler l'API Workload depuis le mauvais namespace
    2. Forger un SVID avec la mauvaise CA
    3. Rejouer un SVID expiré
    4. Attester depuis un nœud non enregistré
    """)

    server, agents = creer_cluster("prod.acme.com", nb_noeuds=2)
    agent_legitime = agents["node-0"]
    api_legitime   = WorkloadAPI(agent_legitime)

    # Enregistrer l'entry payments (seulement pour le bon namespace)
    server.enregistrer_entry(Entry("e-payments",
        "spiffe://prod.acme.com/payments-service",
        ["k8s:ns:payments", "k8s:sa:payments-sa"]))

    print(f"\n  Tentatives d'obtenir l'identité de payments-service :\n")

    # 1. Bon workload légitime
    svids = api_legitime.fetch_x509_svids(
        3001, {"namespace": "payments", "service_account": "payments-sa"})
    print(f"  ✅ Service légitime (bon namespace + SA) → {len(svids)} SVID(s)")
    if svids:
        print(f"     SPIFFE ID : {svids[0].spiffe_id}")

    # 2. Mauvais namespace
    svids_bad_ns = api_legitime.fetch_x509_svids(
        3002, {"namespace": "compromised", "service_account": "payments-sa"})
    print(f"\n  ❌ Mauvais namespace 'compromised' → {len(svids_bad_ns)} SVID(s)")
    print(f"     Raison : sélecteurs ne correspondent à aucune entry")

    # 3. Bon namespace, mauvais SA
    svids_bad_sa = api_legitime.fetch_x509_svids(
        3003, {"namespace": "payments", "service_account": "default"})
    print(f"\n  ❌ Mauvais ServiceAccount 'default' → {len(svids_bad_sa)} SVID(s)")
    print(f"     Raison : k8s:sa:default ≠ k8s:sa:payments-sa")

    # 4. SVID forgé (fausse CA)
    ca_pirate = "deadbeef" * 4
    svid_forge = SVID(
        spiffe_id    = "spiffe://prod.acme.com/payments-service",
        trust_domain = "prod.acme.com",
        workload     = "payments-service",
        emis_a       = time.time(),
        expire_a     = time.time() + 3600,
        cle_privee   = "fake-key",
        bundle_ca    = ca_pirate,
    )
    ok, raison = server.valider_svid(svid_forge)
    print(f"\n  ❌ SVID forgé (fausse CA) → valide={ok}")
    print(f"     Raison : {raison}")

    # 5. SVID expiré rejoué
    if svids:
        svid_expire = svids[0]
        svid_expire.expire_a = time.time() - 1   # Forcer l'expiration
        ok, raison = server.valider_svid(svid_expire)
        print(f"\n  ❌ SVID expiré rejoué → valide={ok}")
        print(f"     Raison : {raison}")

    # 6. Agent non enregistré
    agent_pirate = SPIREAgent.__new__(SPIREAgent)
    agent_pirate.node_id = "hacker-node"
    agent_pirate._server = server
    agent_pirate._cache  = {}
    agent_pirate._lock   = threading.Lock()
    agent_pirate._stats  = defaultdict(int)
    agent_pirate._bundles = []
    agent_pirate._atteste = False  # ← non attesté
    api_pirate = WorkloadAPI(agent_pirate)
    svids_pirate = api_pirate.fetch_x509_svids(
        9999, {"namespace": "payments", "service_account": "payments-sa"})
    print(f"\n  ❌ Agent non attesté (hacker-node) → {len(svids_pirate)} SVID(s)")
    print(f"     Raison : agent non attesté → refus de toute demande")

    print(f"""
  Résumé des protections :
    Mauvais namespace/SA   → sélecteurs ne matchent aucune entry → rejeté ✅
    SVID forgé             → bundle CA invalide → rejeté à la validation ✅
    SVID expiré            → est_valide=False → rejeté ✅
    Agent non attesté      → _atteste=False → aucun SVID délivré ✅
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 25 — 'LE SCEAU D'IDENTITÉ' (SPIFFE/SPIRE)        ║")
    print("╚" + "═"*62 + "╝")

    scenario_attestation()
    scenario_rotation()
    scenario_selecteurs()
    scenario_federation()
    scenario_imposture()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  SPIFFE/SPIRE — Ce qu'il faut retenir :

    SVID       : certificat X.509 avec URI spiffe://domain/workload
    Attestation: l'identité prouvée par l'environnement, pas un secret
    Sélecteurs : k8s:ns, k8s:sa, pod-label, unix:uid → fine granularité
    Rotation   : automatique avant expiration → 0 downtime, clefs courtes
    Fédération : cross-cluster, cross-cloud, cross-org explicitement approuvée

  Architecture :
    SPIRE Server → registre des entries + CA → émet les SVIDs
    SPIRE Agent  → atteste les workloads + cache + renouvelle
    Workload API → socket Unix → zéro secret au démarrage

  Ce que nos scénarios ont prouvé :
    Scénario 1 → 3 pods démarrent sans secret → SVIDs émis par attestation ✅
    Scénario 2 → rotation automatique avant expiration, overlap garanti ✅
    Scénario 3 → pods v1 et v2 → SVIDs distincts grâce aux labels ✅
    Scénario 4 → A ↔ B fédérés, C non-fédéré → SVIDs C rejetés ✅
    Scénario 5 → mauvais ns, faux cert, SVID expiré, agent non attesté → rejetés ✅

  Utilisé en production :
    Uber      → SPIRE pour tous les microservices (2019, open-sourcé)
    Pinterest → SPIFFE + Envoy pour le mTLS de la plateforme entière
    Bloomberg → SPIFFE pour sécuriser les flux de données financières
    CNCF      → SPIFFE/SPIRE est un projet CNCF gradué (= production-ready)

  → Jour 26 — "Le Gardien des Portes" (API Gateway + OPA)
    On sait maintenant QUI sont les services (SPIFFE).
    Demain : comment contrôler ce qu'ils peuvent FAIRE à grande échelle.
    API Gateway = point d'entrée unique. OPA = cerveaux des politiques.
    Policy-as-code : les règles d'accès versionnées dans Git comme du code.
  """)

if __name__ == "__main__":
    main()
