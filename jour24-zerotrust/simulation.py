"""
Jour 24 — "Confiance Zéro" — Simulation Zero-Trust
====================================================
5 scénarios :
  1. mTLS : handshake mutuel, certificat révoqué, certificat expiré
  2. Politique d'autorisation : allow/deny par service et par ressource
  3. Rotation automatique : renouvellement de certificat sans downtime
  4. Attaque latérale : un service compromis ne peut pas pivoter
  5. Audit log : chaque accès est traçable
"""

import time
from zerotrust import (
    AutoriteCertification, HandshakeMTLS, MoteurPolitique,
    RequeteAutorisation, AgentZeroTrust, Certificat
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


def creer_infrastructure():
    """Créer la CA et le moteur de politiques pour l'ensemble du cluster."""
    ca = AutoriteCertification("acme-ca", "acme.corp")

    moteur = MoteurPolitique()

    # ── Politique 1 : services internes → lecture des données utilisateurs ──
    def politique_data_users(req: RequeteAutorisation):
        services_autorises = {
            "spiffe://acme.corp/analytics-service",
            "spiffe://acme.corp/billing-service",
            "spiffe://acme.corp/recommendations-service",
        }
        if (req.ressource.startswith("hdfs:///data/users") and
                req.action == "GET"):
            if req.sujet in services_autorises:
                return True, f"Service autorisé à lire les données utilisateurs"
            return False, f"{req.sujet} n'est pas autorisé à lire /data/users"
        return None  # Pas concerné par cette règle

    # ── Politique 2 : seul payments-service peut écrire dans payments-topic ──
    def politique_kafka_payments(req: RequeteAutorisation):
        if req.ressource == "kafka://payments":
            if req.action == "PRODUCE":
                if req.sujet == "spiffe://acme.corp/payments-service":
                    return True, "payments-service autorisé à produire"
                return False, f"Seul payments-service peut produire dans payments"
            if req.action == "CONSUME":
                autorise = {
                    "spiffe://acme.corp/fraud-detection-service",
                    "spiffe://acme.corp/analytics-service",
                }
                if req.sujet in autorise:
                    return True, "Service autorisé à consommer payments"
                return False, f"{req.sujet} non autorisé à consommer payments"
        return None

    # ── Politique 3 : admin uniquement pour les opérations destructives ──
    def politique_admin(req: RequeteAutorisation):
        if req.action in ("DELETE", "TRUNCATE", "DROP"):
            if req.sujet == "spiffe://acme.corp/admin-service":
                if req.context.get("mfa_validee"):
                    return True, "Admin avec MFA autorisé"
                return False, "Admin : MFA requis pour opérations destructives"
            return False, "Seul admin-service peut effectuer des opérations destructives"
        return None

    # ── Politique 4 : health checks inter-services ──
    def politique_health(req: RequeteAutorisation):
        if req.ressource.endswith("/health") and req.action == "GET":
            return True, "Health check toujours autorisé"
        return None

    moteur.ajouter_politique("data-users-acl",    politique_data_users)
    moteur.ajouter_politique("kafka-payments-acl", politique_kafka_payments)
    moteur.ajouter_politique("admin-destructive",  politique_admin)
    moteur.ajouter_politique("health-check",       politique_health)
    # Default-deny implicite

    return ca, moteur


# ─── SCÉNARIO 1 : mTLS HANDSHAKE ─────────────────────────────────────────────

def scenario_mtls():
    titre("SCÉNARIO 1 — mTLS : authentification mutuelle des services")

    print("""
  TLS normal   : seul le CLIENT vérifie l'identité du serveur.
  mTLS (mutual) : SERVEUR et CLIENT vérifient mutuellement leurs certificats.
  → Chaque service prouve son identité cryptographiquement.
  → Un attaquant sans certificat CA valide ne peut pas se connecter.
    """)

    ca, _ = creer_infrastructure()
    mtls  = HandshakeMTLS(ca)

    # Cas 1 : connexion normale entre deux services légitimes
    cert_payments  = ca.emettre("payments-service",  duree_s=3600)
    cert_analytics = ca.emettre("analytics-service", duree_s=3600)
    cert_fraud     = ca.emettre("fraud-detection-service", duree_s=3600)

    # Cas 2 : certificat révoqué (service compromis)
    cert_compromis = ca.emettre("compromised-service", duree_s=3600)
    ca.revoquer(cert_compromis.spiffe_uri)

    # Cas 3 : certificat expiré (rotation manquée)
    cert_expire = ca.emettre("old-service", duree_s=-1)  # déjà expiré

    # Cas 4 : certificat d'une CA inconnue (attaquant externe)
    ca_pirate = AutoriteCertification("hacker-ca", "evil.corp")
    cert_pirate = ca_pirate.emettre("fake-payments", duree_s=86400)

    cas = [
        ("payments → analytics (légitimes)",   cert_payments,   cert_analytics),
        ("analytics → fraud (légitimes)",       cert_analytics,  cert_fraud),
        ("compromis → payments (révoqué)",      cert_compromis,  cert_payments),
        ("old-service → analytics (expiré)",    cert_expire,     cert_analytics),
        ("fake-payments → analytics (CA ext.)", cert_pirate,     cert_analytics),
    ]

    print(f"  {'Connexion':<40}  {'Résultat':<8}  {'Raison'}")
    print(f"  " + "─"*80)
    for label, c_client, c_server in cas:
        r = mtls.connecter(c_client, c_server)
        icone = "✅" if r.succes else "🔴"
        print(f"  {label:<40}  {icone}        {r.raison}")

    print(f"""
  mTLS garantit :
    → Pas de connexion possible sans certificat CA valide
    → Révocation immédiate (CRL/OCSP) coupe le service compromis
    → Expiration automatique = rotation forcée toutes les 24h en prod
    """)


# ─── SCÉNARIO 2 : POLITIQUES D'AUTORISATION ──────────────────────────────────

def scenario_politiques():
    titre("SCÉNARIO 2 — Politiques d'autorisation : qui peut faire quoi")

    print("""
  Authentification ≠ Autorisation.
  mTLS prouve QUOI tu es.
  La politique décide CE QUE tu peux faire.

  Moindre privilège : chaque service n'a accès qu'à ce dont il a besoin.
    """)

    ca, moteur = creer_infrastructure()

    # Créer les agents
    agents = {s: AgentZeroTrust(s, ca, moteur)
              for s in ["payments-service", "analytics-service",
                        "fraud-detection-service", "billing-service",
                        "recommendations-service", "rogue-service",
                        "admin-service"]}

    requetes = [
        # (appelant, cible_service, action, ressource, context, description)
        ("analytics-service",         "payments-service",
         "GET",     "hdfs:///data/users/profiles",  {},
         "analytics lit les profils utilisateurs"),

        ("rogue-service",             "payments-service",
         "GET",     "hdfs:///data/users/profiles",  {},
         "service non autorisé tente de lire /data/users"),

        ("payments-service",          "fraud-detection-service",
         "PRODUCE", "kafka://payments",              {},
         "payments produit dans le topic payments"),

        ("rogue-service",             "fraud-detection-service",
         "PRODUCE", "kafka://payments",              {},
         "service pirate tente de produire dans payments"),

        ("fraud-detection-service",   "analytics-service",
         "CONSUME", "kafka://payments",              {},
         "fraud consomme depuis le topic payments"),

        ("admin-service",             "payments-service",
         "DELETE",  "hdfs:///data/users/old",
         {"mfa_validee": True},
         "admin supprime avec MFA"),

        ("admin-service",             "payments-service",
         "DELETE",  "hdfs:///data/users/old",
         {"mfa_validee": False},
         "admin supprime SANS MFA"),

        ("payments-service",          "analytics-service",
         "GET",     "analytics-service/health",      {},
         "health check inter-services"),
    ]

    print(f"  {'Requête':<50}  {'Décision':>8}  Règle")
    print(f"  " + "─"*82)
    for appelant, cible_nom, action, ressource, ctx, desc in requetes:
        r = agents[appelant].appeler(agents[cible_nom], action, ressource, ctx)
        icone = "✅ ALLOW" if r["ok"] else "🔴 DENY "
        raison = r.get("politique", r.get("raison", ""))[:30]
        print(f"  {desc:<50}  {icone}   {raison}")

    s = moteur.stats()
    print(f"\n  Résumé des décisions : {s}")


# ─── SCÉNARIO 3 : ROTATION AUTOMATIQUE ───────────────────────────────────────

def scenario_rotation():
    titre("SCÉNARIO 3 — Rotation automatique : certificats courts = moindre risque")

    print("""
  Pourquoi des certificats courts (24h vs 1 an) ?
    → Clef compromise : validité max 24h avant expiration naturelle
    → Pas de révocation manuelle nécessaire dans 95% des cas
    → Rotation forcée = preuve régulière que le système fonctionne

  Le sidecar SPIRE renouvelle le certificat automatiquement à 2/3 de sa durée.
  Ex : cert 24h → renouvellement à t=16h → overlap de 8h pour 0 downtime.
    """)

    ca, moteur = creer_infrastructure()
    agent      = AgentZeroTrust("payments-service", ca, moteur)

    print(f"  Certificat initial :")
    c = agent._cert
    print(f"    Sujet      : {c.sujet}")
    print(f"    SPIFFE URI : {c.spiffe_uri}")
    print(f"    Expire à   : t+{c.expire_a - c.valide_depuis:.0f}s")
    print(f"    Valide     : {c.est_valide} ✅")

    # Simuler une rotation
    print(f"\n  Rotation du certificat (renouvellement avant expiration) :")
    ancien, nouveau = agent.renouveler_cert(duree_s=3600)
    print(f"    Ancien cert : {ancien.empreinte[:16]}...  "
          f"{'valide' if ancien.est_valide else 'expiré'}")
    print(f"    Nouveau cert: {nouveau.empreinte[:16]}...  "
          f"{'valide' if nouveau.est_valide else 'expiré'}")
    print(f"    Overlap     : les deux coexistent → 0 downtime ✅")

    # Simuler des rotations multiples avec statistiques
    print(f"\n  Simulation de 10 rotations successives :")
    for i in range(10):
        _, c = agent.renouveler_cert(duree_s=3600)

    stats_ca = ca.stats()
    print(f"    Certificats émis par la CA : {stats_ca.get('emis', 0)}")
    print(f"    Empreinte actuelle         : {agent._cert.empreinte[:20]}...")

    # Impact sur la sécurité
    print(f"""
  Comparaison durée de certificat :
    {'Durée':>8}  {'Fenêtre d\'exposition si compromis':>35}  Opérationnel
    {'─'*60}
    {'1 an':>8}  {'365 jours (!)':>35}  Simple
    {'90 jours':>8}  {'90 jours':>35}  Let's Encrypt standard
    {'24h':>8}  {'24 heures':>35}  SPIFFE/SPIRE recommandé
    {'1h':>8}  {'1 heure':>35}  Haute sécurité
    """)


# ─── SCÉNARIO 4 : ATTAQUE LATÉRALE ───────────────────────────────────────────

def scenario_lateral_movement():
    titre("SCÉNARIO 4 — Attaque latérale : Zero-Trust bloque le pivot réseau")

    print("""
  Scénario d'attaque :
    L'attaquant compromet le service "recommendations-service" (faible valeur).
    Dans un réseau traditionnel : il peut maintenant accéder à TOUT le réseau interne.
    Avec Zero-Trust : son identité est limitée aux droits de recommendations-service.
    → Le pivot latéral est bloqué par la politique de moindre privilège.
    """)

    ca, moteur = creer_infrastructure()

    # "Compromis" = l'attaquant a le cert de recommendations-service
    # mais ne peut pas en obtenir d'autres (il n'est pas la CA)
    cert_compromis = ca.emettre("recommendations-service", duree_s=3600)

    # Créer les services cibles
    cert_payments  = ca.emettre("payments-service",  duree_s=3600)
    cert_admin     = ca.emettre("admin-service",      duree_s=3600)
    cert_analytics = ca.emettre("analytics-service",  duree_s=3600)

    mtls = HandshakeMTLS(ca)

    print(f"  L'attaquant contrôle : recommendations-service\n")
    print(f"  Tentatives de pivot latéral :\n")

    tentatives = [
        (cert_payments,  "GET",    "hdfs:///data/payments",
         "Accéder aux données de paiement"),
        (cert_payments,  "PRODUCE","kafka://payments",
         "Injecter des faux paiements"),
        (cert_admin,     "DELETE", "hdfs:///data/users",
         "Supprimer les données utilisateurs"),
        (cert_analytics, "GET",    "hdfs:///data/users/profiles",
         "Exfiltrer les profils"),
        (cert_analytics, "GET",    "analytics-service/health",
         "Health check (autorisé !)"),
    ]

    for cert_cible, action, ressource, desc in tentatives:
        # mTLS : l'attaquant SE connecte avec le cert compromis
        h = mtls.connecter(cert_compromis, cert_cible)
        if not h.succes:
            print(f"  🛡️  {desc:<45}  BLOQUÉ en mTLS")
            continue

        # Autorisation
        req = RequeteAutorisation(
            sujet     = cert_compromis.spiffe_uri,
            action    = action,
            ressource = ressource,
        )
        dec = moteur.evaluer(req)
        icone = "✅ AUTORISÉ" if dec.autorise else "🛡️  BLOQUÉ "
        print(f"  {icone}  {desc:<45}  ({dec.raison[:40]})")

    print(f"""
  L'attaquant peut seulement faire ce que recommendations-service peut faire.
  Il ne peut pas accéder aux paiements, ni aux données utilisateurs complètes.
  Chaque tentative est LOGGÉE → détection d'intrusion possible.

  Dans un réseau traditionnel (périmètre) :
    Compromis recommendations-service → accès complet au LAN interne
    → peut scanner les autres services, accéder sans authentification

  Avec Zero-Trust :
    Compromis recommendations-service → droits de recommendations-service SEULEMENT
    → chaque tentative d'accès hors-périmètre est refusée ET loggée
    """)


# ─── SCÉNARIO 5 : AUDIT LOG ──────────────────────────────────────────────────

def scenario_audit():
    titre("SCÉNARIO 5 — Audit Log : chaque accès est tracé et analysable")

    print("""
  Zero-Trust = vérifier à CHAQUE requête → chaque décision est loggée.
  L'audit log permet :
    - Détecter les comportements anormaux (SIEM / UEBA)
    - Répondre à un incident : "qui a accédé à quoi, quand ?"
    - Compliance (RGPD, SOC2, PCI-DSS)
    """)

    ca, moteur = creer_infrastructure()
    agents = {s: AgentZeroTrust(s, ca, moteur)
              for s in ["payments-service", "analytics-service",
                        "fraud-detection-service", "rogue-service",
                        "admin-service"]}

    # Générer du trafic varié
    import random
    random.seed(42)
    appels = [
        ("analytics-service", "payments-service",
         "GET", "hdfs:///data/users/profiles", {}),
        ("payments-service", "fraud-detection-service",
         "PRODUCE", "kafka://payments", {}),
        ("fraud-detection-service", "analytics-service",
         "CONSUME", "kafka://payments", {}),
        ("rogue-service", "payments-service",
         "GET", "hdfs:///data/users/profiles", {}),
        ("rogue-service", "payments-service",
         "PRODUCE", "kafka://payments", {}),
        ("admin-service", "payments-service",
         "DELETE", "hdfs:///data/users/old", {"mfa_validee": True}),
        ("analytics-service", "fraud-detection-service",
         "GET", "fraud-detection-service/health", {}),
        ("rogue-service", "analytics-service",
         "DELETE", "hdfs:///data/users/all", {}),
    ]

    for appelant, cible, action, ressource, ctx in appels:
        agents[appelant].appeler(agents[cible], action, ressource, ctx)

    # Afficher l'audit log
    logs = moteur.derniers_logs(20)
    print(f"  Audit log ({len(logs)} entrées) :\n")
    print(f"  {'Sujet':<40}  {'Action':<8}  {'Ressource':<35}  Décision")
    print(f"  " + "─"*100)
    for log in logs:
        sujet_court = log["sujet"].replace("spiffe://acme.corp/", "")
        ressource_courte = log["ressource"][:33]
        icone = "✅" if log["decision"] == "autorise" else "🔴"
        print(f"  {sujet_court:<40}  {log['action']:<8}  "
              f"{ressource_courte:<35}  {icone} {log['decision']}")

    s = moteur.stats()
    print(f"\n  Statistiques globales :")
    print(f"    Requêtes autorisées : {s.get('autorise', 0)}")
    print(f"    Requêtes refusées   : {s.get('refuse', 0)}")

    # Détecter les anomalies : services avec beaucoup de refus
    refus_par_sujet = {}
    for log in logs:
        if log["decision"] == "refuse":
            s_court = log["sujet"].replace("spiffe://acme.corp/", "")
            refus_par_sujet[s_court] = refus_par_sujet.get(s_court, 0) + 1

    if refus_par_sujet:
        print(f"\n  ⚠️  Services avec refus multiples (potentielle intrusion) :")
        for svc, nb in sorted(refus_par_sujet.items(), key=lambda x: -x[1]):
            print(f"    {svc:<35} : {nb} refus 🚨")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 24 — 'CONFIANCE ZÉRO' (ZERO-TRUST)               ║")
    print("╚" + "═"*62 + "╝")

    scenario_mtls()
    scenario_politiques()
    scenario_rotation()
    scenario_lateral_movement()
    scenario_audit()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Zero-Trust — Ce qu'il faut retenir :

    "Never trust, always verify" — même à l'intérieur du réseau
    Identité  : SPIFFE SVID = certificat X.509 + URI spiffe://domain/service
    mTLS      : client ET serveur s'authentifient mutuellement
    Politique : chaque requête est évaluée (OPA / Rego) → moindre privilège
    Rotation  : certificats courts (24h) → exposition minimale si compromis
    Audit     : chaque accès loggé → détection d'intrusion + compliance

  Ce que nos scénarios ont prouvé :
    Scénario 1 → cert révoqué/expiré/CA-inconnue → connexion refusée ✅
    Scénario 2 → rogue-service bloqué sur /data/users et kafka://payments ✅
    Scénario 3 → rotation 10× sans downtime, empreintes différentes ✅
    Scénario 4 → service compromis ne peut pas pivoter vers les paiements ✅
    Scénario 5 → audit log détecte rogue-service : 3 refus consécutifs ✅

  Utilisé en production :
    Google    → BeyondCorp (2014) — inventeurs du Zero-Trust moderne
    Netflix   → Zero-Trust pour microservices (SPIFFE + Envoy)
    Uber      → mTLS sur tout le trafic inter-services avec SPIRE
    Cloudflare → Cloudflare Access = Zero-Trust pour les accès employés

  → Jour 25 — "Le Sceau d'Identité" (SPIFFE / SPIRE)
    Aujourd'hui : les concepts Zero-Trust et mTLS.
    Demain : le framework qui les implémente en production.
    SPIRE = serveur qui émet les SVIDs, surveille les workloads,
    et gère la rotation automatique à l'échelle du cluster.
  """)

if __name__ == "__main__":
    main()
