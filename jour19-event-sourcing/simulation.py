"""
Jour 19 — Simulation : Event Sourcing en action
=================================================
5 scénarios :
  1. Flux d'événements basique : compte bancaire, audit trail complet
  2. Time-travel : reconstituer l'état à un instant T passé
  3. Snapshots : mesurer le gain de performance à grande échelle
  4. Concurrence optimiste : deux clients qui modifient le même aggregate
  5. Projections CQRS : même événements, vues différentes
"""

import time
import threading
import random
from collections import defaultdict
from event_sourcing import (
    EventStore, Compte, ProjectionSoldes, Snapshot,
    CompteOuvert, CompteDebite, CompteCredite,
    CompteSuspendu, CompteReactive, ConcurrencyException
)

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s): time.sleep(s)

def afficher_stream(store: EventStore, account_id: str, label: str = ""):
    if label:
        print(f"\n  {label}")
    events = store.lire_stream(account_id)
    if not events:
        print("    (stream vide)")
        return
    for e in events:
        ts_fmt = time.strftime("%H:%M:%S", time.localtime(e.ts))
        payload = e._payload()
        payload_str = "  ".join(f"{k}={v}" for k, v in payload.items()
                                if k not in ("proprietaire",))
        print(f"    v{e.version:<2}  [{ts_fmt}]  {e.type_event:<20}  {payload_str}")


# ─── SCÉNARIO 1 : FLUX D'ÉVÉNEMENTS ET AUDIT TRAIL ───────────────────────────

def scenario_audit_trail():
    titre("SCÉNARIO 1 — Flux d'événements : compte bancaire et audit trail")

    print("""
  Avec CRUD traditionnel :
    UPDATE accounts SET balance = 1350 WHERE id = 'acc-42'
    → On sait que le solde est 1350€. On ne sait PAS comment on y est arrivé.

  Avec Event Sourcing :
    Chaque opération produit un événement immuable.
    L'audit trail EST le stockage primaire, pas un à-côté.
    """)

    store = EventStore()

    # Ouvrir le compte
    compte = Compte("acc-alice", store)
    compte.ouvrir("Alice Dupont", solde_initial=1000.0)

    # Séquence d'opérations
    operations = [
        ("crediter",  500.0,  "Salaire mars",    "employeur-SA"),
        ("debiter",   120.0,  "Loyer",            "propriétaire"),
        ("debiter",    45.50, "Courses",          "supermarché"),
        ("crediter",   80.0,  "Remboursement",   "ami-bob"),
        ("debiter",   200.0,  "Électricité",     "EDF"),
        ("debiter",    15.99, "Netflix",          "netflix"),
        ("crediter",  150.0,  "Freelance",       "client-X"),
    ]

    for op, montant, motif, tiers in operations:
        getattr(compte, op)(montant, motif=motif,
                            **{"destinataire" if op == "debiter" else "source": tiers})

    print(f"  Solde actuel : {compte.solde:.2f}€")
    afficher_stream(store, "acc-alice", "Stream d'événements (audit trail complet) :")

    # Démontrer l'avantage : "qui a fait quoi et quand ?"
    print(f"\n  Questions auxquelles on peut répondre depuis le stream :")
    events = store.lire_stream("acc-alice")
    debits = [e for e in events if e.type_event == "CompteDebite"]
    total_debite = sum(e.montant for e in debits)
    plus_gros_debit = max(debits, key=lambda e: e.montant)
    print(f"    Total débité  : {total_debite:.2f}€")
    print(f"    Nb débits     : {len(debits)}")
    print(f"    Plus gros     : {plus_gros_debit.montant:.2f}€ ({plus_gros_debit.motif})")
    print(f"    → Impossible à obtenir depuis un simple UPDATE balance ✅")


# ─── SCÉNARIO 2 : TIME-TRAVEL ─────────────────────────────────────────────────

def scenario_time_travel():
    titre("SCÉNARIO 2 — Time-travel : reconstituer l'état à n'importe quel instant T")

    print("""
  Cas d'usage réel : "Le 15 du mois, quel était le solde du client ?"
  Avec CRUD : impossible (l'état a été écrasé).
  Avec Event Sourcing : rejouer jusqu'à la version correspondant à cette date.
    """)

    store   = EventStore()
    compte  = Compte("acc-bob", store)
    compte.ouvrir("Bob Martin", solde_initial=500.0)

    # Séquence avec timestamps enregistrés à chaque version
    checkpoints = []  # (version, solde, description)

    compte.crediter(1000.0, "Salaire",   "employeur")
    checkpoints.append((store.nb_evenements("acc-bob"), compte.solde, "après salaire"))

    compte.debiter(450.0, "Loyer", "propriétaire")
    checkpoints.append((store.nb_evenements("acc-bob"), compte.solde, "après loyer"))

    compte.debiter(80.0, "Assurance", "assureur")
    checkpoints.append((store.nb_evenements("acc-bob"), compte.solde, "après assurance"))

    compte.crediter(200.0, "Bonus",  "employeur")
    checkpoints.append((store.nb_evenements("acc-bob"), compte.solde, "après bonus"))

    compte.debiter(300.0, "Vacances", "agence")
    checkpoints.append((store.nb_evenements("acc-bob"), compte.solde, "après vacances"))

    print(f"  Solde actuel (v{compte.version}) : {compte.solde:.2f}€\n")
    afficher_stream(store, "acc-bob", "Stream complet :")

    print(f"\n  Time-travel : reconstitution à chaque étape\n")
    print(f"  {'Version':>8}  {'Solde':>10}  Description")
    print("  " + "─"*40)

    for nb_events, solde_attendu, description in checkpoints:
        # Reconstruire l'état en ne lisant que les N premiers événements
        events_partiel = store.lire_stream("acc-bob", jusqu_a_version=nb_events)

        # Rejouer manuellement
        solde_reconstitue = 0.0
        for e in events_partiel:
            if e.type_event == "CompteOuvert":
                solde_reconstitue = e.solde_initial
            elif e.type_event == "CompteCredite":
                solde_reconstitue += e.montant
            elif e.type_event == "CompteDebite":
                solde_reconstitue -= e.montant

        match = "✅" if abs(solde_reconstitue - solde_attendu) < 0.01 else "❌"
        print(f"  v{nb_events-1:>7}  {solde_reconstitue:>8.2f}€  {description} {match}")

    print(f"""
  En production (EventStoreDB) :
    GET /streams/acc-bob?maxCount=3  → renvoie les 3 premiers events
    → Solde reconstitué à cet instant précis ✅
    """)


# ─── SCÉNARIO 3 : SNAPSHOTS ───────────────────────────────────────────────────

def scenario_snapshots():
    titre("SCÉNARIO 3 — Snapshots : éviter de rejouer 10 000 événements")

    print("""
  Problème : un compte très actif peut avoir 100 000 événements.
  Rejouer tout à chaque lecture → O(N) inacceptable.

  Solution : Snapshot périodique
    Tous les N événements, sauvegarder l'état courant.
    À la reconstruction : charger le snapshot + rejouer seulement les events suivants.
    Coût : O(1) pour le snapshot + O(events depuis snapshot) << O(total events)
    """)

    # Mesurer le coût SANS snapshot
    store_sans = EventStore()
    c_sans = Compte("acc-perf-sans", store_sans)
    c_sans.SEUIL_SNAPSHOT = 99999   # Désactiver les snapshots
    c_sans.ouvrir("Test User", solde_initial=1000.0)

    N_EVENTS = 100
    for i in range(N_EVENTS):
        if i % 2 == 0:
            c_sans.crediter(10.0, f"crédit-{i}")
        else:
            c_sans.debiter(5.0, f"débit-{i}")

    t0 = time.perf_counter()
    for _ in range(20):
        c_reconstitue = Compte.__new__(Compte)
        c_reconstitue.id      = "acc-perf-sans"
        c_reconstitue._store  = store_sans
        c_reconstitue.solde   = 0.0
        c_reconstitue.proprietaire = ""
        c_reconstitue.suspendu = False
        c_reconstitue.version  = 0
        c_reconstitue._recharger()
    t_sans = (time.perf_counter() - t0) / 20 * 1000

    # Mesurer le coût AVEC snapshot
    store_avec = EventStore()
    c_avec = Compte("acc-perf-avec", store_avec)
    c_avec.SEUIL_SNAPSHOT = 10   # Snapshot tous les 10 events
    c_avec.ouvrir("Test User", solde_initial=1000.0)

    for i in range(N_EVENTS):
        if i % 2 == 0:
            c_avec.crediter(10.0, f"crédit-{i}")
        else:
            c_avec.debiter(5.0, f"débit-{i}")

    t0 = time.perf_counter()
    for _ in range(20):
        c_reconstitue2 = Compte.__new__(Compte)
        c_reconstitue2.id      = "acc-perf-avec"
        c_reconstitue2._store  = store_avec
        c_reconstitue2.solde   = 0.0
        c_reconstitue2.proprietaire = ""
        c_reconstitue2.suspendu = False
        c_reconstitue2.version  = 0
        c_reconstitue2._recharger()
    t_avec = (time.perf_counter() - t0) / 20 * 1000

    snap = store_avec.charger_snapshot("acc-perf-avec")
    events_apres_snap = store_avec.nb_evenements("acc-perf-avec") - (snap.version + 1 if snap else 0)

    print(f"  {N_EVENTS} événements par compte, 20 reconstitutions mesurées :\n")
    print(f"  {'Stratégie':<25}  {'Events à rejouer':>18}  {'Temps moy':>10}")
    print("  " + "─"*58)
    print(f"  {'Sans snapshot':<25}  {N_EVENTS+1:>18}  {t_sans:>8.3f}ms")
    print(f"  {'Avec snapshot (tous 10)':<25}  {events_apres_snap:>18}  {t_avec:>8.3f}ms")
    print(f"\n  Snapshots créés : {store_avec.stats['snapshots_crees']}")
    if snap:
        print(f"  Dernier snapshot : version={snap.version}, "
              f"solde={snap.etat['solde']:.2f}€")
    print(f"  Events rejoués après snapshot : {events_apres_snap} (au lieu de {N_EVENTS+1})")

    if t_sans > 0:
        gain = t_sans / max(t_avec, 0.001)
        print(f"  Gain de performance : ~{gain:.1f}x ✅")

    print(f"""
  À 100 000 événements (compte très actif sur 10 ans) :
    Sans snapshot : rejouer 100 000 events à chaque lecture → secondes
    Avec snapshot : rejouer ~10 events → microsecondes
  Stratégie production : snapshot tous les 50-100 events.
    """)


# ─── SCÉNARIO 4 : CONCURRENCE OPTIMISTE ───────────────────────────────────────

def scenario_concurrence():
    titre("SCÉNARIO 4 — Concurrence optimiste : deux clients, même aggregate")

    print("""
  Problème classique : Alice et Bob lisent le même compte simultanément.
  Tous les deux veulent faire un débit. Sans protection → Lost Update.

  Event Sourcing utilise la concurrence optimiste :
    Chaque append vérifie version_attendue == version_actuelle.
    Si quelqu'un a écrit entre-temps → ConcurrencyException → retry.
    Pas de verrou distribué nécessaire (contrairement à 2PC ou Redlock).
    """)

    store = EventStore()
    compte = Compte("acc-concurrent", store)
    compte.ouvrir("Charlie", solde_initial=1000.0)

    # Simuler deux clients qui lisent l'état (version=1) puis écrivent
    version_lue_par_alice = store.nb_evenements("acc-concurrent")
    version_lue_par_bob   = store.nb_evenements("acc-concurrent")

    print(f"  Solde initial : {compte.solde:.2f}€  (version={compte.version})")
    print(f"  Alice et Bob lisent le compte simultanément (version={version_lue_par_alice})\n")

    resultats = {}

    def alice_debite():
        try:
            # Alice fait son débit en fournissant la version qu'elle a lue
            from event_sourcing import CompteDebite
            e = CompteDebite(montant=400.0, motif="Loyer Alice",
                             destinataire="propriétaire")
            store.appendre("acc-concurrent", e,
                           version_attendue=version_lue_par_alice)
            resultats["alice"] = "✅ succès (version acceptée)"
        except ConcurrencyException as ex:
            resultats["alice"] = f"❌ ConcurrencyException : {ex}"

    def bob_debite():
        try:
            # Bob essaie aussi avec la même version initiale
            time.sleep(0.01)  # Bob arrive légèrement après Alice
            from event_sourcing import CompteDebite
            e = CompteDebite(montant=700.0, motif="Loyer Bob",
                             destinataire="propriétaire")
            store.appendre("acc-concurrent", e,
                           version_attendue=version_lue_par_bob)
            resultats["bob"] = "✅ succès (version acceptée)"
        except ConcurrencyException as ex:
            resultats["bob"] = f"❌ ConcurrencyException : {ex}"

    t_alice = threading.Thread(target=alice_debite)
    t_bob   = threading.Thread(target=bob_debite)
    t_alice.start()
    t_bob.start()
    t_alice.join()
    t_bob.join()

    print(f"  Alice (débit 400€)  : {resultats.get('alice', '(aucun résultat)')}")
    print(f"  Bob   (débit 700€)  : {resultats.get('bob',   '(aucun résultat)')}")

    events = store.lire_stream("acc-concurrent")
    print(f"\n  Stream final ({len(events)} événements) :")
    afficher_stream(store, "acc-concurrent")

    # Reconstruire le solde final
    solde_final = 1000.0
    for e in events:
        if e.type_event == "CompteDebite":
            solde_final -= e.montant
        elif e.type_event == "CompteCredite":
            solde_final += e.montant

    nb_debits = len([e for e in events if e.type_event == "CompteDebite"])
    print(f"\n  Solde final : {solde_final:.2f}€  ({nb_debits} débit(s) accepté(s))")
    print(f"""
  Sans concurrence optimiste : les deux débits passeraient
  → solde = 1000 - 400 - 700 = -100€  (découvert non autorisé)
  Avec version check : seul le premier gagne, le second fait un retry ✅

  En pratique :
    Retry automatique : recharger l'aggregate, re-valider les règles métier,
    re-tenter l'append. Bob voit alors solde=600€ < 700€ → règle métier refuse.
    """)


# ─── SCÉNARIO 5 : PROJECTIONS CQRS ───────────────────────────────────────────

def scenario_projections():
    titre("SCÉNARIO 5 — Projections CQRS : même events, vues radicalement différentes")

    print("""
  CQRS (Command Query Responsibility Segregation) :
    Command side : append d'événements (écriture)
    Query side   : projections optimisées pour la lecture

  Depuis les MÊMES événements, on peut construire N projections :
    → Solde courant par compte  (ProjectionSoldes)
    → Historique des transactions (ProjectionHistorique)
    → Rapport de risque (transactions > 1000€)
    → Tableau de bord temps réel

  Avantage clé : si on veut une nouvelle vue, on la construit
  en rejouant tous les événements depuis le début. Pas de migration SQL.
    """)

    store = EventStore()

    # ── Projection 1 : Soldes ──────────────────────────────────────────────
    proj_soldes = ProjectionSoldes(store)

    # ── Projection 2 : Historique (construite manuellement) ───────────────
    historique: dict[str, list[dict]] = defaultdict(list)
    def on_debit(e: CompteDebite):
        historique[e.aggregate_id].append({
            "type": "débit", "montant": e.montant,
            "motif": e.motif, "ts": e.ts
        })
    def on_credit(e: CompteCredite):
        historique[e.aggregate_id].append({
            "type": "crédit", "montant": e.montant,
            "motif": e.motif, "ts": e.ts
        })
    store.souscrire("CompteDebite",  on_debit)
    store.souscrire("CompteCredite", on_credit)

    # ── Projection 3 : Transactions à risque (> 500€) ─────────────────────
    transactions_risque: list[dict] = []
    def on_gros_debit(e: CompteDebite):
        if e.montant >= 500:
            transactions_risque.append({
                "account": e.aggregate_id, "montant": e.montant,
                "motif": e.motif
            })
    store.souscrire("CompteDebite", on_gros_debit)

    # Créer plusieurs comptes avec activité
    comptes_config = [
        ("acc-p1", "Paul",  500.0),
        ("acc-p2", "Marie", 2000.0),
        ("acc-p3", "Jean",  100.0),
    ]
    comptes = {}
    for cid, nom, solde in comptes_config:
        c = Compte(cid, store)
        c.ouvrir(nom, solde_initial=solde)
        comptes[cid] = c

    # Opérations variées
    comptes["acc-p1"].crediter(1000.0, "Salaire",    "employeur")
    comptes["acc-p1"].debiter(  800.0, "Loyer",      "propriétaire")
    comptes["acc-p1"].debiter(   50.0, "Transport",  "SNCF")
    comptes["acc-p2"].debiter(  600.0, "Voyage",     "Air France")
    comptes["acc-p2"].crediter(  300.0, "Remboursement", "assurance")
    comptes["acc-p3"].crediter(  200.0, "Freelance",  "client")
    comptes["acc-p3"].debiter(   30.0, "Café",       "brasserie")

    attendre(0.1)  # Laisser les projections se mettre à jour

    # ── Afficher les 3 projections ─────────────────────────────────────────
    print(f"  Projection 1 — Soldes actuels (mise à jour temps réel) :\n")
    print(f"  {'Compte':<12} {'Solde':>10}")
    print("  " + "─"*25)
    for cid, nom, _ in comptes_config:
        solde = proj_soldes.solde(cid)
        print(f"  {cid:<12} {solde:>8.2f}€")

    print(f"\n  Projection 2 — Historique par compte :\n")
    for cid in ["acc-p1", "acc-p2"]:
        ops = historique.get(cid, [])
        print(f"  {cid} ({len(ops)} opérations) :")
        for op in ops:
            signe = "-" if op["type"] == "débit" else "+"
            print(f"    {signe}{op['montant']:>7.2f}€  {op['motif']}")

    print(f"\n  Projection 3 — Transactions à risque (≥ 500€) :\n")
    if transactions_risque:
        for t in transactions_risque:
            print(f"  ⚠️  {t['account']}  {t['montant']:.2f}€  {t['motif']}")
    else:
        print("  (aucune)")

    print(f"""
  Reconstruction à froid (nouvelle projection) :
    proj_soldes.reconstruire_depuis(store)
    → Rejoue tous les événements → état identique ✅
    Utile : ajouter une nouvelle projection sans changer le modèle d'écriture.

  Avantage CQRS :
    Chaque projection est optimisée pour son cas de lecture.
    Projection soldes → dict en RAM (O(1) par lookup).
    Projection historique → liste ordonnée (O(1) append).
    Projection risque → filtre en temps réel.
    Impossible à obtenir efficacement depuis 1 seule table SQL normalisée.
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 19 — EVENT SOURCING + CQRS                       ║")
    print("╚" + "═"*62 + "╝")

    scenario_audit_trail()
    scenario_time_travel()
    scenario_snapshots()
    scenario_concurrence()
    scenario_projections()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Event Sourcing — Ce qu'il faut retenir :

    Stocker les ÉVÉNEMENTS, pas l'état.
    L'état courant = replay de tous les événements.
    Les événements sont IMMUABLES — jamais de UPDATE/DELETE.

    Event  : fait passé immuable (CompteDebite, CommandePassee)
    Aggregate : entité qui produit des événements
    Projection : vue calculée depuis les événements (Query Side)
    Snapshot   : état capturé à T pour accélérer la reconstruction

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Audit trail complet et gratuit depuis le stream ✅
    Scénario 2 → Time-travel : solde exact à chaque version ✅
    Scénario 3 → Snapshot : O(events depuis snap) vs O(total events) ✅
    Scénario 4 → Concurrence optimiste : un seul gagne, pas de Lost Update ✅
    Scénario 5 → 3 projections différentes depuis les mêmes événements ✅

  Utilisé en production :
    EventStoreDB  → base dédiée Event Sourcing, subscribe, catchup
    Axon Framework → Java, Event Sourcing + CQRS intégré
    Kafka         → log d'événements distribué (pas un event store pur)
    PostgreSQL    → table events avec LISTEN/NOTIFY pour les projections

  Limites à connaître :
    ❌ Schéma évolutif : renommer un champ → upcasting nécessaire
    ❌ Requêtes complexes : impossible sans projection dédiée
    ❌ Suppression RGPD : "droit à l'oubli" vs immuabilité → crypto-shredding

  → Jour 20 : CQRS approfondi + Read Models
    Séparation complète commande/requête avec des stores différents.
    Le Read Model est mis à jour asynchronement depuis les événements.
  """)


if __name__ == "__main__":
    main()
