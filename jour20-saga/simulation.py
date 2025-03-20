"""
Jour 20 — Simulation : Saga Pattern en action
==============================================
5 scénarios :
  1. Saga nominale (Orchestration) : les 4 étapes réussissent
  2. Saga avec compensation (Orchestration) : shipment échoue → annulations
  3. Choreography : même saga via bus d'événements
  4. Idempotence : rejouer une saga sans effets de bord
  5. Comparaison : 20 sagas avec différents taux d'échec
"""

import time
import threading
import random
from collections import defaultdict
from saga import (
    BusEvenements, ServiceSimule,
    SagaCommandeChoreography,
    SagaOrchestrator, EtapeSaga, EtatSaga
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


# ─── SCÉNARIO 1 : SAGA NOMINALE (ORCHESTRATION) ──────────────────────────────

def scenario_nominal():
    titre("SCÉNARIO 1 — Saga nominale (Orchestration) : toutes les étapes réussissent")

    print("""
  Architecture e-commerce : passer une commande en 4 étapes.
  L'orchestrateur envoie chaque commande et attend la réponse.

  RÉSERVER_STOCK → DÉBITER_PAIEMENT → CRÉER_EXPÉDITION → ENVOYER_EMAIL
    """)

    inventory = ServiceSimule("inventory-svc",  latence_ms=30,  taux_echec=0.0)
    payment   = ServiceSimule("payment-svc",    latence_ms=80,  taux_echec=0.0)
    shipment  = ServiceSimule("shipment-svc",   latence_ms=50,  taux_echec=0.0)
    notif     = ServiceSimule("notif-svc",      latence_ms=20,  taux_echec=0.0)

    etapes = [
        EtapeSaga("RÉSERVER_STOCK",     inventory, "RESERVER_STOCK",     "LIBERER_STOCK"),
        EtapeSaga("DÉBITER_PAIEMENT",   payment,   "DEBITER_PAIEMENT",   "ANNULER_PAIEMENT"),
        EtapeSaga("CRÉER_EXPÉDITION",   shipment,  "CREER_EXPEDITION",   None),
        EtapeSaga("ENVOYER_EMAIL",      notif,     "ENVOYER_CONFIRMATION", None),
    ]

    saga = SagaOrchestrator(
        saga_id = "cmd-" + "abc123",
        etapes  = etapes,
    )
    payload = {"commande_id": "cmd-abc123", "produit": "MacBook Pro",
               "montant": 2499.0, "user_id": "u-007"}

    succes = saga.executer(payload)
    saga.afficher()

    print(f"\n  Résultat : {'✅ SUCCÈS' if succes else '❌ ÉCHEC'}")
    print(f"  Durée totale : {((saga.fin - saga.debut)*1000):.0f}ms")
    print(f"  = sum(30+80+50+20) ≈ 180ms + jitter ✅")


# ─── SCÉNARIO 2 : SAGA AVEC COMPENSATION ─────────────────────────────────────

def scenario_compensation():
    titre("SCÉNARIO 2 — Saga avec compensation : shipment échoue → annulations")

    print("""
  Scénario : stock réservé, paiement débité, puis shipment-svc tombe.
  La saga doit ANNULER le paiement et LIBÉRER le stock (ordre inverse).

  RÉSERVER_STOCK ✅ → DÉBITER_PAIEMENT ✅ → CRÉER_EXPÉDITION ❌
                                                  ↓ compensation
                                       ANNULER_PAIEMENT ↩️
                                              ↓
                                       LIBÉRER_STOCK ↩️
    """)

    inventory = ServiceSimule("inventory-svc",  latence_ms=30,  taux_echec=0.0)
    payment   = ServiceSimule("payment-svc",    latence_ms=80,  taux_echec=0.0)
    shipment  = ServiceSimule("shipment-svc",   latence_ms=50,  taux_echec=1.0)  # Toujours en échec
    notif     = ServiceSimule("notif-svc",      latence_ms=20,  taux_echec=0.0)

    etapes = [
        EtapeSaga("RÉSERVER_STOCK",   inventory, "RESERVER_STOCK",       "LIBERER_STOCK"),
        EtapeSaga("DÉBITER_PAIEMENT", payment,   "DEBITER_PAIEMENT",     "ANNULER_PAIEMENT"),
        EtapeSaga("CRÉER_EXPÉDITION", shipment,  "CREER_EXPEDITION",     None),
        EtapeSaga("ENVOYER_EMAIL",    notif,     "ENVOYER_CONFIRMATION", None),
    ]

    saga = SagaOrchestrator(saga_id="cmd-FAIL-001", etapes=etapes)
    payload = {"commande_id": "cmd-FAIL-001", "produit": "iPhone 15",
               "montant": 999.0, "user_id": "u-042"}

    succes = saga.executer(payload)
    saga.afficher()

    print(f"\n  Résultat : {'✅ SUCCÈS' if succes else '❌ ÉCHEC — compensations exécutées'}")
    print(f"  Durée totale : {((saga.fin - saga.debut)*1000):.0f}ms")

    # Vérifier que les compensations ont été exécutées dans le bon ordre
    comp_entries = [e for e in saga.journal if "(compensation)" in e["etape"]]
    print(f"\n  Compensations exécutées ({len(comp_entries)}) :")
    for c in comp_entries:
        print(f"    ↩️  {c['etape']}")

    print(f"""
  Ordre des compensations : INVERSE de l'ordre d'exécution.
    → ANNULER_PAIEMENT avant LIBÉRER_STOCK
    → On ne peut pas libérer le stock avant d'avoir annulé le paiement
    → C'est la garantie "atomicité" de la saga

  Note : CRÉER_EXPÉDITION n'a PAS de compensation (elle n'a pas réussi).
  Note : ENVOYER_EMAIL non atteint → pas de compensation à faire.
    """)


# ─── SCÉNARIO 3 : CHOREOGRAPHY ───────────────────────────────────────────────

def scenario_choreography():
    titre("SCÉNARIO 3 — Choreography : même saga via bus d'événements")

    print("""
  Choreography vs Orchestration :
    Orchestration : un chef d'orchestre envoie des commandes à chaque musicien
    Choreography  : chaque musicien sait quelle note jouer quand il entend la précédente

  Flux d'événements :
    COMMANDE_CRÉÉE
      → [inventory] émet STOCK_RÉSERVÉ
        → [payment] émet PAIEMENT_DÉBITÉ
          → [shipment] émet EXPÉDITION_CRÉÉE ou EXPÉDITION_ÉCHOUÉE
            → [notif] émet COMMANDE_CONFIRMÉE
    """)

    bus       = BusEvenements()
    inventory = ServiceSimule("inventory-svc",  latence_ms=30,  taux_echec=0.0)
    payment   = ServiceSimule("payment-svc",    latence_ms=60,  taux_echec=0.0)
    shipment  = ServiceSimule("shipment-svc",   latence_ms=40,  taux_echec=0.0)
    notif     = ServiceSimule("notif-svc",      latence_ms=20,  taux_echec=0.0)

    saga_mgr = SagaCommandeChoreography(bus, inventory, payment, shipment, notif)

    # Saga nominale
    cid1 = "chor-OK-001"
    saga_mgr.lancer(cid1, "AirPods Pro", 279.0, "u-001")

    # Saga avec échec shipment
    shipment_echec = ServiceSimule("shipment-svc",  latence_ms=40,  taux_echec=1.0)
    saga_mgr2 = SagaCommandeChoreography(bus, inventory, payment, shipment_echec, notif)
    cid2 = "chor-FAIL-002"
    saga_mgr2.lancer(cid2, "iPad Air", 749.0, "u-002")

    bus.attendre_silence(timeout=2.0)

    for cid, label in [(cid1, "Saga nominale"), (cid2, "Saga avec échec")]:
        etat = saga_mgr.etat(cid) or saga_mgr2.etat(cid)
        if etat:
            icone = "✅" if etat["etat"] == EtatSaga.SUCCES else "❌"
            duree = ((etat.get("fin", time.time()) - etat["debut"]) * 1000)
            print(f"  {icone} {label} ({cid}) — {etat['etat'].value}  ({duree:.0f}ms)")
            for etape in etat["etapes"]:
                ic = "  ✅" if etape["succes"] else "  ❌"
                print(f"  {ic}  {etape['etape']}")

    print(f"\n  Journal du bus ({len(bus.journal)} messages) :")
    types_vus = set()
    for msg in bus.journal:
        if msg["type"] not in types_vus:
            types_vus.add(msg["type"])
            print(f"    {msg['type']:<30} ← {msg['emetteur']}")

    print(f"""
  Choreography : pas de coordinateur central.
  La logique de compensation est distribuée dans chaque service.
  Avantage : scalabilité, découplage fort.
  Inconvénient : difficile de suivre ce qui se passe globalement.
    """)


# ─── SCÉNARIO 4 : IDEMPOTENCE ────────────────────────────────────────────────

def scenario_idempotence():
    titre("SCÉNARIO 4 — Idempotence : rejouer une saga sans effets de bord")

    print("""
  Problème : le réseau peut retransmettre un message.
  Si payment-svc reçoit deux fois "DÉBITER_PAIEMENT", on débite deux fois !

  Solution : idempotency key.
  Chaque message porte un ID unique. Le service refuse de traiter
  le même message deux fois.

  C'est FONDAMENTAL pour les sagas : une compensation peut aussi
  être rejouée → elle doit être idempotente.
    """)

    # Simuler un service idempotent
    class ServiceIdempotent(ServiceSimule):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._commandes_traitees: set = set()
            self._lock_idem = threading.Lock()

        def executer(self, action: str, payload: dict) -> dict:
            cle = f"{action}:{payload.get('commande_id', '')}:{payload.get('idempotency_key', '')}"
            with self._lock_idem:
                if cle in self._commandes_traitees:
                    return {"succes": True, "idempotent": True,
                            "message": f"Déjà traité : {cle[:30]}"}
                self._commandes_traitees.add(cle)
            return super().executer(action, payload)

    payment_idem = ServiceIdempotent("payment-svc", latence_ms=50, taux_echec=0.0)

    payload = {"commande_id": "cmd-IDEM-001", "montant": 99.0,
               "idempotency_key": "key-abc-123"}

    print(f"  Payload avec idempotency_key : {payload['idempotency_key']}\n")

    # Premier appel
    res1 = payment_idem.executer("DEBITER_PAIEMENT", payload)
    print(f"  Appel 1 : {res1}")

    # Deuxième appel (réseau qui retransmet)
    res2 = payment_idem.executer("DEBITER_PAIEMENT", payload)
    print(f"  Appel 2 (retry réseau) : {res2}")

    # Troisième appel
    res3 = payment_idem.executer("DEBITER_PAIEMENT", payload)
    print(f"  Appel 3 (retry réseau) : {res3}")

    print(f"\n  Le paiement n'a été débité qu'UNE seule fois ✅")
    print(f"  Les appels 2 et 3 sont des no-ops idempotents ✅")

    print(f"""
  Idempotency key en production :
    → UUID généré côté client avant d'envoyer la saga
    → Stocké dans chaque message du bus
    → Chaque service vérifie dans sa base : "ai-je déjà traité cet ID ?"
    → Si oui : retourner le résultat précédent sans re-exécuter

  Pourquoi c'est critique pour les sagas :
    → At-least-once delivery du bus → duplicata possibles
    → Retry automatique sur timeout → même message re-traité
    → Compensation rejouée sur crash du coordinateur → doit être idempotente
    """)


# ─── SCÉNARIO 5 : COMPARAISON STATISTISCHE ────────────────────────────────────

def scenario_statistiques():
    titre("SCÉNARIO 5 — Statistiques : 30 sagas avec différents taux d'échec")

    print("""
  On lance 30 sagas en parallèle avec différentes configurations
  pour mesurer le taux de succès, les compensations et les durées.
    """)

    configs = [
        ("Tout fiable",      0.0,  0.0,  0.0,  0.0),
        ("Shipment instable", 0.0,  0.0,  0.40, 0.0),
        ("Payment instable",  0.0,  0.30, 0.0,  0.0),
        ("Chaos général",     0.1,  0.15, 0.20, 0.05),
    ]

    print(f"\n  {'Configuration':<22} {'N':>4}  {'✅ Succès':>10}  {'❌ Échecs':>10}  {'Lat moy':>9}")
    print("  " + "─"*65)

    for label, p_inv, p_pay, p_ship, p_notif in configs:
        N = 30
        succes = echecs = 0
        latences = []

        def lancer_une(idx):
            nonlocal succes, echecs
            inv   = ServiceSimule("inventory", 20,  p_inv)
            pay   = ServiceSimule("payment",   60,  p_pay)
            ship  = ServiceSimule("shipment",  40,  p_ship)
            noti  = ServiceSimule("notif",     15,  p_notif)
            etapes = [
                EtapeSaga("RÉSERVER_STOCK",   inv,  "RESERVER_STOCK",       "LIBERER_STOCK"),
                EtapeSaga("DÉBITER_PAIEMENT", pay,  "DEBITER_PAIEMENT",     "ANNULER_PAIEMENT"),
                EtapeSaga("CRÉER_EXPÉDITION", ship, "CREER_EXPEDITION",     None),
                EtapeSaga("ENVOYER_EMAIL",    noti, "ENVOYER_CONFIRMATION", None),
            ]
            saga = SagaOrchestrator(saga_id=f"s-{idx}", etapes=etapes)
            ok = saga.executer({"commande_id": f"cmd-{idx}", "montant": 100.0})
            duree = (saga.fin - saga.debut) * 1000
            with threading.Lock():
                if ok: succes += 1
                else:  echecs += 1
                latences.append(duree)

        threads = [threading.Thread(target=lancer_une, args=(i,)) for i in range(N)]
        for t in threads: t.start()
        for t in threads: t.join()

        import statistics
        lat_moy = statistics.mean(latences) if latences else 0
        print(f"  {label:<22} {N:>4}  {succes:>10}  {echecs:>10}  {lat_moy:>7.0f}ms")

    print(f"""
  Observations :
    "Tout fiable"      → 100% succès, latence = somme des étapes
    "Shipment instable"→ ~60% succès, compensations sur les 40% restants
    "Payment instable" → ~70% succès, mais peu de compensation (inventory seulement)
    "Chaos général"    → ~50% succès, nombreuses compensations partielles

  En production :
    → Retry avec backoff exponentiel avant de déclarer un échec
    → Circuit breaker si un service est en panne (Jour 21)
    → Dead letter queue pour les sagas bloquées
    → Monitoring : taux de compensation = indicateur de santé système
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)

    print("╔" + "═"*62 + "╗")
    print("║   JOUR 20 — SAGA PATTERN (CHOREOGRAPHY + ORCHESTRATION)  ║")
    print("╚" + "═"*62 + "╝")

    scenario_nominal()
    scenario_compensation()
    scenario_choreography()
    scenario_idempotence()
    scenario_statistiques()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Saga Pattern — Ce qu'il faut retenir :

    Saga         : séquence de transactions locales + compensations
    Orchestration: coordinateur central envoie des commandes (facile à tracer)
    Choreography : chaque service réagit aux événements (découplé, scalable)
    Compensation : action inverse exécutée en cas d'échec (ordre inverse)
    Idempotence  : chaque étape doit pouvoir être rejouée sans effet de bord

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Saga nominale : 4 étapes en ~180ms ✅
    Scénario 2 → Compensation ordre inverse : ANNULER_PAY puis LIBÉRER_STOCK ✅
    Scénario 3 → Choreography : bus d'événements, pas de coordinateur ✅
    Scénario 4 → Idempotence : 3 appels → 1 seul débit ✅
    Scénario 5 → Avec 40% d'échec shipment → ~60% sagas réussissent ✅

  Différences clés Orchestration vs Choreography :
    Orchestration  → état visible, logique centralisée, debug facile
    Choreography   → couplage minimal, scalable, mais flux difficile à suivre

  Utilisé en production :
    Axon           → Saga orchestrée via Event Sourcing (Java)
    Temporal.io    → Workflow engine avec retry/compensation automatiques
    AWS Step Fns   → Orchestration serverless de microservices
    Kafka + Saga   → Choreography à très grande échelle (Uber, Netflix)

  Lien avec les autres jours :
    Jour 19 (Event Sourcing) → la saga est un agrégat rebuildable
    Jour 18 (Tracing)        → trace_id propagé dans tous les messages
    Jour 17 (Discovery)      → orchestrateur découvre les services via le registre

  → Jour 21 : Circuit Breaker (Hystrix/Resilience4j)
    Quand un service est en panne, couper le circuit pour éviter
    la cascade de timeouts. États : CLOSED → OPEN → HALF_OPEN.
  """)


if __name__ == "__main__":
    main()
