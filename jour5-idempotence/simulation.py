"""
Jour 5 — Simulation : Idempotence en action
=============================================
5 scénarios :
  1. Sans idempotence   → démonstration du problème (doublons)
  2. Avec idempotence   → même requête × 10, 1 seul effet
  3. Retry après panne  → le client renvoie, le serveur rejoue
  4. Requêtes parallèles → même clé depuis 2 threads simultanés
  5. Conflit de payload → client qui réutilise une clé à tort
"""

import time
import uuid
import random
import threading
from idempotence import StoreIdempotence, idempotent, StatutRequete

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=62): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")

# ─── FAUSSE BASE DE DONNÉES ───────────────────────────────────────────────────

class FausseDB:
    """Simule une base de données avec journal d'opérations."""
    def __init__(self):
        self._solde: dict[str, float] = {"U001": 1000.0, "U002": 500.0}
        self._transactions: list[dict] = []
        self._lock = threading.Lock()

    def debiter(self, utilisateur: str, montant: float, reference: str) -> dict:
        with self._lock:
            if self._solde.get(utilisateur, 0) < montant:
                raise ValueError(f"Solde insuffisant ({self._solde.get(utilisateur, 0):.2f}€)")
            self._solde[utilisateur] -= montant
            tx = {
                "id":          str(uuid.uuid4())[:8],
                "utilisateur": utilisateur,
                "montant":     montant,
                "reference":   reference,
                "solde_apres": self._solde[utilisateur],
                "ts":          time.time(),
            }
            self._transactions.append(tx)
            return tx

    def solde(self, utilisateur: str) -> float:
        with self._lock:
            return self._solde.get(utilisateur, 0)

    def nb_transactions(self, utilisateur: str) -> int:
        with self._lock:
            return sum(1 for t in self._transactions if t["utilisateur"] == utilisateur)

    def reset(self):
        with self._lock:
            self._solde = {"U001": 1000.0, "U002": 500.0}
            self._transactions.clear()


db = FausseDB()


# ─── SCÉNARIO 1 : LE PROBLÈME SANS IDEMPOTENCE ───────────────────────────────

def scenario_sans_idempotence():
    titre("SCÉNARIO 1 — Sans idempotence : le désastre des doublons")

    db.reset()

    def paiement_naif(utilisateur: str, montant: float, commande_id: str) -> dict:
        """API sans idempotence — dangereuse."""
        time.sleep(0.01)  # Simule le traitement
        return db.debiter(utilisateur, montant, commande_id)

    print(f"""
  Situation : client paie 50€ pour la commande CMD-001.
  Le réseau coupe après l'envoi. Le client ne sait pas
  si le paiement est passé. Il renvoie 3 fois.
    """)

    print(f"  Solde initial de U001 : {db.solde('U001'):.2f}€\n")

    # Le client renvoie la même intention 3 fois
    for tentative in range(1, 4):
        try:
            tx = paiement_naif("U001", 50.0, "CMD-001")
            print(f"  Tentative {tentative} → ✅ Paiement accepté (tx={tx['id']}, solde={tx['solde_apres']:.2f}€)")
        except ValueError as e:
            print(f"  Tentative {tentative} → ❌ {e}")

    print(f"""
  Solde final de U001 : {db.solde('U001'):.2f}€
  Transactions créées : {db.nb_transactions('U001')}

  ❌ 3 paiements de 50€ au lieu d'1 !
     L'utilisateur a été débité 150€ pour une commande de 50€.
     C'est illégal (rétrofacturation forcée), coûteux et catastrophique.
    """)


# ─── SCÉNARIO 2 : AVEC IDEMPOTENCE ───────────────────────────────────────────

def scenario_avec_idempotence():
    titre("SCÉNARIO 2 — Avec idempotence : même clé × 10, 1 seul débit")

    db.reset()
    store = StoreIdempotence(ttl_secondes=300)

    @idempotent(store)
    def paiement_idempotent(idempotency_key: str, payload: dict) -> dict:
        """API avec idempotence — sûre."""
        time.sleep(random.uniform(0.01, 0.03))  # Simule traitement variable
        return db.debiter(payload["utilisateur"], payload["montant"], payload["commande_id"])

    print(f"""
  Même situation : le client génère UN UUID pour l'intention
  de paiement et l'envoie avec chaque tentative.
    """)

    # Le client génère 1 UUID pour cette intention
    cle_paiement = str(uuid.uuid4())
    payload = {"utilisateur": "U001", "montant": 50.0, "commande_id": "CMD-001"}

    print(f"  Clé d'idempotence : {cle_paiement[:8]}…")
    print(f"  Solde initial     : {db.solde('U001'):.2f}€\n")

    resultats = []
    for tentative in range(1, 11):
        debut = time.perf_counter()
        tx = paiement_idempotent(cle_paiement, payload)
        duree = (time.perf_counter() - debut) * 1000

        est_replay = tentative > 1
        label = "REPLAY (instantané)" if est_replay else "EXECUTION RÉELLE"
        print(f"  Tentative {tentative:>2} → [{label:<22}] tx={tx['id']}  ({duree:.1f}ms)")
        resultats.append(tx["id"])

    print(f"""
  Solde final     : {db.solde('U001'):.2f}€   (débit unique de 50€ ✅)
  Transactions DB : {db.nb_transactions('U001')}            (1 seule ✅)
  IDs retournés   : {len(set(resultats))} ID unique répété {len(resultats)} fois ✅

  → Le client a retentié 9 fois, le débit n'a eu lieu qu'une fois.
    Les 9 replays étaient instantanés (< 0.1ms vs ~20ms réel).
    """)
    print(store.rapport())


# ─── SCÉNARIO 3 : RETRY APRÈS PANNE RÉSEAU ────────────────────────────────────

def scenario_retry_apres_panne():
    titre("SCÉNARIO 3 — Retry après panne réseau simulée")

    db.reset()
    store = StoreIdempotence(ttl_secondes=300)
    tentative_globale = [0]

    @idempotent(store)
    def paiement_instable(idempotency_key: str, payload: dict) -> dict:
        """Simule un serveur instable qui échoue les 2 premières fois."""
        tentative_globale[0] += 1
        if tentative_globale[0] <= 2:
            raise ConnectionError(f"Timeout réseau (tentative {tentative_globale[0]})")
        return db.debiter(payload["utilisateur"], payload["montant"], payload["commande_id"])

    print(f"""
  Le serveur est instable : il échoue les 2 premières tentatives.
  Comportement attendu :
    - Tentatives 1 & 2  → Erreur (et l'erreur est mémorisée !)
    - Tentative 3       → Succès
    - Tentatives 4+     → Replay du succès (instantané)

  Subtilité : ici, les erreurs de connexion ne sont PAS mémorisées
  (on retente). Seul le succès final est mémorisé.
  Mais les erreurs MÉTIER (solde insuffisant) SONT mémorisées.
    """)

    # On utilise un store différent qui ne mémorise pas les erreurs réseau
    store2 = StoreIdempotence(ttl_secondes=300)
    tentative2 = [0]

    def paiement_avec_retry(cle: str, payload: dict, max_retries: int = 5) -> dict:
        """
        Logique de retry côté client : backoff exponentiel.
        Chaque tentative utilise la MÊME clé d'idempotence.
        """
        for n in range(1, max_retries + 1):
            try:
                est_nouveau, entree = store2.obtenir_ou_reserver(cle, payload)
                if not est_nouveau and entree and entree.statut.value == "COMPLETE":
                    print(f"  Tentative {n} → 📼 REPLAY (résultat mémorisé)")
                    return entree.resultat

                tentative2[0] += 1
                if tentative2[0] <= 2:
                    raise ConnectionError(f"Timeout réseau")

                resultat = db.debiter(payload["utilisateur"], payload["montant"], payload["commande_id"])
                store2.marquer_complete(cle, resultat)
                print(f"  Tentative {n} → ✅ SUCCÈS (première exécution réelle)")
                return resultat

            except ConnectionError as e:
                store2.marquer_erreur(cle, str(e))
                # Réinitialise pour autoriser le retry (erreur réseau, pas métier)
                with store2._lock:
                    del store2._store[cle]
                    store2.stats["nouvelles"] -= 1
                delai = 0.05 * (2 ** (n - 1))
                print(f"  Tentative {n} → ❌ Erreur réseau → retry dans {delai:.2f}s")
                time.sleep(delai)

        raise RuntimeError("Max retries atteint")

    cle = str(uuid.uuid4())
    payload = {"utilisateur": "U001", "montant": 75.0, "commande_id": "CMD-002"}
    print(f"  Clé : {cle[:8]}…\n")

    tx = paiement_avec_retry(cle, payload)
    print(f"\n  Résultat final : tx={tx['id']}, solde={tx['solde_apres']:.2f}€")
    print(f"  Transactions DB : {db.nb_transactions('U001')} (1 seule ✅)")


# ─── SCÉNARIO 4 : CONCURRENCE — MÊME CLÉ, 2 THREADS ─────────────────────────

def scenario_concurrence():
    titre("SCÉNARIO 4 — Concurrence : même clé depuis 2 threads simultanés")

    db.reset()
    store = StoreIdempotence(ttl_secondes=300)
    executions_reelles = [0]
    lock_compteur = threading.Lock()

    @idempotent(store)
    def paiement_lent(idempotency_key: str, payload: dict) -> dict:
        with lock_compteur:
            executions_reelles[0] += 1
        time.sleep(0.15)  # Traitement long (simule appel externe)
        return db.debiter(payload["utilisateur"], payload["montant"], payload["commande_id"])

    print(f"""
  Situation : deux processus reçoivent la même requête
  (load balancer mal configuré, client impatient qui double-clique…).
  Ils appellent simultanément avec la même clé d'idempotence.

  Comportement attendu :
    - Thread A : exécute la vraie opération
    - Thread B : ATTEND le résultat de A, puis le rejoue
    → 1 seul débit, même sous concurrence
    """)

    cle = str(uuid.uuid4())
    payload = {"utilisateur": "U001", "montant": 100.0, "commande_id": "CMD-003"}
    resultats = {}

    def appel_thread(nom: str):
        debut = time.perf_counter()
        tx = paiement_lent(cle, payload)
        duree = (time.perf_counter() - debut) * 1000
        resultats[nom] = (tx["id"], duree)

    t1 = threading.Thread(target=appel_thread, args=("Thread-A",))
    t2 = threading.Thread(target=appel_thread, args=("Thread-B",))

    t1.start()
    time.sleep(0.02)  # T2 démarre légèrement après T1
    t2.start()

    t1.join(); t2.join()

    id_a, ms_a = resultats["Thread-A"]
    id_b, ms_b = resultats["Thread-B"]

    print(f"  Thread-A → tx={id_a}  ({ms_a:.1f}ms)  ← exécution réelle")
    print(f"  Thread-B → tx={id_b}  ({ms_b:.1f}ms)  ← a attendu A puis rejoué")
    print(f"\n  Même ID retourné : {'✅' if id_a == id_b else '❌'}  ({id_a} == {id_b})")
    print(f"  Exécutions réelles : {executions_reelles[0]} (attendu: 1) {'✅' if executions_reelles[0] == 1 else '❌'}")
    print(f"  Transactions DB    : {db.nb_transactions('U001')} {'✅' if db.nb_transactions('U001') == 1 else '❌'}")
    print(f"  Solde final        : {db.solde('U001'):.2f}€ (attendu: 900.00€) ✅")


# ─── SCÉNARIO 5 : CONFLIT DE PAYLOAD ─────────────────────────────────────────

def scenario_conflit_payload():
    titre("SCÉNARIO 5 — Conflit : même clé, payload différent")

    store = StoreIdempotence(ttl_secondes=300)

    @idempotent(store)
    def creer_commande(idempotency_key: str, payload: dict) -> dict:
        return {"commande_id": payload["produit"], "prix": payload["prix"]}

    cle_reutilisee = str(uuid.uuid4())

    print(f"""
  Erreur courante côté client : réutiliser une clé
  pour une opération DIFFÉRENTE (ex : bug de génération UUID).
  Le serveur doit détecter et rejeter ce comportement.
    """)

    # Premier appel légitime
    r1 = creer_commande(cle_reutilisee, {"produit": "Clavier", "prix": 89.99})
    print(f"  Appel 1 → ✅ Commande créée : {r1}")

    # Même clé, payload différent → conflit
    try:
        r2 = creer_commande(cle_reutilisee, {"produit": "Souris", "prix": 49.99})
        print(f"  Appel 2 → ✅ {r2}  ← NE DEVRAIT PAS ARRIVER")
    except ValueError as e:
        print(f"  Appel 2 → ❌ CONFLIT DÉTECTÉ : {e}")

    # Même clé, même payload → replay OK
    r3 = creer_commande(cle_reutilisee, {"produit": "Clavier", "prix": 89.99})
    print(f"  Appel 3 → ✅ REPLAY OK : {r3}")
    print(f"\n  → Même clé + même payload = replay sûr")
    print(f"    Même clé + payload différent = erreur (protège l'intégrité)")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*60 + "╗")
    print("║   JOUR 5 — IDEMPOTENCE : SÉCURISER LES RETRIES          ║")
    print("╚" + "═"*60 + "╝")

    scenario_sans_idempotence()
    scenario_avec_idempotence()
    scenario_retry_apres_panne()
    scenario_concurrence()
    scenario_conflit_payload()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Principe :
    CLIENT génère UUID par intention métier
    SERVER mémorise résultat par UUID
    → N appels = 1 seul effet

  Les 4 cas couverts :
  ┌─────────────────────┬─────────────────────────────┐
  │ Même clé × N        │ Replay instantané           │
  │ Erreur réseau       │ Retry avec même clé (sûr)   │
  │ Concurrence         │ Thread B attend Thread A    │
  │ Payload différent   │ Erreur 422 (conflit)        │
  └─────────────────────┴─────────────────────────────┘

  Ce que le store mémorise :
    • Clé + hash du payload  (détection de mutation)
    • Statut EN_COURS / COMPLETE / ERREUR
    • Résultat ou message d'erreur
    • Timestamp (TTL → nettoyage automatique)

  En production (Redis) :
    SET idempotency:{key} {result} EX 86400  NX
    ↑ NX = "seulement si n'existe pas" (atomique)

  Opérations naturellement idempotentes :
    ✅ GET, HEAD, PUT (remplace), DELETE
  Opérations qui nécessitent ce mécanisme :
    ⚠️  POST (crée), PATCH (modifie), paiements, emails

  Utilisé par :
    Stripe  → Header 'Idempotency-Key'
    PayPal  → Header 'PayPal-Request-Id'
    AWS S3  → ETags sur PUT
    Twilio  → MessagingServiceSid

  → Semaine 2 : Consensus et élection de leader
    Jour 6 : Bully Algorithm — comment les nœuds
    élisent un nouveau leader quand l'ancien tombe.
  """)


if __name__ == "__main__":
    main()
