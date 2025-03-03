"""
Jour 3 — Simulation : Scénarios Lamport Timestamps
====================================================
On simule 4 scénarios progressifs :
  1. Le problème de base (horloges physiques qui mentent)
  2. Solution Lamport sur 3 processus
  3. Scénario e-commerce (commande → paiement → livraison)
  4. Détection de causalité et ordre global
"""

import time
import random
import threading
from lamport import Processus, Evenement


# ─── UTILITAIRES D'AFFICHAGE ─────────────────────────────────────────────────

def ligne(c="═", n=65):
    return c * n

def titre(texte):
    print(f"\n{ligne()}")
    print(f"  {texte}")
    print(ligne())

def afficher_journal(evenements: list[Evenement], entete="Journal des événements"):
    print(f"\n  {entete} :")
    print(f"  {'─'*61}")
    print(f"  {'t':>4} {'Processus':<14} {'Type':<10} Description")
    print(f"  {'─'*61}")
    for e in sorted(evenements):
        icone = {"INTERNE": "⚙️ ", "ENVOI": "📤", "RECEPTION": "📥"}.get(e.type_evt, "  ")
        print(f"  {e.timestamp:>4}  {e.processus:<14} {icone} {e.description}")
    print(f"  {'─'*61}")


def afficher_diagramme_sequence(evenements: list[Evenement], processus: list[str]):
    """
    Affiche un diagramme de séquence ASCII des échanges de messages.
    """
    print(f"\n  Diagramme de séquence :")
    
    # En-tête
    cols = {p: i for i, p in enumerate(processus)}
    largeur_col = 18
    header = "  " + "".join(f"{p:^{largeur_col}}" for p in processus)
    print(header)
    print("  " + "".join(f"{'│':^{largeur_col}}" for _ in processus))

    for e in sorted(evenements):
        if e.type_evt == "INTERNE":
            row = ["│" for _ in processus]
            col = cols[e.processus]
            row[col] = f"●[t={e.timestamp}]"
            print("  " + "".join(f"{r:^{largeur_col}}" for r in row))

        elif e.type_evt == "ENVOI":
            row = ["│" for _ in processus]
            col = cols[e.processus]
            row[col] = f"◄[t={e.timestamp}]"
            print("  " + "".join(f"{r:^{largeur_col}}" for r in row))

        elif e.type_evt == "RECEPTION":
            row = ["│" for _ in processus]
            col = cols[e.processus]
            row[col] = f"►[t={e.timestamp}]"
            print("  " + "".join(f"{r:^{largeur_col}}" for r in row))

    print("  " + "".join(f"{'│':^{largeur_col}}" for _ in processus))


# ─── SCÉNARIO 1 : LE PROBLÈME ────────────────────────────────────────────────

def scenario_probleme():
    titre("SCÉNARIO 1 — Le problème : les horloges physiques mentent")

    print("""
  Imaginons 2 serveurs, Paris et Tokyo.
  Réseau instable → Tokyo a 500ms de retard sur Paris.
  
  Séquence d'événements RÉELS (ordre causal) :
    1. Paris : "Utilisateur crée le compte"    (10:00:00.000)
    2. Paris → Tokyo : "Synchronise le compte" (10:00:00.100)  
    3. Tokyo : "Utilisateur se connecte"       (10:00:00.050) ← horloge en retard !
    
  Si on trie par horloge physique :
    10:00:00.000  Paris   → Création compte  ✅
    10:00:00.050  Tokyo   → Connexion        ← PROBLÈME : avant la synchro !
    10:00:00.100  Paris   → Synchro          
    
  Tokyo pense que la connexion s'est faite AVANT de recevoir le compte.
  Le système croit que l'utilisateur s'est connecté à un compte inexistant !
    """)

    # Simulation avec horloges physiques
    paris_physique  = []
    tokyo_physique  = []
    derive_tokyo_ms = -500  # Tokyo a 500ms de retard

    t_base = time.time() * 1000

    paris_physique.append((t_base + 0,   "Paris",  "Création compte"))
    paris_physique.append((t_base + 100, "Paris",  "Synchro → Tokyo"))
    tokyo_physique.append((t_base + 50,  "Tokyo",  "Connexion utilisateur"))  # horloge décalée

    tous = paris_physique + tokyo_physique
    tous_tries = sorted(tous, key=lambda x: x[0])

    print("  Ordre selon l'horloge physique (INCORRECT) :")
    for i, (ts, proc, desc) in enumerate(tous_tries, 1):
        decalage = ts - t_base
        print(f"    {i}. [{proc}] +{decalage:.0f}ms → {desc}")

    print("\n  ❌ La connexion (Tokyo) apparaît avant la synchro (Paris→Tokyo) !")
    print("  → Un système distribué ne peut pas se fier aux horloges physiques.")


# ─── SCÉNARIO 2 : SOLUTION LAMPORT ───────────────────────────────────────────

def scenario_solution_lamport():
    titre("SCÉNARIO 2 — Solution : Lamport Timestamps")

    print("""
  Même scénario, mais avec des horloges logiques de Lamport.
  Les horloges physiques n'ont plus d'importance.
  La CAUSALITÉ est préservée par les règles de Lamport.
    """)

    paris = Processus("Paris", derive_ms=0)
    tokyo = Processus("Tokyo", derive_ms=-500)

    # Étape 1 : Paris crée le compte (événement interne)
    paris.evenement_interne("Création du compte utilisateur")

    # Étape 2 : Paris envoie la synchro à Tokyo
    msg, ts_envoi = paris.preparer_message("compte_sync")

    # Étape 3 : Tokyo reçoit (sa propre horloge était à 0 → passe à max(0,2)+1=3)
    tokyo.recevoir_message(msg, ts_envoi)

    # Étape 4 : Tokyo enregistre la connexion APRÈS avoir reçu le compte
    tokyo.evenement_interne("Connexion utilisateur (compte reçu)")

    tous_evenements = paris.evenements + tokyo.evenements

    afficher_journal(tous_evenements, "Ordre Lamport (CORRECT)")
    afficher_diagramme_sequence(tous_evenements, ["Paris", "Tokyo"])

    print("""
  ✅ Avec Lamport :
     t=1 Paris INTERNE → Création compte
     t=2 Paris ENVOI   → Synchro Tokyo
     t=3 Tokyo RECEPT. → Réception synchro   (max(0,2)+1 = 3)
     t=4 Tokyo INTERNE → Connexion           (après réception !)
     
  La connexion (t=4) est TOUJOURS après la synchro (t=3).
  Garanti même si l'horloge physique de Tokyo est en retard.
    """)


# ─── SCÉNARIO 3 : COMMANDE E-COMMERCE ────────────────────────────────────────

def scenario_ecommerce():
    titre("SCÉNARIO 3 — E-commerce : Commande → Paiement → Stock → Livraison")

    print("  Simulation d'un flux de commande sur 4 microservices.\n")

    # Les 4 services
    api      = Processus("API-Gateway")
    paiement = Processus("Paiement")
    stock    = Processus("Stock")
    livraison = Processus("Livraison")

    # Flux de la commande
    # 1. Client passe commande
    api.evenement_interne("Commande reçue (user=42, produit=P001, qté=2)")

    # 2. API → Service Paiement
    msg, ts = api.preparer_message("PAYER: 89.99€, cmd=CMD-001")
    paiement.recevoir_message(msg, ts)

    # 3. Paiement traite
    paiement.evenement_interne("Vérification carte bancaire...")
    paiement.evenement_interne("Paiement autorisé ✅")

    # 4. Paiement → API (confirmation)
    msg, ts = paiement.preparer_message("PAIEMENT_OK: cmd=CMD-001")
    api.recevoir_message(msg, ts)

    # 5. API → Stock (en parallèle avec la notification)
    msg, ts = api.preparer_message("RESERVER: P001 x2, cmd=CMD-001")
    stock.recevoir_message(msg, ts)

    # 6. Stock vérifie et réserve
    stock.evenement_interne("Stock disponible: 42 unités")
    stock.evenement_interne("Réservation P001 x2 → stock=40")

    # 7. Stock → Livraison
    msg, ts = stock.preparer_message("EXPEDIER: P001 x2, addr=Paris, cmd=CMD-001")
    livraison.recevoir_message(msg, ts)

    # 8. Livraison programme
    livraison.evenement_interne("Création bon de livraison BL-2024-789")
    livraison.evenement_interne("Transporteur assigné: Colissimo J+1")

    # 9. Livraison → API (confirmation finale)
    msg, ts = livraison.preparer_message("LIVRAISON_PLANIFIEE: BL-2024-789")
    api.recevoir_message(msg, ts)

    # 10. API clôture
    api.evenement_interne("Commande CMD-001 complète → email client envoyé")

    tous = api.evenements + paiement.evenements + stock.evenements + livraison.evenements
    afficher_journal(tous, "Flux e-commerce ordonné par Lamport")

    print("""
  📌 Points clés de ce scénario :
  
  - Chaque service a son propre compteur Lamport indépendant
  - Les messages propagent le temps logique entre services
  - L'ordre final reflète la CAUSALITÉ réelle du flux métier
  - Si on rejoue les événements dans cet ordre → état cohérent garanti
  - C'est la base de l'Event Sourcing (Jour 19 du challenge !)
    """)


# ─── SCÉNARIO 4 : CONCURRENCE ET LIMITE DE LAMPORT ───────────────────────────

def scenario_concurrence():
    titre("SCÉNARIO 4 — Limite : événements concurrents")

    print("""
  Problème connu de Lamport : si deux événements n'ont PAS
  de relation causale (concurrents), leurs timestamps peuvent
  être dans n'importe quel ordre — et ce n'est pas un bug.
  
  Exemple : deux utilisateurs modifient le même document
  en même temps sur deux serveurs différents.
    """)

    serveur_a = Processus("Serveur-A")
    serveur_b = Processus("Serveur-B")

    # Les deux modifient sans se parler (concurrent !)
    serveur_a.evenement_interne("User Alice : titre = 'Rapport Q3'")
    serveur_a.evenement_interne("User Alice : section 1 ajoutée")

    serveur_b.evenement_interne("User Bob   : titre = 'Rapport Q4'")  # Concurrent !
    serveur_b.evenement_interne("User Bob   : section 2 ajoutée")

    # Seulement MAINTENANT ils se synchronisent
    msg, ts = serveur_a.preparer_message("SYNC: état A → B")
    serveur_b.recevoir_message(msg, ts)

    serveur_b.evenement_interne("CONFLIT détecté : 'Q3' vs 'Q4' !")

    tous = serveur_a.evenements + serveur_b.evenements
    afficher_journal(tous, "Événements concurrents")

    print("""
  ⚠️  Lamport ne résout PAS les conflits concurrents.
  
  Pour ça, il faut :
    → Vector Clocks (Jour 14) : détecte précisément la concurrence
    → CRDT              : fusionne automatiquement sans conflit
    → Last-Write-Wins   : simple mais risqué (perte de données)
    
  Propriété formelle de Lamport :
    A → B  ⟹  L(A) < L(B)          [causalité implique ordre]
    L(A) < L(B)  ⟹  PAS forcément A → B  [ordre n'implique PAS causalité]
    L(A) = L(B)  ⟹  événements concurrents ou même instant logique
    """)


# ─── SCÉNARIO 5 : THREADS PARALLÈLES ─────────────────────────────────────────

def scenario_threads():
    titre("SCÉNARIO 5 — Simulation temps réel avec threads")

    print("  3 processus tournent en parallèle, s'échangeant des messages.\n")

    p1 = Processus("Nœud-1")
    p2 = Processus("Nœud-2")
    p3 = Processus("Nœud-3")

    messages_queue = {
        "2": [],  # Messages en attente pour Nœud-2
        "3": [],
    }
    queue_lock = threading.Lock()
    tous_evenements = []
    evt_lock = threading.Lock()

    def enregistrer(evts):
        with evt_lock:
            tous_evenements.extend(evts)

    def thread_noeud1():
        time.sleep(random.uniform(0, 0.05))
        p1.evenement_interne("Démarrage du nœud")
        enregistrer([p1.evenements[-1]])

        time.sleep(random.uniform(0.01, 0.05))
        msg, ts = p1.preparer_message("HEARTBEAT depuis Nœud-1")
        enregistrer([p1.evenements[-1]])
        with queue_lock:
            messages_queue["2"].append((msg, ts))
            messages_queue["3"].append((msg, ts))

        time.sleep(random.uniform(0.01, 0.05))
        p1.evenement_interne("Traitement local terminé")
        enregistrer([p1.evenements[-1]])

    def thread_noeud2():
        time.sleep(random.uniform(0, 0.05))
        p2.evenement_interne("Démarrage du nœud")
        enregistrer([p2.evenements[-1]])

        # Attendre le message de Nœud-1
        for _ in range(20):
            time.sleep(0.02)
            with queue_lock:
                if messages_queue["2"]:
                    msg, ts = messages_queue["2"].pop(0)
                    p2.recevoir_message(msg, ts)
                    enregistrer([p2.evenements[-1]])
                    break

        p2.evenement_interne("Réponse au heartbeat traitée")
        enregistrer([p2.evenements[-1]])

    def thread_noeud3():
        time.sleep(random.uniform(0.02, 0.08))
        p3.evenement_interne("Démarrage du nœud")
        enregistrer([p3.evenements[-1]])

        for _ in range(20):
            time.sleep(0.02)
            with queue_lock:
                if messages_queue["3"]:
                    msg, ts = messages_queue["3"].pop(0)
                    p3.recevoir_message(msg, ts)
                    enregistrer([p3.evenements[-1]])
                    break

        msg, ts = p3.preparer_message("ACK reçu, état synchro")
        enregistrer([p3.evenements[-1]])
        with queue_lock:
            messages_queue["2"].append((msg, ts))

    # Lancer les 3 threads
    threads = [
        threading.Thread(target=thread_noeud1),
        threading.Thread(target=thread_noeud2),
        threading.Thread(target=thread_noeud3),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=3)

    afficher_journal(tous_evenements, "Exécution parallèle — ordre Lamport")
    print("""
  ✅ Malgré l'exécution parallèle et les délais aléatoires,
     Lamport garantit un ordre causal cohérent.
     Les RÉCEPTIONS ont toujours un timestamp > ENVOI correspondant.
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═" * 63 + "╗")
    print("║   JOUR 3 — LAMPORT TIMESTAMPS : ORDONNER SANS HORLOGE    ║")
    print("╚" + "═" * 63 + "╝")

    scenario_probleme()
    scenario_solution_lamport()
    scenario_ecommerce()
    scenario_concurrence()
    scenario_threads()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Lamport Timestamps — Ce qu'il faut retenir :

  ┌─────────────────────────────────────────────────────────┐
  │ RÈGLES D'UNE HORLOGE LAMPORT                           │
  │                                                         │
  │  Événement interne   →  t = t + 1                       │
  │  Envoi message       →  t = t + 1  (envoyer t)          │
  │  Réception message   →  t = max(t_local, t_reçu) + 1   │
  └─────────────────────────────────────────────────────────┘
  
  Ce que Lamport GARANTIT :
    ✅ Si A cause B  →  lamport(A) < lamport(B)
    ✅ Ordre total déterministe (pour un tiebreak : ajouter PID)
    ✅ Fonctionne sans synchronisation d'horloges physiques
    
  Ce que Lamport ne GARANTIT PAS :
    ❌ lamport(A) < lamport(B)  ⟹  A cause B  (faux !)
    ❌ Détection des conflits concurrents
    → Pour ça : Vector Clocks (Jour 14)

  Utilisé en pratique dans :
    • Bases de données distribuées (CockroachDB, Spanner)
    • Systèmes de fichiers distribués (HDFS)
    • Protocoles de consensus (base de Raft, Paxos)
    • Message queues (Kafka offsets)

  → Jour 4 : Heartbeat — comment savoir qu'un nœud est mort ?
  """)


if __name__ == "__main__":
    main()
