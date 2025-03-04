"""
Jour 4 — Simulation : Scénarios de pannes et détections
=========================================================
5 scénarios progressifs :
  1. Cluster sain — heartbeats normaux
  2. Panne franche — nœud qui s'arrête brutalement
  3. Panne lente — nœud qui ralentit avant de mourir (split brain)
  4. Résurrection — nœud mort qui redémarre
  5. Tempête de pannes — plusieurs nœuds tombent en cascade
"""

import time
import threading
import random
from heartbeat import MoniteurHeartbeat, NoeudReseau, EtatNoeud

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=62): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")
def attendre(s, msg=""):
    if msg: print(f"\n  ⏳ {msg} ({s}s)...")
    time.sleep(s)

def afficher_journal(moniteur, n=15):
    """Affiche les N derniers événements du journal."""
    print("\n  Journal des événements :")
    print("  " + "─"*55)
    with moniteur._journal_lock:
        entrees = moniteur.journal[-n:]
    if not entrees:
        print("  (vide)")
        return
    t0 = entrees[0][0]
    for ts, msg in entrees:
        print(f"  +{ts-t0:5.2f}s  {msg}")

def attendre_etat(moniteur, noeud, etat_cible, timeout=8.0) -> bool:
    """Attend qu'un nœud atteigne un état donné, avec timeout."""
    debut = time.time()
    while time.time() - debut < timeout:
        etats = moniteur.etat_cluster()
        if etats.get(noeud) == etat_cible:
            return True
        time.sleep(0.1)
    return False


# ─── SCÉNARIO 1 : CLUSTER SAIN ───────────────────────────────────────────────

def scenario_cluster_sain():
    titre("SCÉNARIO 1 — Cluster sain : heartbeats normaux")

    print("""
  3 nœuds envoient des heartbeats toutes les 0.3s avec
  une gigue de ±50ms (variabilité réseau simulée).
  On observe le timeout adaptatif se calibrer.
    """)

    evenements = []
    def log_evt(msg): evenements.append(msg)

    moniteur = MoniteurHeartbeat(
        nom="Master",
        intervalle_check=0.2,
        on_suspect     = lambda n: log_evt(f"⚠️  Suspect : {n}"),
        on_mort        = lambda n: log_evt(f"💀 Mort    : {n}"),
        on_resurrection= lambda n: log_evt(f"💚 Résurrection : {n}"),
    )
    moniteur.demarrer()

    noeuds = [
        NoeudReseau("Web-1",  moniteur, intervalle=0.3, gigue=0.05),
        NoeudReseau("Web-2",  moniteur, intervalle=0.3, gigue=0.05),
        NoeudReseau("DB-1",   moniteur, intervalle=0.3, gigue=0.05),
    ]
    for n in noeuds: n.demarrer()

    attendre(2.0, "Calibration des timeouts adaptatifs")
    print(moniteur.rapport())

    print("\n  Timeouts adaptatifs après calibration :")
    with moniteur._lock:
        for nom, noeud in moniteur._noeuds.items():
            moy = noeud.intervalle_moyen()
            to  = noeud.timeout_adaptatif()
            print(f"    {nom:<10} → intervalle moy={moy:.3f}s  timeout={to:.3f}s  (×{to/moy:.1f})")

    if evenements:
        print("\n  Alertes :", evenements)
    else:
        print("\n  ✅ Aucune alerte — cluster parfaitement sain")

    for n in noeuds: n.arreter()
    moniteur.arreter()


# ─── SCÉNARIO 2 : PANNE FRANCHE ──────────────────────────────────────────────

def scenario_panne_franche():
    titre("SCÉNARIO 2 — Panne franche : nœud qui s'arrête brutalement")

    print("""
  Séquence :
    t=0s   → 4 nœuds actifs, tout va bien
    t=1.5s → DB-Primary tombe (crash, coupure réseau…)
    t=?    → Moniteur le détecte (ALIVE → SUSPECT → DEAD)
    """)

    transitions = []

    def on_suspect(nom):
        transitions.append((time.time(), nom, "SUSPECT"))
        print(f"\n  🟡 t+{time.time()-t0:.2f}s  SUSPECT détecté : {nom}")

    def on_mort(nom):
        transitions.append((time.time(), nom, "DEAD"))
        print(f"\n  🔴 t+{time.time()-t0:.2f}s  MORT confirmé   : {nom}")

    moniteur = MoniteurHeartbeat(
        nom="Monitor",
        intervalle_check=0.15,
        on_suspect=on_suspect,
        on_mort=on_mort,
    )
    moniteur.demarrer()

    noeuds = {
        "API-1":       NoeudReseau("API-1",      moniteur, 0.3, 0.05),
        "API-2":       NoeudReseau("API-2",      moniteur, 0.3, 0.05),
        "DB-Primary":  NoeudReseau("DB-Primary", moniteur, 0.3, 0.05),
        "DB-Replica":  NoeudReseau("DB-Replica", moniteur, 0.3, 0.05),
    }
    for n in noeuds.values(): n.demarrer()

    attendre(1.2, "Calibration")
    t0 = time.time()

    print(f"\n  💥 t+0.00s  DB-Primary CRASH !")
    noeuds["DB-Primary"].tomber_en_panne()

    # Attendre la détection DEAD
    mort_detecte = attendre_etat(moniteur, "DB-Primary", EtatNoeud.DEAD, timeout=10)

    print(moniteur.rapport())

    if transitions:
        panne_t = next((t for t, n, e in transitions if n == "DB-Primary"), None)
        mort_t  = next((t for t, n, e in transitions if n == "DB-Primary" and e == "DEAD"), None)
        if panne_t and mort_t:
            print(f"\n  ⏱️  Délai détection SUSPECT : {panne_t - t0:.2f}s")
            print(f"  ⏱️  Délai détection DEAD    : {mort_t - t0:.2f}s")

    afficher_journal(moniteur, 10)

    for n in noeuds.values(): n.arreter()
    moniteur.arreter()


# ─── SCÉNARIO 3 : PANNE LENTE (DÉGRADATION) ─────────────────────────────────

def scenario_panne_lente():
    titre("SCÉNARIO 3 — Panne lente : nœud qui ralentit progressivement")

    print("""
  Cas réaliste : un nœud surchargé commence à répondre
  de plus en plus lentement avant de s'arrêter.
  Le timeout adaptatif doit absorber la variabilité.
    """)

    moniteur = MoniteurHeartbeat(
        nom="Monitor",
        intervalle_check=0.15,
        on_suspect     = lambda n: print(f"\n  🟡 SUSPECT : {n}"),
        on_mort        = lambda n: print(f"\n  🔴 MORT    : {n}"),
    )
    moniteur.demarrer()

    # Nœud normal
    stable = NoeudReseau("Cache-1", moniteur, intervalle=0.3, gigue=0.05)
    stable.demarrer()

    # Nœud qui va se dégrader
    lent = NoeudReseau("Cache-2", moniteur, intervalle=0.3, gigue=0.05)
    lent.demarrer()

    attendre(1.0, "Calibration initiale")

    print("\n  📉 Cache-2 commence à ralentir...")

    # Dégradation progressive en background
    def degrader():
        for delai in [0.5, 0.8, 1.2, 1.8]:
            time.sleep(0.4)
            lent.intervalle = delai
            lent.gigue = delai * 0.3
            print(f"  ⚠️  Cache-2 ralenti → intervalle={delai}s")
        time.sleep(0.5)
        print(f"  💥 Cache-2 tombe définitivement")
        lent.tomber_en_panne()

    t = threading.Thread(target=degrader, daemon=True)
    t.start()
    t.join()

    attendre(4.0, "Attente détection")
    print(moniteur.rapport())
    afficher_journal(moniteur, 12)

    stable.arreter()
    lent.arreter()
    moniteur.arreter()


# ─── SCÉNARIO 4 : RÉSURRECTION ────────────────────────────────────────────────

def scenario_resurrection():
    titre("SCÉNARIO 4 — Résurrection : nœud mort qui redémarre")

    print("""
  Un nœud tombe, est déclaré mort, puis redémarre.
  Le moniteur doit le passer de DEAD → ALIVE automatiquement.
  La séquence repart à 0 au redémarrage → heartbeats manqués détectés.
    """)

    transitions = []
    t0 = time.time()

    def evt(nom, etat):
        delta = time.time() - t0
        transitions.append((delta, nom, etat))
        icone = {"SUSPECT":"🟡","DEAD":"🔴","ALIVE":"💚"}[etat]
        print(f"\n  {icone} t+{delta:.2f}s  {etat} : {nom}")

    moniteur = MoniteurHeartbeat(
        nom="Monitor",
        intervalle_check=0.15,
        on_suspect     = lambda n: evt(n, "SUSPECT"),
        on_mort        = lambda n: evt(n, "DEAD"),
        on_resurrection= lambda n: evt(n, "ALIVE"),
    )
    moniteur.demarrer()

    noeud = NoeudReseau("Worker-1", moniteur, intervalle=0.3, gigue=0.04)
    noeud.demarrer()

    attendre(1.0, "Calibration")

    print(f"\n  💥 t+{time.time()-t0:.2f}s  Worker-1 CRASH")
    noeud.tomber_en_panne()
    attendre_etat(moniteur, "Worker-1", EtatNoeud.DEAD, timeout=8)

    attendre(0.5)
    print(f"\n  🔄 t+{time.time()-t0:.2f}s  Worker-1 REDÉMARRE")
    noeud.redemarrer()
    attendre_etat(moniteur, "Worker-1", EtatNoeud.ALIVE, timeout=5)

    attendre(0.5)
    print(moniteur.rapport())

    if len(transitions) >= 3:
        suspect_t, _, _ = transitions[0]
        mort_t, _, _    = transitions[1]
        resu_t, _, _    = transitions[2]
        print(f"""
  Timeline :
    t+{suspect_t:.2f}s  → SUSPECT (timeout dépassé)
    t+{mort_t:.2f}s   → DEAD    (2ème timeout)
    t+{resu_t:.2f}s   → ALIVE   (premier heartbeat reçu)
        """)

    afficher_journal(moniteur, 10)

    noeud.arreter()
    moniteur.arreter()


# ─── SCÉNARIO 5 : CASCADE DE PANNES ──────────────────────────────────────────

def scenario_cascade():
    titre("SCÉNARIO 5 — Tempête : pannes en cascade")

    print("""
  Simulation d'un incident majeur : les nœuds tombent
  les uns après les autres (surcharge, effet domino).
  Le moniteur doit traquer tous les états simultanément.
    """)

    morts = []
    t0 = time.time()

    moniteur = MoniteurHeartbeat(
        nom="Ops-Monitor",
        intervalle_check=0.15,
        on_mort = lambda n: morts.append((time.time()-t0, n)) or
                            print(f"  🔴 t+{time.time()-t0:.2f}s  MORT : {n}"),
    )
    moniteur.demarrer()

    cluster = {
        nom: NoeudReseau(nom, moniteur, intervalle=0.3, gigue=0.04)
        for nom in ["API-1","API-2","API-3","Worker-1","Worker-2","DB-Primary","DB-Replica","Cache"]
    }
    for n in cluster.values(): n.demarrer()

    attendre(1.2, "Cluster nominal")
    print(moniteur.rapport())

    # Cascade : un nœud tombe toutes les 0.8s
    sequence_pannes = ["DB-Primary", "Cache", "Worker-1", "API-1"]
    print("\n  ⚡ DÉBUT DE L'INCIDENT — pannes en cascade\n")

    for nom in sequence_pannes:
        time.sleep(0.6)
        print(f"  💥 t+{time.time()-t0:.2f}s  {nom} tombe")
        cluster[nom].tomber_en_panne()

    attendre(5.0, "Détection de toutes les pannes")

    print(moniteur.rapport())

    print(f"\n  📊 Résumé de l'incident :")
    for t_mort, nom in sorted(morts):
        print(f"    t+{t_mort:.2f}s  {nom} déclaré mort")

    vivants  = [n for n, e in moniteur.etat_cluster().items() if e == EtatNoeud.ALIVE]
    morts_l  = [n for n, e in moniteur.etat_cluster().items() if e == EtatNoeud.DEAD]
    print(f"\n  Vivants ({len(vivants)}) : {', '.join(vivants)}")
    print(f"  Morts   ({len(morts_l)}) : {', '.join(morts_l)}")
    print(f"\n  → En production : déclenchement automatique du failover")
    print(f"    DB-Replica prend le rôle de Primary, alertes PagerDuty envoyées.")

    for n in cluster.values(): n.arreter()
    moniteur.arreter()


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*60 + "╗")
    print("║   JOUR 4 — HEARTBEAT : DÉTECTION DE PANNES              ║")
    print("╚" + "═"*60 + "╝")

    scenario_cluster_sain()
    scenario_panne_franche()
    scenario_panne_lente()
    scenario_resurrection()
    scenario_cascade()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Machine à états d'un nœud :
    ALIVE → SUSPECT  : silence > timeout_adaptatif
    SUSPECT → DEAD   : silence > timeout_adaptatif × 2
    DEAD → ALIVE     : premier heartbeat reçu (résurrection)

  Timeout adaptatif :
    timeout = (moyenne_délais + écart_max) × facteur(3)
    ✅ S'auto-calibre selon la variabilité réseau réelle
    ✅ Moins de faux positifs qu'un timeout fixe

  Heartbeat contient :
    • Numéro de séquence  → détecte les pertes de messages
    • Métadonnées (CPU)   → diagnostic sans appel supplémentaire
    • Timestamp           → calcul de latence réseau

  En production :
    • Consul / etcd       → health checks HTTP + TTL
    • Kubernetes          → liveness + readiness probes
    • Zookeeper           → sessions avec TTL
    • TCP keepalive       → niveau OS (couche 4)

  Limite du heartbeat simple :
    ❌ Ne distingue pas "nœud mort" de "réseau partitionné"
    → Pour ça : Quorum + algorithmes de consensus (Jour 9, 10)

  → Jour 5 : Idempotence — envoyer la même requête 10 fois
    doit avoir le même effet qu'une seule fois.
  """)


if __name__ == "__main__":
    main()
