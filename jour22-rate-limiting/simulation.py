"""
Jour 22 — Simulation : Rate Limiting en action
===============================================
5 scénarios :
  1. Comparaison des 4 algorithmes sur le même trafic
  2. Le bug Fixed Window : 2×limite en 2ε (burst en bordure de fenêtre)
  3. Token Bucket : burst autorisé puis lissage progressif
  4. Multi-tenant : chaque client a sa propre limite
  5. Headers HTTP 429 : Retry-After, X-RateLimit-*
"""

import time
import threading
import statistics
from collections import defaultdict
from rate_limiter import (
    FixedWindow, SlidingWindowLog, TokenBucket, LeakyBucket,
    RateLimiterMultiTenant, Resultat
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


# ─── SCÉNARIO 1 : COMPARAISON DES 4 ALGORITHMES ──────────────────────────────

def scenario_comparaison():
    titre("SCÉNARIO 1 — Comparaison des 4 algorithmes : même trafic, comportements différents")

    print("""
  Limite : 10 req / seconde pour tous.
  Trafic : 25 requêtes envoyées aussi vite que possible (burst).
  On mesure combien sont autorisées, refusées, et à quel moment.
    """)

    LIMITE   = 10
    FENETRE  = 1.0   # 1 seconde
    N        = 25

    algorithmes = [
        ("Fixed Window",    FixedWindow(LIMITE, FENETRE)),
        ("Sliding Log",     SlidingWindowLog(LIMITE, FENETRE)),
        ("Token Bucket",    TokenBucket(LIMITE, LIMITE)),   # recharge = limite/s
        ("Leaky Bucket",    LeakyBucket(LIMITE, LIMITE)),
    ]

    print(f"  {'Algorithme':<18} {'✅ OK':>6}  {'❌ Refus':>8}  Séquence des 25 premières décisions")
    print("  " + "─"*70)

    for nom, rl in algorithmes:
        resultats = []
        for _ in range(N):
            r = rl.autoriser()
            resultats.append("✅" if r.ok else "❌")

        ok     = resultats.count("✅")
        refus  = resultats.count("❌")
        seq    = " ".join(resultats[:15]) + "..."
        print(f"  {nom:<18} {ok:>6}  {refus:>8}  {seq}")

    print(f"""
  Observations :
    Fixed Window  : 10 OK puis 15 refus (compteur = 10, fenêtre pas encore reset)
    Sliding Log   : identique en burst immédiat (toutes dans la même seconde)
    Token Bucket  : 10 OK (seau plein au départ) puis 15 refus
    Leaky Bucket  : 10 OK puis 15 refus

  La vraie différence apparaît sur la BORDURE DE FENÊTRE (scénario 2)
  et sur le BURST PROGRESSIF avec Token Bucket (scénario 3).
    """)


# ─── SCÉNARIO 2 : BUG FIXED WINDOW ───────────────────────────────────────────

def scenario_fixed_window_bug():
    titre("SCÉNARIO 2 — Le bug Fixed Window : 2×limite en 2×epsilon secondes")

    print("""
  Fenêtre fixe : [0s──1s] [1s──2s] [2s──3s] ...

  Attaque :
    À t=0.95s : envoyer 10 requêtes (fin de fenêtre 1)  → 10 autorisées
    À t=1.05s : envoyer 10 requêtes (début fenêtre 2)   → 10 autorisées
    Total : 20 requêtes en 100ms, soit 2×la limite !

  Sliding Window Log : détecte le burst car la fenêtre [0.05s, 1.05s]
  contient les 20 requêtes → bloque les 10 dernières.
    """)

    LIMITE  = 10
    FW = FixedWindow(LIMITE, 1.0)
    SW = SlidingWindowLog(LIMITE, 1.0)

    # Attendre d'être à ~50ms de la fin d'une fenêtre
    # On simule en manipulant directement les timers internes
    # plutôt que d'attendre 1s

    # Simuler manuellement l'attaque
    print(f"  Simulation de l'attaque :")
    print(f"  {'Requête':>10}  {'Moment':>12}  {'Fixed Window':>14}  {'Sliding Log':>12}")
    print("  " + "─"*54)

    # Phase 1 : 10 requêtes à t=0 (fin de fenêtre simulée)
    ok_fw_1 = ok_sw_1 = 0
    for i in range(LIMITE):
        ok_fw_1 += 1 if FW.autoriser().ok else 0
        ok_sw_1 += 1 if SW.autoriser().ok else 0

    # Forcer le reset de la fenêtre fixe (simuler t > fenêtre)
    FW._debut = time.time() - FW.fenetre_s - 0.001  # expire immédiatement
    # Le sliding log garde les timestamps réels : les 10 requêtes sont toujours dans la fenêtre

    # Phase 2 : 10 requêtes après reset
    ok_fw_2 = ok_sw_2 = 0
    for i in range(LIMITE):
        ok_fw_2 += 1 if FW.autoriser().ok else 0
        ok_sw_2 += 1 if SW.autoriser().ok else 0

    print(f"  {'Lot 1 (fin fen.)':<14}  {'t = 0.99s':>12}  {ok_fw_1:>6} autorisées   {ok_sw_1:>6} autorisées")
    print(f"  {'Lot 2 (début fen)':<14}  {'t = 1.01s':>12}  {ok_fw_2:>6} autorisées   {ok_sw_2:>6} autorisées")
    print(f"  {'TOTAL':<14}  {'(~10ms)':>12}  {ok_fw_1+ok_fw_2:>6} autorisées   {ok_sw_1+ok_sw_2:>6} autorisées")
    print()
    print(f"  Fixed Window  : {ok_fw_1+ok_fw_2} req en ~10ms = {ok_fw_1+ok_fw_2}× la limite ❌")
    print(f"  Sliding Log   : {ok_sw_1+ok_sw_2} req en ~10ms = exactement la limite ✅")

    print(f"""
  C'est le "boundary burst attack" du Fixed Window.
  Solution : Sliding Window Counter (approximation de sliding log en O(1)) :
    count_approx = count_fenetre_precedente × (1 - elapsed/fenetre) + count_fenetre_courante
    Utilisé par Redis + Cloudflare pour approximer le sliding log à O(1) mémoire.
    """)


# ─── SCÉNARIO 3 : TOKEN BUCKET BURST ─────────────────────────────────────────

def scenario_token_bucket():
    titre("SCÉNARIO 3 — Token Bucket : burst initial puis lissage progressif")

    print("""
  Token Bucket : capacité=20, recharge=5/s
  Un développeur qui démarre son app : burst de 20 req immédiates (seau plein),
  puis environ 5 req/s en régime continu.

  C'est le comportement souhaité pour les APIs :
  permettre des pics légitimes tout en limitant le débit moyen.
    """)

    tb = TokenBucket(capacite=20, recharge_par_s=5.0)

    phases = [
        ("Burst initial",    20, 0.0),     # 20 req sans pause
        ("Pause 1s",          0, 1.0),
        ("Après 1s",          8, 0.0),     # 5 jetons rechargés
        ("Pause 2s",          0, 2.0),
        ("Après 2s",         15, 0.0),     # 10 nouveaux jetons
    ]

    print(f"  {'Phase':<22} {'Requêtes':>10}  {'✅ OK':>6}  {'❌ Refus':>8}  Jetons restants")
    print("  " + "─"*65)

    for label, n_req, pause in phases:
        if pause > 0:
            time.sleep(pause)
            print(f"  {'--- pause ' + str(pause) + 's ---':<22} {'':>10}  "
                  f"{'':>6}  {'':>8}  {tb.etat()['jetons']:.1f} jetons rechargés")
            continue

        ok = refus = 0
        for _ in range(n_req):
            r = tb.autoriser()
            if r.ok: ok += 1
            else:    refus += 1

        print(f"  {label:<22} {n_req:>10}  {ok:>6}  {refus:>8}  {tb.etat()['jetons']:.1f}")

    print(f"""
  Token Bucket en production :
    capacite   = burst maximum autorisé
    recharge/s = débit moyen autorisé

  Ex : GitHub API = 5000 req/heure, burst de 10 req/s
    → capacite=10, recharge=5000/3600=1.39/s

  Le client peut "économiser" ses jetons pour un burst :
    → Télécharger 100 objets d'un coup après 1min d'inactivité
    → Légalement prévu dans la spec du Token Bucket
    """)


# ─── SCÉNARIO 4 : MULTI-TENANT ────────────────────────────────────────────────

def scenario_multi_tenant():
    titre("SCÉNARIO 4 — Multi-tenant : chaque client a sa propre limite")

    print("""
  En production, chaque client (IP, API key, user_id) a sa propre limite.
  Un client abusif ne pénalise pas les autres.

  Plans tarifaires simulés :
    free      : 10 req/s, burst 10
    pro       : 100 req/s, burst 100
    enterprise: 1000 req/s, burst 1000
    """)

    plans = {
        "alice (free)":       RateLimiterMultiTenant(10,   10),
        "bob (pro)":          RateLimiterMultiTenant(100,  100),
        "corp (enterprise)":  RateLimiterMultiTenant(1000, 1000),
    }

    # Chaque client envoie un burst de requêtes
    demandes = {
        "alice (free)":      25,
        "bob (pro)":         150,
        "corp (enterprise)": 500,
    }

    print(f"\n  {'Client':<22} {'Demandes':>10}  {'✅ OK':>8}  {'❌ Refus':>8}  {'Taux OK':>8}")
    print("  " + "─"*62)

    for client, rl in plans.items():
        n = demandes[client]
        ok = refus = 0
        for _ in range(n):
            r = rl.autoriser(client)
            if r.ok: ok += 1
            else:    refus += 1
        taux = ok / n * 100
        print(f"  {client:<22} {n:>10}  {ok:>8}  {refus:>8}  {taux:>7.0f}%")

    print(f"""
  alice dépasse sa limite (free) → 15 requêtes bloquées
  bob dépasse sa limite (pro)    → 50 requêtes bloquées
  corp reste dans sa limite      → 100% autorisé

  Isolation garantie :
  Si alice fait 10 000 req/s, bob et corp ne voient rien.
  Chaque client a son propre Token Bucket en mémoire (ou Redis).
    """)


# ─── SCÉNARIO 5 : HEADERS HTTP 429 ───────────────────────────────────────────

def scenario_headers():
    titre("SCÉNARIO 5 — Headers HTTP : 429 Too Many Requests avec Retry-After")

    print("""
  Un bon rate limiter ne se contente pas de bloquer.
  Il communique au client :
    X-RateLimit-Limit    : la limite totale
    X-RateLimit-Remaining: requêtes restantes dans la fenêtre
    X-RateLimit-Reset    : timestamp UNIX de la prochaine fenêtre
    Retry-After          : secondes à attendre (sur 429)

  Le client peut ainsi adapter son comportement automatiquement.
    """)

    tb = TokenBucket(capacite=5, recharge_par_s=2.0)

    def simuler_requete(req_num: int) -> dict:
        r = tb.autoriser()
        now = time.time()
        if r.ok:
            return {
                "status":                   200,
                "X-RateLimit-Limit":        5,
                "X-RateLimit-Remaining":    r.restantes,
                "X-RateLimit-Reset":        int(now + r.retry_s),
                "X-RateLimit-Algorithm":    r.algo,
            }
        else:
            return {
                "status":                   429,
                "Retry-After":              round(r.retry_s, 2),
                "X-RateLimit-Limit":        5,
                "X-RateLimit-Remaining":    0,
                "X-RateLimit-Reset":        int(now + r.retry_s),
                "message":                  "Too Many Requests",
            }

    print(f"  Simulation de 8 requêtes (limite=5, recharge=2/s) :\n")
    print(f"  {'Req':>4}  {'Status':>6}  {'Remaining':>10}  {'Retry-After':>12}  Info")
    print("  " + "─"*58)

    for i in range(8):
        resp = simuler_requete(i + 1)
        status = resp["status"]
        remaining = resp.get("X-RateLimit-Remaining", "-")
        retry = resp.get("Retry-After", "-")
        info  = "✅ OK" if status == 200 else f"❌ 429 – attendre {retry}s"
        print(f"  {i+1:>4}  {status:>6}  {str(remaining):>10}  {str(retry):>12}  {info}")

    print(f"""
  Headers standardisés (draft IETF RateLimit Headers) :
    RateLimit-Limit     : politique applicable
    RateLimit-Remaining : quota restant
    RateLimit-Reset     : timestamp de réinitialisation

  Retry-After : utilisé par les clients bien élevés pour back-off automatique.
  Un client qui respecte Retry-After ne génère pas de charge inutile.

  Bonne pratique : retourner 429 AVANT d'être saturé (ex: à 80% de la limite)
  pour laisser de la marge aux pics de trafic légitimes.
    """)

    print(f"  Jetons restants dans le seau : {tb.etat()['jetons']:.2f}")
    print(f"  (après {8} requêtes avec capacité=5 et recharge=2/s)")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 22 — RATE LIMITING & THROTTLING                  ║")
    print("╚" + "═"*62 + "╝")

    scenario_comparaison()
    scenario_fixed_window_bug()
    scenario_token_bucket()
    scenario_multi_tenant()
    scenario_headers()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Rate Limiting — Ce qu'il faut retenir :

    Fixed Window   : O(1), simple, mais burst 2×limite en bordure de fenêtre
    Sliding Log    : O(N), précis, pas de burst, coûteux en mémoire
    Token Bucket   : O(1), bursts contrôlés, débit moyen garanti → LE STANDARD
    Leaky Bucket   : O(1), débit constant lissé → APIs critiques, Nginx

  Ce que nos scénarios ont prouvé :
    Scénario 1 → 4 algos, même limite, comportement identique en burst pur ✅
    Scénario 2 → Fixed Window : 20 req en 10ms (2×limite) ❌ Sliding : 10 ✅
    Scénario 3 → Token Bucket : burst 20 puis ~5/s en régime ✅
    Scénario 4 → Multi-tenant : alice bloquée, bob et corp isolés ✅
    Scénario 5 → 429 avec Retry-After : le client sait quand réessayer ✅

  En production :
    Redis + Lua  → Token Bucket distribué atomique (le plus courant)
    Nginx        → limit_req_zone (Leaky Bucket par IP)
    AWS APIGW    → Token Bucket par clef d'API
    Cloudflare   → Sliding Window Counter approximé (O(1) via Redis)
    Kong/Envoy   → Plugins configurables par route

  → Jour 23 : Backpressure & Queue Management
    Quand les requêtes arrivent plus vite qu'on peut les traiter,
    il faut décider : mettre en file (latence), shed load (rejeter),
    ou prioriser (SLA). Patterns : queue bounded, work stealing,
    rejection sampling avec load shedding.
  """)

if __name__ == "__main__":
    main()
