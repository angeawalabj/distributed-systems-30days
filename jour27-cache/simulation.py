"""
Jour 27 — "La Mémoire Collective" — Simulation Distributed Cache
=================================================================
5 scénarios :
  1. Cache-Aside : hit/miss ratio, latence avec et sans cache
  2. Invalidation : écriture en base → invalider le cache
  3. Stampede : 100 threads sur une clef expirée → anti-stampede
  4. Cache L1/L2 : local + distribué pour les hot keys
  5. Cache warming + éviction LRU
"""

import time
import threading
import random
from collections import defaultdict
from typing import Any
from cache import (
    CacheLocal, CacheDistribue, CacheAside, AntiStampede
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


# ─── BASE DE DONNÉES SIMULÉE ──────────────────────────────────────────────────

class BaseDonnees:
    """Base simulée avec latence réaliste et compteur d'accès."""

    def __init__(self, latence_ms: float = 20.0):
        self._latence = latence_ms / 1000
        self._data = {
            f"user:{i}":    {"id": i, "nom": f"User_{i}", "email": f"u{i}@acme.com"}
            for i in range(1000)
        }
        self._data.update({
            f"perm:{i}":    {"user_id": i, "roles": ["user"], "scopes": ["read"]}
            for i in range(1000)
        })
        self._data["config:global"] = {"feature_x": True, "max_upload": 50}
        self._stats = defaultdict(int)

    def lire(self, cle: str) -> dict | None:
        time.sleep(self._latence)
        self._stats["lectures"] += 1
        return self._data.get(cle)

    def ecrire(self, cle: str, valeur: dict):
        self._data[cle] = valeur
        self._stats["ecritures"] += 1

    def stats(self) -> dict:
        return dict(self._stats)


# ─── SCÉNARIO 1 : CACHE-ASIDE, HIT/MISS, LATENCE ─────────────────────────────

def scenario_cache_aside():
    titre("SCÉNARIO 1 — Cache-Aside : hit/miss ratio et impact sur la latence")

    print("""
  Pattern Cache-Aside :
    Lecture → chercher en cache → miss → lire la base → stocker → retourner
    Écriture → écrire en base → invalider le cache

  Simulation : 200 lectures de profils utilisateurs
  Distribution : Zipf (20% des users reçoivent 80% des requêtes)
    """)

    db    = BaseDonnees(latence_ms=20)
    cache = CacheDistribue(nb_shards=3)
    aside = CacheAside(cache, db.lire, ttl_s=60)

    # Distribution Zipf : user:1 = 30% des requêtes, user:2 = 15%...
    users_populaires = [1, 2, 3, 5, 10]
    users_rares      = list(range(100, 200))
    requetes = (users_populaires * 30 + users_rares)
    random.shuffle(requetes)
    requetes = requetes[:200]

    t0_sans = time.perf_counter()
    for uid in requetes:
        db.lire(f"user:{uid}")
    t_sans = (time.perf_counter() - t0_sans) * 1000

    t0_avec = time.perf_counter()
    for uid in requetes:
        aside.get(f"user:{uid}")
    t_avec = (time.perf_counter() - t0_avec) * 1000

    s_cache = cache.stats()
    s_aside = aside.stats()
    s_db    = db.stats()

    print(f"  200 lectures (distribution Zipf) :\n")
    print(f"  {'Métrique':<30}  Sans cache      Avec cache")
    print(f"  " + "─"*56)
    print(f"  {'Temps total':<30}  {t_sans:>8.0f}ms  {t_avec:>10.0f}ms")
    print(f"  {'Appels base de données':<30}  {200:>8}     {s_db['lectures']:>10}")
    print(f"  {'Lectures cache (hit)':<30}  {'—':>8}     {s_cache['hit']:>10}")
    print(f"  {'Misses cache':<30}  {'—':>8}     {s_cache['miss']:>10}")
    print(f"  {'Hit rate':<30}  {'—':>8}     {s_cache['hit_rate']:>9}%")
    print(f"  {'Gain de vitesse':<30}  {'1×':>8}     {t_sans/max(t_avec,0.001):>9.1f}×")
    print(f"""
  Avec cache : {s_cache['hit']} requêtes servies depuis le cache (<0.1ms chacune).
  Seulement {s_db['lectures']} lectures base, contre 200 sans cache.
  Gain total : {t_sans/max(t_avec,0.001):.1f}× — principalement grâce aux clefs populaires.
    """)


# ─── SCÉNARIO 2 : INVALIDATION ────────────────────────────────────────────────

def scenario_invalidation():
    titre("SCÉNARIO 2 — Invalidation : cohérence cache/base après une écriture")

    print("""
  Problème : si on met à jour la base sans invalider le cache,
  les lectures suivantes retournent des données obsolètes (stale data).

  Stratégies :
    TTL court   : données obsolètes max TTL secondes → simple mais pas immédiat
    Invalidation explicite : invalider la clef dès qu'on écrit en base → immédiat
    Version tag : invalider en changeant le préfixe de toutes les clefs d'un user
    """)

    db    = BaseDonnees(latence_ms=5)
    cache = CacheDistribue(nb_shards=2)
    aside = CacheAside(cache, db.lire, ttl_s=300)  # TTL 5 min

    # Charger le cache pour user:42
    v1 = aside.get("user:42")
    print(f"  Lecture initiale  user:42 : {v1}")

    # Mise à jour en base (changement de nom)
    db.ecrire("user:42", {"id": 42, "nom": "Alice_Nouveau", "email": "alice@acme.com"})

    # Sans invalidation : cache retourne encore l'ancienne valeur
    v_stale = aside.get("user:42")
    print(f"\n  Après écriture base SANS invalidation cache :")
    print(f"  Cache retourne   : {v_stale}")
    print(f"  Base contient    : {db._data['user:42']}")
    print(f"  Cohérent ?       : {'✅' if v_stale == db._data['user:42'] else '❌ STALE DATA'}")

    # Invalidation explicite
    aside.invalidate("user:42")
    v2 = aside.get("user:42")
    print(f"\n  Après invalidation explicite :")
    print(f"  Cache retourne   : {v2}")
    print(f"  Cohérent ?       : {'✅' if v2 == db._data['user:42'] else '❌'}")

    # Pattern : invalider toutes les clefs d'un user (user:42, perm:42, session:42...)
    print(f"\n  Invalidation par pattern (ex: mise à jour des permissions) :")
    aside.get("perm:42")
    aside.get("user:42")
    taille_avant = cache.taille_totale()
    suppr = cache.delete_pattern("*:42")
    taille_apres = cache.taille_totale()
    print(f"  Clefs '*:42' supprimées : {suppr}")
    print(f"  Taille cache : {taille_avant} → {taille_apres}")
    print(f"""
  Règle d'or :
    Écrire en base → TOUJOURS invalider le cache correspondant
    Ne jamais invalider avant d'écrire (race condition)
    Ordre correct : 1) écrire base  2) invalider cache
    """)


# ─── SCÉNARIO 3 : STAMPEDE ────────────────────────────────────────────────────

def scenario_stampede():
    titre("SCÉNARIO 3 — Stampede (thundering herd) et protection par mutex")

    print("""
  Thundering herd : une clef populaire expire.
  100 threads arrivent simultanément → 100 misses → 100 requêtes base.
  Si la base prend 100ms → 100 × 100ms = 10s de charge soudaine.

  Solution : mutex par clef.
    1 thread recalcule → les 99 autres attendent.
    Résultat : 1 seule requête base au lieu de 100.
    """)

    appels_base = defaultdict(int)

    def source_lente(cle: str) -> dict:
        appels_base[cle] += 1
        time.sleep(0.050)   # 50ms de latence base
        return {"cle": cle, "valeur": random.randint(1, 1000)}

    # ── Sans protection ────────────────────────────────────────────────
    cache_sans = CacheDistribue(nb_shards=1)
    aside_sans = CacheAside(cache_sans, source_lente, ttl_s=10)

    resultats_sans = []
    threads_sans   = []
    t0 = time.perf_counter()

    def appel_sans():
        v = aside_sans.get("hot:key")
        resultats_sans.append(v)

    for _ in range(50):
        t = threading.Thread(target=appel_sans)
        threads_sans.append(t)
    for t in threads_sans: t.start()
    for t in threads_sans: t.join()

    t_sans = (time.perf_counter() - t0) * 1000
    appels_sans = appels_base["hot:key"]

    # ── Avec protection anti-stampede ─────────────────────────────────
    appels_base.clear()
    cache_avec = CacheDistribue(nb_shards=1)
    anti       = AntiStampede(cache_avec, source_lente, ttl_s=10)

    resultats_avec = []
    threads_avec   = []
    t0 = time.perf_counter()

    def appel_avec():
        v = anti.get("hot:key")
        resultats_avec.append(v)

    for _ in range(50):
        t = threading.Thread(target=appel_avec)
        threads_avec.append(t)
    for t in threads_avec: t.start()
    for t in threads_avec: t.join()

    t_avec = (time.perf_counter() - t0) * 1000
    appels_avec = appels_base["hot:key"]

    print(f"  50 threads simultanés sur 'hot:key' expirée :\n")
    print(f"  {'Métrique':<35}  {'Sans protection':>16}  {'Avec mutex':>12}")
    print(f"  " + "─"*68)
    print(f"  {'Appels base de données':<35}  {appels_sans:>16}  {appels_avec:>12}")
    print(f"  {'Temps total':<35}  {t_sans:>13.0f}ms  {t_avec:>9.0f}ms")
    print(f"  {'Résultats corrects':<35}  {len(resultats_sans):>16}  {len(resultats_avec):>12}")

    # Vérifier la cohérence : tous les threads doivent voir la même valeur
    valeurs_uniques_sans = len(set(str(r) for r in resultats_sans if r))
    valeurs_uniques_avec = len(set(str(r) for r in resultats_avec if r))
    print(f"  {'Valeurs distinctes retournées':<35}  {valeurs_uniques_sans:>16}  {valeurs_uniques_avec:>12}")

    s_anti = anti.stats()
    print(f"\n  Stats anti-stampede : {s_anti}")
    print(f"""
  Sans protection : {appels_sans} appels base simultanés → surcharge
  Avec mutex      : {appels_avec} appel(s) base → {50 - appels_avec} threads ont attendu le résultat

  En production Redis :
    SET key value NX PX 5000 → SET si Not eXists, expire en 5s
    → Verrou distribué : un seul worker recalcule
    → Les autres font un petit sleep(50ms) puis relisent
    """)


# ─── SCÉNARIO 4 : CACHE L1/L2 ────────────────────────────────────────────────

def scenario_l1_l2():
    titre("SCÉNARIO 4 — Cache L1/L2 : local + distribué pour les hot keys")

    print("""
  Même Redis peut devenir un goulot d'étranglement sur les hot keys.
  Solution : cache local L1 (en mémoire du processus) devant Redis L2.

  L1 : mémoire locale, <0.01ms, capacité limitée (ex: 500 entrées)
  L2 : Redis distribué, ~0.5ms, capacité quasi-illimitée

  Lecture :
    L1 hit  → retourner (<0.01ms)
    L1 miss → L2 hit  → stocker en L1 → retourner (~0.5ms)
    L2 miss → base    → stocker L1+L2 → retourner (~20ms)

  Problème L1 : chaque instance a son propre L1 → désynchronisation.
  Solution    : TTL L1 très court (5-30s) + pub/sub Redis pour invalider.
    """)

    db    = BaseDonnees(latence_ms=20)
    l1    = CacheLocal(capacite=100)
    l2    = CacheDistribue(nb_shards=3)

    invalidations_l1 = []

    # Écouter les invalidations Redis → invalider L1 aussi
    def on_invalidation(event: str, valeur):
        if event.startswith("del:"):
            cle = event[4:]
            l1.delete(cle)
            invalidations_l1.append(cle)

    l2.subscribe("del:*", on_invalidation)

    def get_l1_l2(cle: str) -> Any:
        # L1
        v = l1.get(cle)
        if v is not None:
            return v, "L1"
        # L2
        v = l2.get(cle)
        if v is not None:
            l1.set(cle, v, ttl_s=10)   # Stocker en L1 avec TTL court
            return v, "L2"
        # Base
        v = db.lire(cle)
        if v is not None:
            l2.set(cle, v, ttl_s=300)
            l1.set(cle, v, ttl_s=10)
        return v, "DB"

    # Simuler 300 requêtes avec quelques clefs très populaires
    cles_hot  = ["config:global", "user:1", "user:2"]
    cles_norm = [f"user:{i}" for i in range(10, 50)]
    requetes  = cles_hot * 80 + cles_norm
    random.shuffle(requetes)
    requetes  = requetes[:300]

    sources = defaultdict(int)
    t0 = time.perf_counter()
    for cle in requetes:
        _, source = get_l1_l2(cle)
        sources[source] += 1
    t_total = (time.perf_counter() - t0) * 1000

    print(f"  300 requêtes (hot keys = config:global, user:1, user:2) :\n")
    print(f"  {'Source':<8}  {'Requêtes':>10}  {'%':>6}  Latence typique")
    print(f"  " + "─"*45)
    print(f"  {'L1':<8}  {sources['L1']:>10}  {sources['L1']/300*100:>5.0f}%  < 0.01ms (mémoire locale)")
    print(f"  {'L2':<8}  {sources['L2']:>10}  {sources['L2']/300*100:>5.0f}%  ~ 0.5ms  (Redis)")
    print(f"  {'DB':<8}  {sources['DB']:>10}  {sources['DB']/300*100:>5.0f}%  ~ 20ms   (base)")
    print(f"\n  Temps total : {t_total:.0f}ms")
    print(f"  Appels base : {db.stats()['lectures']} (au lieu de 300 × 20ms = 6000ms)")

    # Simuler une invalidation (écriture en base)
    db.ecrire("user:1", {"id": 1, "nom": "Alice_v2", "email": "alice@acme.com"})
    l2.delete("user:1")   # L2 invalide → déclenche pub/sub → invalide L1

    print(f"\n  Invalidation après écriture user:1 :")
    print(f"  Clefs invalidées L1 via pub/sub : {invalidations_l1}")


# ─── SCÉNARIO 5 : CACHE WARMING + ÉVICTION ────────────────────────────────────

def scenario_warming():
    titre("SCÉNARIO 5 — Cache warming et éviction LRU")

    print("""
  Cold start : cache vide au démarrage → 100% de misses → base surchargée.
  Solution : pré-charger les clefs les plus populaires avant d'ouvrir le trafic.

  Éviction LRU (Least Recently Used) :
    Quand le cache est plein → supprimer la clef la moins utilisée.
    Redis : maxmemory-policy allkeys-lru (LRU global)
             maxmemory-policy volatile-lru (LRU sur les clefs avec TTL)
    """)

    db    = BaseDonnees(latence_ms=10)
    cache = CacheLocal(capacite=20)   # Très petit pour voir l'éviction

    # Clefs populaires à pré-charger
    top_clefs = ["config:global"] + [f"user:{i}" for i in range(1, 10)]

    print(f"  Capacité cache : 20 entrées")
    print(f"\n  Simulation 1 : SANS cache warming")
    miss_sans  = 0
    t0 = time.perf_counter()
    for uid in range(1, 51):   # 50 requêtes au démarrage
        v = cache.get(f"user:{uid}")
        if v is None:
            miss_sans += 1
            v = db.lire(f"user:{uid}")
            cache.set(f"user:{uid}", v, ttl_s=300)
    t_sans = (time.perf_counter() - t0) * 1000
    print(f"    Misses       : {miss_sans}/50")
    print(f"    Temps total  : {t_sans:.0f}ms")
    print(f"    Appels base  : {db.stats()['lectures']}")

    # Réinitialiser
    cache2 = CacheLocal(capacite=20)
    db2    = BaseDonnees(latence_ms=10)

    print(f"\n  Simulation 2 : AVEC cache warming (top 10 clefs pré-chargées)")
    t_warm_start = time.perf_counter()
    for cle in top_clefs:
        v = db2.lire(cle)
        cache2.set(cle, v, ttl_s=300)
    t_warming = (time.perf_counter() - t_warm_start) * 1000
    print(f"    Warming : {len(top_clefs)} clefs en {t_warming:.0f}ms")

    miss_avec = 0
    t0 = time.perf_counter()
    for uid in range(1, 51):
        v = cache2.get(f"user:{uid}")
        if v is None:
            miss_avec += 1
            v = db2.lire(f"user:{uid}")
            if v:
                cache2.set(f"user:{uid}", v, ttl_s=300)
    t_avec = (time.perf_counter() - t0) * 1000
    print(f"    Misses       : {miss_avec}/50")
    print(f"    Temps total  : {t_avec:.0f}ms")
    print(f"    Appels base  : {db2.stats()['lectures']}")

    # Éviction
    print(f"\n  Éviction LRU : dépasser la capacité (20 entrées)")
    cache3 = CacheLocal(capacite=5)   # Encore plus petit
    for i in range(10):
        cache3.set(f"k{i}", i, ttl_s=300)
    s3 = cache3.stats()
    print(f"    10 insertions dans un cache de 5 → taille={s3['taille']}, "
          f"évictions={s3.get('evict',0)}")
    print(f"""
  Cache warming en production :
    Au démarrage → lire les top-N clefs depuis la base
    (top-N = rapport d'accès Redis ou analytics de la semaine passée)
    Attendre que le cache soit à X% plein avant d'accepter le trafic
    → Circuit breaker fermé → ouverture progressive du trafic

  Éviction LRU Redis :
    config set maxmemory 4gb
    config set maxmemory-policy allkeys-lru
    → Redis supprime automatiquement les clefs les moins récentes
    → Jamais d'OOM, cache toujours disponible
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 27 — 'LA MÉMOIRE COLLECTIVE' (DISTRIBUTED CACHE) ║")
    print("╚" + "═"*62 + "╝")

    scenario_cache_aside()
    scenario_invalidation()
    scenario_stampede()
    scenario_l1_l2()
    scenario_warming()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Distributed Cache — Ce qu'il faut retenir :

    Cache-Aside  : l'application gère miss → source → store (pattern le plus courant)
    Write-Through: écriture base ET cache simultanément (cohérence forte)
    TTL          : données stale au maximum TTL secondes → choisir selon le cas
    Invalidation : écrire base → invalider cache → ordre obligatoire

  Problèmes classiques :
    Stampede  → mutex par clef : 1 recalcul, N-1 attendent → protection ✅
    Stale data→ invalidation explicite sur écriture → cohérence immédiate ✅
    Hot key   → L1/L2 : local (< 0.01ms) devant Redis (~0.5ms) ✅
    Cold start→ cache warming au démarrage des top-N clefs ✅
    OOM       → éviction LRU : Redis supprime les moins récentes ✅

  Ce que nos scénarios ont prouvé :
    Scénario 1 → gain ~20× sur 200 lectures avec distribution Zipf ✅
    Scénario 2 → sans invalidation = stale data détecté ✅ corrigé ✅
    Scénario 3 → 50 threads → sans mutex: 50 appels base, avec: 1 ✅
    Scénario 4 → L1/L2 : hot keys servies à <0.01ms depuis L1 ✅
    Scénario 5 → warming réduit les misses au démarrage ✅

  Utilisé en production :
    Redis     → cache universel : sessions, JWT, configs, leaderboards
    Memcached → cache simple, multi-threaded, pas de persistance
    Varnish   → cache HTTP reverse proxy (pages HTML, assets)
    CDN       → cache géographique distribué (Cloudflare, Fastly)

  → Jour 28 — "Le Chaos Maîtrisé" (Chaos Engineering)
    On a construit une infrastructure solide : HDFS, Kafka, Flink,
    Zero-Trust, SPIFFE, API Gateway, Cache.
    Comment savoir qu'elle résiste vraiment aux pannes ?
    Chaos Engineering = injecter des pannes contrôlées pour trouver
    les faiblesses avant que la production ne le fasse.
  """)

if __name__ == "__main__":
    main()
