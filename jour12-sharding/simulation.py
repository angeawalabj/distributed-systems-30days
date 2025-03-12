"""
Jour 12 — Simulation : Sharding en action
==========================================
5 scénarios :
  1. Hash vs Range vs Directory : comparaison des 3 stratégies
  2. Hotspot sur Range Sharding : les nouvelles inscriptions
  3. Resharding douloureux avec hash naïf vs Consistent Hash
  4. Range Query : pourquoi Range Sharding gagne
  5. Migration chirurgicale avec Directory Sharding
"""

import time
import random
import string
from sharding import (
    Shard, HashSharding, RangeSharding, DirectorySharding,
    ConsistentHashSharding, Plage
)

# ─── UTILITAIRES ─────────────────────────────────────────────────────────────

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")

def afficher_distribution(dist: dict, total: int, label: str = ""):
    if label: print(f"\n  {label}")
    if not dist or total == 0:
        return
    ideal   = total / len(dist)
    max_val = max(dist.values()) if dist.values() else 1
    for sid, nb in sorted(dist.items()):
        pct   = nb / total * 100
        barre = "█" * int(nb / max(max_val, 1) * 28)
        ecart = (nb - ideal) / ideal * 100 if ideal > 0 else 0
        signe = "+" if ecart >= 0 else ""
        print(f"    Shard-{sid}  {barre:<28} {nb:>6}  {pct:5.1f}%  ({signe}{ecart:.0f}%)")

def cv(dist: dict) -> float:
    """Coefficient de variation (mesure d'inégalité)."""
    vals = list(dist.values())
    if not vals: return 0
    moy = sum(vals) / len(vals)
    if moy == 0: return 0
    variance = sum((v - moy)**2 for v in vals) / len(vals)
    return (variance**0.5) / moy * 100

def generer_cles_users(n: int) -> list[str]:
    return [f"user:{random.randint(1, 10_000_000):08d}" for _ in range(n)]

def generer_cles_alpha(n: int) -> list[str]:
    """Clés alphabétiques pour le range sharding."""
    noms = ["alice", "bob", "carol", "david", "eve", "frank",
            "grace", "henry", "iris", "jack", "kate", "leo",
            "mia", "noah", "olivia", "paul", "quinn", "rachel",
            "sam", "tina", "uma", "victor", "wendy", "xena",
            "yves", "zara"]
    return [f"user:{random.choice(noms)}{random.randint(1000,9999)}"
            for _ in range(n)]


# ─── SCÉNARIO 1 : COMPARAISON DES 3 STRATÉGIES ───────────────────────────────

def scenario_comparaison():
    titre("SCÉNARIO 1 — Comparaison : Hash vs Range vs Directory Sharding")

    print("""
  On insère 3 000 enregistrements dans chaque stratégie (4 shards)
  et on mesure : distribution, latence, et capacité de range query.
    """)

    random.seed(42)
    N = 3_000
    cles  = [f"user:{i:06d}" for i in random.sample(range(100_000), N)]
    valeurs = {c: {"nom": f"User{c[5:]}", "score": random.randint(0,1000)} for c in cles}

    resultats = {}

    for nom, strategie_fn in [
        ("Hash",      lambda: HashSharding([Shard(i, latence_ms=2) for i in range(4)])),
        ("Range",     lambda: RangeSharding(
            [Shard(i, latence_ms=2) for i in range(4)],
            [Plage("", "user:025000", 0),
             Plage("user:025000", "user:050000", 1),
             Plage("user:050000", "user:075000", 2),
             Plage("user:075000", "", 3)],
        )),
        ("Directory", lambda: DirectorySharding([Shard(i, latence_ms=2) for i in range(4)])),
    ]:
        strategie = strategie_fn()
        t0 = time.perf_counter()
        for cle in cles:
            strategie.ecrire(cle, valeurs[cle])
        duree_w = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        for cle in cles[:200]:
            strategie.lire(cle)
        duree_r = (time.perf_counter() - t0) * 1000

        dist = strategie.distribution()
        resultats[nom] = {
            "strategie": strategie,
            "dist": dist,
            "cv": cv(dist),
            "duree_w": duree_w,
            "duree_r_200": duree_r,
        }

    print(f"  {'Stratégie':<12} {'CV%':>6}  {'Équilibre':<16} {'Écriture 3k':>12}  {'Lecture 200':>12}  {'Range Query'}")
    print("  " + "─"*75)
    for nom, r in resultats.items():
        equil = "✅ uniforme" if r["cv"] < 15 else ("⚠️  inégal" if r["cv"] < 40 else "❌ hotspot")
        range_q = "✅ efficace" if nom == "Range" else "❌ scan all"
        print(f"  {nom:<12} {r['cv']:>5.0f}%  {equil:<16} {r['duree_w']:>10.0f}ms  {r['duree_r_200']:>10.0f}ms  {range_q}")

    print()
    afficher_distribution(resultats["Hash"]["dist"],    N, "Hash Sharding :")
    afficher_distribution(resultats["Range"]["dist"],   N, "Range Sharding :")
    afficher_distribution(resultats["Directory"]["dist"], N, "Directory Sharding :")


# ─── SCÉNARIO 2 : HOTSPOT RANGE SHARDING ─────────────────────────────────────

def scenario_hotspot():
    titre("SCÉNARIO 2 — Hotspot : Range Sharding et les nouvelles inscriptions")

    print("""
  Range sharding par ID numérique croissant :
    Shard 0 : user:000001 – user:250000
    Shard 1 : user:250001 – user:500000
    ...

  Problème : tous les NOUVEAUX utilisateurs ont des IDs élevés
  → ils vont tous sur le dernier shard → HOTSPOT.

  Solution : utiliser des UUIDs ou des hash IDs plutôt que
  des IDs séquentiels, ou faire du range sharding sur
  un attribut mieux distribué (ex : date de naissance).
    """)

    shards = [Shard(i, latence_ms=1) for i in range(4)]
    strategie = RangeSharding(
        shards,
        [Plage("user:0000000", "user:0002500", 0),
         Plage("user:0002500", "user:0005000", 1),
         Plage("user:0005000", "user:0007500", 2),
         Plage("user:0007500", "", 3)],
    )

    # Simulation : nouvelles inscriptions (IDs croissants)
    print("  1000 nouvelles inscriptions (IDs séquentiels croissants) :\n")
    for i in range(9001, 10001):
        strategie.ecrire(f"user:{i:07d}", {"signup_ts": time.time()})

    dist = strategie.distribution()
    afficher_distribution(dist, 1000, "Distribution avec IDs séquentiels :")

    cv_val = cv(dist)
    print(f"\n  CV = {cv_val:.0f}% → {'❌ HOTSPOT sévère' if cv_val > 60 else '⚠️  inégal'}")
    print(f"""
  Shard-3 reçoit 100% du trafic d'écriture.
  Les shards 0, 1, 2 sont inactifs → ressources gâchées.
  En production : le disque du Shard-3 sature, les autres sont vides.

  Solutions :
    → Utiliser des UUIDs (distribution uniforme par hash)
    → Range sur une date hachée (ex : hash(date) % 4)
    → Consistent Hashing (Jour 11) qui évite ce problème naturellement
    """)


# ─── SCÉNARIO 3 : RESHARDING NAÏF vs CONSISTENT ──────────────────────────────

def scenario_resharding():
    titre("SCÉNARIO 3 — Resharding : hash naïf vs Consistent Hash")

    print("""
  On part de 3 shards avec 5 000 clés.
  On ajoute 1 shard (passage de 3 à 4).
  Combien de clés doivent migrer ?

  Hash naïf (modulo) : ~75% des clés bougent → migration catastrophique
  Consistent Hash    : ~25% des clés bougent → migration maîtrisée
    """)

    random.seed(1)
    K    = 5_000
    cles = [f"key:{random.randint(0, 10_000_000)}" for _ in range(K)]

    # Hash naïf
    shards_hash = [Shard(i, latence_ms=0) for i in range(3)]
    strat_hash  = HashSharding(shards_hash)
    for cle in cles:
        strat_hash.ecrire(cle, "v")

    n_hash, pct_hash = strat_hash.cles_reaffectees_si_ajout(cles)

    # Consistent Hash
    shards_ch = [Shard(i, latence_ms=0) for i in range(3)]
    strat_ch  = ConsistentHashSharding(shards_ch, vnodes=150)
    for cle in cles:
        strat_ch.ecrire(cle, "v")

    n_ch, pct_ch = strat_ch.cles_reaffectees_si_ajout(cles, nouveau_id=3)

    th_hash = (3/4) * 100   # (N-1)/N = 3/4
    th_ch   = (1/4) * 100   # 1/N

    print(f"\n  {'Méthode':<22} {'Clés migrées':>14}  {'%':>6}  {'Théorie':>8}")
    print("  " + "─"*56)
    print(f"  {'Hash naïf (modulo)':<22} {n_hash:>14}  {pct_hash:>5.1f}%  ~{th_hash:.0f}%")
    print(f"  {'Consistent Hashing':<22} {n_ch:>14}  {pct_ch:>5.1f}%  ~{th_ch:.0f}%")

    ratio = pct_hash / pct_ch if pct_ch > 0 else 0
    print(f"""
  Consistent Hashing migre {ratio:.1f}x moins de clés.

  Impact sur une base de 100 Go :
    Hash naïf   : ~75 Go à transférer → heures de downtime
    Consistent  : ~25 Go à transférer → migration en ligne possible

  C'est pourquoi Cassandra et DynamoDB utilisent
  le consistent hashing pour leur sharding.
    """)


# ─── SCÉNARIO 4 : RANGE QUERY ─────────────────────────────────────────────────

def scenario_range_query():
    titre("SCÉNARIO 4 — Range Query : Range Sharding gagne nettement")

    print("""
  Requête : "Tous les utilisateurs dont le nom commence par 'G' à 'M'"
  Avec Hash Sharding  → scanner les 4 shards (pas de localité)
  Avec Range Sharding → scanner seulement 1-2 shards ciblés
    """)

    random.seed(5)
    noms = ["Alice","Bob","Carol","Dave","Eve","Frank","Grace",
            "Henry","Iris","Jack","Kate","Leo","Mia","Noah",
            "Olivia","Paul","Quinn","Rachel","Sam","Tina"]
    N = 2_000
    cles_vals = {f"user:{nom}{i:04d}": {"nom": nom, "age": random.randint(18,80)}
                 for nom in noms for i in range(100)}
    cles = list(cles_vals.keys())

    # Hash sharding
    shards_h = [Shard(i, latence_ms=1) for i in range(4)]
    strat_h  = HashSharding(shards_h)
    for cle, val in cles_vals.items():
        strat_h.ecrire(cle, val)

    t0 = time.perf_counter()
    # Scan ALL shards (hash ne permet pas de cibler)
    resultats_h = []
    for shard in shards_h:
        resultats_h.extend(
            shard.scanner(lambda r: "user:G" <= r.cle <= "user:Nz")
        )
    t_hash = (time.perf_counter() - t0) * 1000

    # Range sharding
    shards_r = [Shard(i, latence_ms=1) for i in range(4)]
    strat_r  = RangeSharding(
        shards_r,
        [Plage("",         "user:F", 0),
         Plage("user:F",   "user:L", 1),
         Plage("user:L",   "user:R", 2),
         Plage("user:R",   "",       3)],
    )
    for cle, val in cles_vals.items():
        strat_r.ecrire(cle, val)

    t0 = time.perf_counter()
    resultats_r = strat_r.range_query("user:G", "user:Nz")
    t_range = (time.perf_counter() - t0) * 1000

    shards_scannes_h = 4
    shards_scannes_r = 2  # Seulement Shard-1 (F–L) et Shard-2 (L–R)

    print(f"  Requête : user:G* à user:M*\n")
    print(f"  {'Stratégie':<18} {'Shards scannés':>16}  {'Résultats':>10}  {'Latence':>10}")
    print("  " + "─"*58)
    print(f"  {'Hash Sharding':<18} {shards_scannes_h:>16}  {len(resultats_h):>10}  {t_hash:>8.1f}ms")
    print(f"  {'Range Sharding':<18} {shards_scannes_r:>16}  {len(resultats_r):>10}  {t_range:>8.1f}ms")

    print(f"""
  Range Sharding est {t_hash/t_range:.1f}x plus rapide pour cette requête.
  Sur un vrai cluster avec 100 shards et 10 To de données :
    Hash   : scanner 100 shards = 100x plus de I/O
    Range  : scanner 2 shards   = minimal

  C'est pourquoi HBase, Bigtable et CockroachDB utilisent
  le range sharding pour les données à accès séquentiel.
    """)


# ─── SCÉNARIO 5 : MIGRATION DIRECTORY ────────────────────────────────────────

def scenario_migration():
    titre("SCÉNARIO 5 — Migration chirurgicale avec Directory Sharding")

    print("""
  Shard-0 est surchargé (disque plein).
  On veut migrer certaines clés vers un nouveau Shard-3
  SANS downtime, SANS rehashing, SANS toucher aux autres clés.

  Avec Hash Sharding  → impossible sans reconfigurer tous les clients
  Avec Directory      → on met à jour le directory, c'est tout
    """)

    shards = [Shard(i, latence_ms=1) for i in range(3)]
    strat  = DirectorySharding(shards)

    # Remplir Shard-0 artificiellement
    cles_shard0 = [f"hotkey:{i:05d}" for i in range(600)]
    cles_autres = [f"normalkey:{i:05d}" for i in range(400)]

    for cle in cles_shard0:
        strat.ecrire(cle, f"valeur_{cle}")
        strat._directory[cle] = 0   # Forcer sur Shard-0

    for cle in cles_autres:
        strat.ecrire(cle, f"valeur_{cle}")

    # Synchroniser le store physique
    for cle in cles_shard0:
        shards[0].ecrire(cle, f"valeur_{cle}")

    print(f"  Distribution avant migration :")
    dist_avant = {
        0: len(cles_shard0),
        1: sum(1 for v in strat._directory.values() if v == 1),
        2: sum(1 for v in strat._directory.values() if v == 2),
    }
    afficher_distribution(dist_avant, 1000)

    # Ajouter Shard-3
    nouveau_shard = Shard(3, latence_ms=1)
    strat.shards[3] = nouveau_shard

    # Migrer 300 clés de Shard-0 vers Shard-3
    print(f"\n  Migration de 300 clés de Shard-0 → Shard-3...")
    t0 = time.perf_counter()
    migrees = 0
    for cle in cles_shard0[:300]:
        if strat.migrer(cle, 3):
            migrees += 1
    duree = (time.perf_counter() - t0) * 1000

    print(f"  {migrees} clés migrées en {duree:.0f}ms")

    dist_apres = {
        0: shards[0].nb_enregistrements(),
        1: shards[1].nb_enregistrements(),
        2: shards[2].nb_enregistrements(),
        3: nouveau_shard.nb_enregistrements(),
    }
    print(f"\n  Distribution après migration :")
    afficher_distribution(dist_apres, 1000)

    # Vérifier que les lectures fonctionnent toujours
    verif_ok = all(strat.lire(cle) is not None for cle in cles_shard0[:10])
    print(f"\n  Lectures post-migration : {'✅ transparentes' if verif_ok else '❌ erreurs'}")
    print(f"""
  Le directory a simplement mis à jour ses pointeurs.
  Les clients ne savent pas que les données ont bougé.
  Aucun rehashing, aucun downtime, aucun changement de config.

  Inconvénient : le directory est maintenant un SPOF.
  En prod → répliquer le directory avec Raft (Jour 7).
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 12 — SHARDING (PARTITIONNEMENT)                   ║")
    print("╚" + "═"*62 + "╝")

    scenario_comparaison()
    scenario_hotspot()
    scenario_resharding()
    scenario_range_query()
    scenario_migration()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Sharding — Les 3 stratégies :

    Hash Sharding      → distribution uniforme, range queries impossibles
    Range Sharding     → range queries O(shards touchés), risque hotspot
    Directory Sharding → flexible, migration chirurgicale, SPOF directory

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Hash CV<5% uniforme | Range CV>60% si IDs séquentiels
    Scénario 2 → Range + IDs croissants = 100% du trafic sur 1 shard
    Scénario 3 → Hash naïf : 75% des clés migrent | Consistent : ~25%
    Scénario 4 → Range query : 2 shards scannés vs 4 avec hash
    Scénario 5 → Migration 300 clés sans downtime avec directory

  Règle pratique :
    Accès par clé exacte       → Hash ou Consistent Hash
    Accès par plage (ORDER BY) → Range Sharding + clé choisie avec soin
    Migration fréquente         → Directory Sharding

  → Jour 13 : Réplication Multi-Maître
    Comment gérer les conflits quand deux utilisateurs modifient
    la même donnée sur deux shards/serveurs différents en même temps.
  """)


if __name__ == "__main__":
    main()
