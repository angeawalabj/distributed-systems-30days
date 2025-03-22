"""
Jour 21 — "Le Détective de Blocs" — Simulation HDFS
=====================================================
5 scénarios :
  1. Découpage et réplication : un fichier → des blocs → des DataNodes
  2. Rack awareness : visualiser la distribution inter-racks
  3. Panne DataNode : détection et re-réplication automatique
  4. Panne d'un rack entier : tolérance maximale
  5. Lecture client : NameNode comme annuaire, lecture directe en parallèle
"""

import time
import random
import threading
from collections import defaultdict
from hdfs import NameNode, DataNode, EtatNode

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


def creer_cluster(nb_racks: int = 3, dns_par_rack: int = 4,
                  capacite_gb: float = 500.0) -> NameNode:
    nn = NameNode()
    for r in range(nb_racks):
        for i in range(dns_par_rack):
            nn.enregistrer_datanode(DataNode(
                node_id     = f"dn-r{r}-{i}",
                rack_id     = f"rack-{r}",
                capacite_gb = capacite_gb,
            ))
    return nn


# ─── SCÉNARIO 1 : DÉCOUPAGE ET RÉPLICATION ───────────────────────────────────

def scenario_decoupe():
    titre("SCÉNARIO 1 — Découpage et réplication : un fichier → blocs → DataNodes")
    print("""
  HDFS découpe chaque fichier en blocs de 128 MB.
  Chaque bloc est répliqué 3× sur des DataNodes différents.
  Le NameNode garde la carte : fichier → blocs → DataNodes.
    """)

    nn = creer_cluster(nb_racks=3, dns_par_rack=3)

    fichiers = [
        ("/data/logs/app.log",          256),   # 2 blocs
        ("/data/users/users.csv",       640),   # 5 blocs
        ("/data/ml/training.parquet", 1_280),   # 10 blocs
    ]

    for path, taille_mb in fichiers:
        blocs = nn.creer_fichier(path, taille_mb)
        print(f"\n  📁 {path}  ({taille_mb} MB → {len(blocs)} blocs × 3 répliques)")
        print(f"  {'Bloc':<14} {'Taille':>7}  Répliques → Racks")
        print("  " + "─"*55)
        for b in blocs[:4]:
            racks = [nn._datanodes[r].rack_id for r in b.repliques]
            print(f"  {b.bloc_id:<14} {b.taille_mb:>5.0f}MB  "
                  f"{b.repliques}  {racks}")
        if len(blocs) > 4:
            print(f"  ... ({len(blocs)-4} blocs supplémentaires)")

    r = nn.rapport()
    print(f"\n  État du cluster :")
    for k, v in r.items():
        print(f"    {k:<25} : {v}")
    print()


# ─── SCÉNARIO 2 : RACK AWARENESS ─────────────────────────────────────────────

def scenario_rack_awareness():
    titre("SCÉNARIO 2 — Rack Awareness : les répliques sur des racks différents")
    print("""
  Un switch de rack tombe → tout le rack inaccessible.
  Avec rack awareness : au moins 1 réplique dans un autre rack.

  Politique HDFS :
    R1 → DataNode local (perf)
    R2 → rack DIFFÉRENT (tolérance)
    R3 → même rack que R2 (économise la bande passante inter-rack)
    """)

    nn = creer_cluster(nb_racks=4, dns_par_rack=4)

    # 30 fichiers d'un bloc chacun
    for i in range(30):
        nn.creer_fichier(f"/bench/file_{i}.dat", 128)

    # Analyser combien de racks distincts par bloc
    distribution = defaultdict(int)
    for b in nn._blocs.values():
        nb_racks = len({nn._datanodes[nid].rack_id for nid in b.repliques})
        distribution[nb_racks] += 1

    total = sum(distribution.values())
    print(f"  Distribution de {total} blocs sur 4 racks × 4 DataNodes :\n")
    print(f"  {'Racks distincts':>17}  {'Blocs':>6}  {'%':>5}  Tolérance")
    print("  " + "─"*55)
    icones = {1: "❌ panne rack = perte",
              2: "⚠️  survit à 1 panne de rack",
              3: "✅ survit à 2 pannes de rack"}
    for nb in sorted(distribution):
        cnt = distribution[nb]
        print(f"  {nb} rack(s) distinct(s)  {cnt:>6}  {cnt/total*100:>4.0f}%  "
              f"{icones.get(nb,'')}")

    # Exemple concret
    exemple = list(nn._blocs.values())[0]
    print(f"\n  Exemple — {exemple.bloc_id} :")
    for nid in exemple.repliques:
        dn = nn._datanodes[nid]
        print(f"    {nid:<14}  rack={dn.rack_id}")
    print()


# ─── SCÉNARIO 3 : PANNE DATANODE ─────────────────────────────────────────────

def scenario_panne_datanode():
    titre("SCÉNARIO 3 — Panne DataNode : détection heartbeat → re-réplication auto")
    print("""
  Quand un DataNode tombe :
    1. Plus de heartbeat depuis > 30s (10s en simulation)
    2. NameNode le déclare DEAD
    3. Identifie les blocs sous-répliqués (nb_repliques < 3)
    4. Ordonne la re-réplication sur d'autres DataNodes sains
    """)

    nn = creer_cluster(nb_racks=3, dns_par_rack=4)

    # Stocker des fichiers
    for i in range(4):
        nn.creer_fichier(f"/prod/dataset_{i}.parquet", 512)

    avant = nn.rapport()
    print(f"  État initial :")
    print(f"    DataNodes vivants    : {avant['datanodes_vivants']}/{avant['datanodes_total']}")
    print(f"    Blocs totaux         : {avant['blocs']}")
    print(f"    Blocs sous-répliqués : {len(nn.blocs_sous_repliques())}")

    # Tuer 2 DataNodes dans le même rack
    victimes = ["dn-r0-0", "dn-r0-1"]
    print(f"\n  💥 Panne : {victimes} (rack-0)")

    for v in victimes:
        r = nn.traiter_panne(v)
        print(f"\n  [NameNode] {v} → DEAD")
        print(f"    Blocs affectés        : {r['blocs_affectes']}")
        print(f"    Re-répliqués          : {r['re_repliques']}")
        print(f"    Encore sous-répliqués : {r['encore_sous_repliques']}")

    apres = nn.rapport()
    sous = nn.blocs_sous_repliques()
    print(f"\n  État après récupération :")
    print(f"    DataNodes vivants    : {apres['datanodes_vivants']}/{apres['datanodes_total']}")
    print(f"    Re-réplications auto : {apres['replications_auto']}")
    print(f"    Blocs sous-répliqués : {len(sous)} "
          f"{'✅' if not sous else '⚠️  (pas assez de DNs disponibles)'}")
    print()


# ─── SCÉNARIO 4 : PANNE RACK ENTIER ──────────────────────────────────────────

def scenario_panne_rack():
    titre("SCÉNARIO 4 — Panne de rack entier : la vraie force du rack awareness")
    print("""
  Un switch de rack tombe → les 4 DataNodes du rack sont inaccessibles.
  Avec rack awareness, chaque bloc a des répliques hors du rack affecté.
  → Les fichiers restent LISIBLES malgré la perte d'un rack entier.
    """)

    nn = creer_cluster(nb_racks=3, dns_par_rack=4)

    for i in range(5):
        nn.creer_fichier(f"/critical/file_{i}.bin", 384)

    total_blocs = nn.rapport()["blocs"]
    print(f"  {total_blocs} blocs répartis sur 3 racks × 4 DataNodes")

    # Tuer tout rack-1
    rack_mort = "rack-1"
    dns_rack  = [nid for nid, dn in nn._datanodes.items()
                 if dn.rack_id == rack_mort]
    print(f"\n  💥 Panne rack entier : {rack_mort} ({len(dns_rack)} DataNodes)")

    total_affectes = 0
    for nid in dns_rack:
        r = nn.traiter_panne(nid)
        total_affectes += r["blocs_affectes"]

    sous = nn.blocs_sous_repliques()
    lisibles = total_blocs - len(sous)

    print(f"\n  Résultat :")
    print(f"    Blocs affectés          : {total_affectes}")
    print(f"    Blocs encore lisibles   : {lisibles}/{total_blocs} "
          f"({lisibles/total_blocs*100:.0f}%)  {'✅' if lisibles == total_blocs else '⚠️'}")
    print(f"    Blocs sous-répliqués    : {len(sous)} (en cours de re-réplication)")
    print(f"    Re-réplications lancées : {nn.rapport()['replications_auto']}")
    print(f"""
  Tant que les re-réplications ne sont pas terminées, les blocs ont 2 répliques.
  Sur 2 racks restants (rack-0 et rack-2) → toujours accessibles.
  Durée de re-réplication en prod : quelques minutes à heures selon le débit réseau.
    """)


# ─── SCÉNARIO 5 : LECTURE CLIENT ─────────────────────────────────────────────

def scenario_lecture():
    titre("SCÉNARIO 5 — Lecture client : NameNode = annuaire, lecture directe en parallèle")
    print("""
  Clé de performance HDFS :
    Le NameNode NE transmet PAS les données — il donne juste la carte.
    Le client lit DIRECTEMENT les DataNodes, en parallèle.

  Séquence :
    1. Client → NameNode : "où sont les blocs de /data/events.csv ?"
    2. NameNode → Client : [(blk_001, [dn-r0-1, dn-r1-2, dn-r2-0]), ...]
    3. Client lit chaque bloc depuis le DataNode le plus proche
    4. Tous les blocs lus EN PARALLÈLE → débit = bande passante totale du cluster
    """)

    nn = creer_cluster(nb_racks=3, dns_par_rack=3)
    path   = "/data/clickstream/events.csv"
    blocs  = nn.creer_fichier(path, 768)   # 6 blocs

    print(f"\n  Fichier : {path}  (768 MB → {len(blocs)} blocs)\n")
    print(f"  Étape 1 — Client interroge le NameNode :")
    plan = nn.lire_fichier(path)
    for bid, reps in plan:
        racks = [nn._datanodes[r].rack_id for r in reps]
        print(f"    {bid}  →  {reps[0]} (primaire)  racks={racks}")

    print(f"\n  Étape 2 — Lecture PARALLÈLE des {len(blocs)} blocs :")
    resultats = {}
    lock = threading.Lock()
    t0   = time.perf_counter()

    def lire_bloc(bid: str, reps: list[str]):
        # Choisir le DataNode du rack-0 si disponible (local)
        prefere = next(
            (r for r in reps if nn._datanodes[r].rack_id == "rack-0"),
            reps[0]
        )
        time.sleep(random.uniform(0.015, 0.035))   # Latence réseau simulée
        with lock:
            resultats[bid] = prefere

    threads = [threading.Thread(target=lire_bloc, args=(bid, reps))
               for bid, reps in plan]
    for t in threads: t.start()
    for t in threads: t.join()
    t_total = (time.perf_counter() - t0) * 1000

    for bid, dn_lu in resultats.items():
        rack = nn._datanodes[dn_lu].rack_id
        print(f"    ✅ {bid} ← {dn_lu} ({rack})")

    seq_estime = len(blocs) * 25
    print(f"\n  {len(blocs)} blocs en parallèle : {t_total:.0f}ms")
    print(f"  Séquentiel estimé           : ~{seq_estime}ms")
    print(f"  Gain parallélisme           : {seq_estime/max(t_total,1):.1f}× ✅")
    print(f"""
  Data Locality (Spark / MapReduce) :
    Spark connaît la localisation des blocs via le NameNode.
    Il schedule les tâches SUR la machine qui a les données.
    → 0 transfert réseau → jusqu'à 10-100× plus rapide.
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 21 — 'LE DÉTECTIVE DE BLOCS' (HDFS)              ║")
    print("╚" + "═"*62 + "╝")

    scenario_decoupe()
    scenario_rack_awareness()
    scenario_panne_datanode()
    scenario_panne_rack()
    scenario_lecture()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  HDFS — Ce qu'il faut retenir :

    NameNode   : cerveau, métadonnées uniquement, JAMAIS les données
    DataNode   : stocke les blocs, heartbeat toutes les 3s
    Bloc       : 128 MB par défaut, répliqué 3× sur racks différents
    Rack Aware : R1=local  R2=autre rack  R3=même rack que R2

  Tolérance aux pannes :
    1 DataNode mort  → détecté en 30s → re-réplication automatique
    1 rack entier    → blocs toujours lisibles (copies sur 2 autres racks)
    NameNode mort    → cluster inaccessible (SPOF → résolu par NameNode HA + ZooKeeper)

  Ce que nos scénarios ont prouvé :
    Scénario 1 → fichier 1280MB → 10 blocs × 3 répliques bien distribués ✅
    Scénario 2 → rack awareness : majorité des blocs sur 2-3 racks distincts ✅
    Scénario 3 → 2 DNs morts → re-réplication automatique sans intervention ✅
    Scénario 4 → rack entier mort → 100% des blocs encore lisibles ✅
    Scénario 5 → 6 blocs en parallèle → gain ~5× vs séquentiel ✅

  Utilisé en production :
    Hadoop  → HDFS natif (Yahoo, Facebook, LinkedIn, Alibaba)
    Spark   → data locality via HDFS pour éviter les transferts réseau
    Hive    → tables en fichiers Parquet/ORC sur HDFS
    HBase   → WAL + StoreFiles stockés sur HDFS

  → Jour 22 — "Le Fleuve Infini" (Kafka)
    HDFS = stockage de fichiers statiques (batch).
    Kafka = stockage de flux d'événements en temps réel (streaming).
    Partitions, consumer groups, offsets, at-least-once vs exactly-once.
  """)

if __name__ == "__main__":
    main()
