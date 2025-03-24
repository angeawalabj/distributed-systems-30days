"""
Jour 22 — "Le Fleuve Infini" — Simulation Kafka
================================================
5 scénarios :
  1. Partitionnement : ordre garanti par clef, round-robin sans clef
  2. Consumer Groups : scalabilité horizontale et rebalancing
  3. Consumer Lag : mesurer le retard d'un consommateur lent
  4. Replay depuis offset 0 : retraiter tout l'historique
  5. At-least-once vs exactly-once : sémantiques de livraison
"""

import time
import random
import threading
from collections import defaultdict
from kafka import KafkaBroker, Message

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")


# ─── SCÉNARIO 1 : PARTITIONNEMENT ────────────────────────────────────────────

def scenario_partitionnement():
    titre("SCÉNARIO 1 — Partitionnement : ordre par clef, équilibrage sans clef")

    print("""
  Avec clef  : hash(clef) % nb_partitions → même clef = même partition
               → Ordre garanti pour tous les messages d'un même utilisateur
  Sans clef  : round-robin → distribution équilibrée sur toutes les partitions
               → Pas d'ordre global, débit maximum
    """)

    broker = KafkaBroker()
    broker.creer_topic("orders", nb_partitions=4)

    # Avec clef : tous les messages d'un user → même partition
    users = ["alice", "bob", "carol", "diana", "eve"]
    print(f"  Production avec clef (user_id) — 3 messages par user :\n")
    for user in users:
        partitions_vues = set()
        for i in range(3):
            msg = broker.produire("orders", cle=user,
                                  valeur={"user": user, "montant": random.randint(10, 200)})
            partitions_vues.add(msg.partition)
        print(f"    user={user:<6} → partition {msg.partition} toujours "
              f"{'✅ (ordre garanti)' if len(partitions_vues) == 1 else '❌'}")

    stats = broker.stats_topic("orders")
    print(f"\n  Distribution des messages par partition :")
    for i, cnt in enumerate(stats["par_partition"]):
        barre = "█" * cnt
        print(f"    partition-{i} : {barre} ({cnt} messages)")

    # Sans clef : round-robin
    broker2 = KafkaBroker()
    broker2.creer_topic("events", nb_partitions=4)
    print(f"\n  Production SANS clef (round-robin) — 12 messages :\n")
    for i in range(12):
        broker2.produire("events", cle=None, valeur={"event": f"click_{i}"})
    stats2 = broker2.stats_topic("events")
    print(f"  Distribution après 12 messages sans clef :")
    for i, cnt in enumerate(stats2["par_partition"]):
        barre = "█" * cnt
        print(f"    partition-{i} : {barre} ({cnt} messages)")
    print(f"\n  Sans clef = distribution équilibrée ✅  (Round-robin)")
    print(f"  Avec clef = même partition pour le même user ✅  (Hash)")


# ─── SCÉNARIO 2 : CONSUMER GROUPS ────────────────────────────────────────────

def scenario_consumer_groups():
    titre("SCÉNARIO 2 — Consumer Groups : scalabilité et rebalancing")

    print("""
  Un Consumer Group se partage les partitions d'un topic.
  Règle : 1 partition = au maximum 1 consommateur dans un groupe.

  Conséquence :
    4 partitions + 2 consommateurs → 2 partitions / consommateur
    4 partitions + 4 consommateurs → 1 partition / consommateur
    4 partitions + 6 consommateurs → 2 consommateurs inactifs (gaspillage)
    """)

    broker = KafkaBroker()
    broker.creer_topic("transactions", nb_partitions=4)

    # Produire 40 messages
    for i in range(40):
        broker.produire("transactions", cle=f"user_{i%10}",
                        valeur={"tx_id": i, "montant": random.randint(5, 500)})

    # Tester différentes tailles de groupes
    configs = [
        ("group-A", ["c1", "c2"]),               # 2 consommateurs
        ("group-B", ["c1", "c2", "c3", "c4"]),   # 4 consommateurs
        ("group-C", ["c1", "c2", "c3", "c4", "c5", "c6"]),  # 6 consommateurs
    ]

    for group_id, membres in configs:
        g = broker.creer_consumer_group(group_id, "transactions")
        for m in membres:
            g.rejoindre(m)

        print(f"\n  {group_id} ({len(membres)} consommateurs, 4 partitions) :")
        for m in membres:
            parts = g.partitions_de(m)
            msgs  = g.consommer(m, max_messages=100)
            statut = "⚠️  inactif" if not parts else f"✅ {len(msgs)} messages"
            print(f"    {m} → partitions {parts}  {statut}")

    print(f"""
  Rebalancing — ajout d'un consommateur en cours de route :
    """)
    broker.creer_topic("live", nb_partitions=4)
    for i in range(20):
        broker.produire("live", cle=f"k{i}", valeur=i)

    g = broker.creer_consumer_group("group-live", "live")
    g.rejoindre("c1")
    g.rejoindre("c2")
    print(f"  Avant rebalancing  : c1={g.partitions_de('c1')}, c2={g.partitions_de('c2')}")

    g.rejoindre("c3")
    print(f"  Après ajout de c3  : c1={g.partitions_de('c1')}, "
          f"c2={g.partitions_de('c2')}, c3={g.partitions_de('c3')}")

    g.quitter("c2")
    print(f"  Après départ de c2 : c1={g.partitions_de('c1')}, "
          f"c3={g.partitions_de('c3')}")
    print(f"  → Partitions redistribuées automatiquement ✅")


# ─── SCÉNARIO 3 : CONSUMER LAG ───────────────────────────────────────────────

def scenario_lag():
    titre("SCÉNARIO 3 — Consumer Lag : mesurer le retard d'un consommateur")

    print("""
  Consumer Lag = log_end_offset - committed_offset
  Si le producteur écrit plus vite que le consommateur ne lit → lag augmente.
  Lag > 0 → traitement en retard → alertes en production.
    """)

    broker = KafkaBroker()
    broker.creer_topic("metrics", nb_partitions=2)

    # Produire des messages rapidement
    NB_MESSAGES = 100
    for i in range(NB_MESSAGES):
        broker.produire("metrics", cle=f"host_{i%5}",
                        valeur={"cpu": random.uniform(0, 100), "seq": i})

    g = broker.creer_consumer_group("monitoring", "metrics")
    g.rejoindre("consumer-1")

    print(f"\n  {NB_MESSAGES} messages produits, consommateur vient de démarrer :")
    print(f"  Lag initial : {g.lag()}")
    print(f"  Lag total   : {g.lag_total()} messages\n")

    # Consommer par petits lots
    print(f"  Traitement par lots de 10 :")
    print(f"  {'Lot':>4}  {'Lus':>5}  {'Lag total':>10}  {'Partitions'}")
    print(f"  " + "─"*42)

    lot = 0
    while g.lag_total() > 0:
        lot += 1
        msgs = g.consommer("consumer-1", max_messages=10)
        if not msgs:
            break
        # Commiter les offsets
        nouveaux_offsets = defaultdict(int)
        for m in msgs:
            nouveaux_offsets[m.partition] = max(
                nouveaux_offsets[m.partition], m.offset + 1
            )
        g.commiter_offsets("consumer-1", dict(nouveaux_offsets))
        lag_detail = g.lag()
        print(f"  {lot:>4}  {len(msgs):>5}  {g.lag_total():>10}  {lag_detail}")
        if lot > 20:
            break

    print(f"\n  Lag final : {g.lag_total()} ✅  (consommateur à jour)")
    print(f"""
  En production (Kafka + Prometheus) :
    kafka_consumer_group_lag → métrique Prometheus
    Alerte si lag > 10 000   → consommateur trop lent
    Solution : ajouter des instances dans le consumer group
              ou augmenter nb_partitions (puis rebalancer)
    """)


# ─── SCÉNARIO 4 : REPLAY ─────────────────────────────────────────────────────

def scenario_replay():
    titre("SCÉNARIO 4 — Replay : retraiter l'historique depuis offset 0")

    print("""
  Kafka garde les messages aussi longtemps que la rétention le permet.
  Un nouveau service peut rejoindre et lire depuis l'offset 0 :
  il voit TOUS les événements passés, comme s'il avait toujours été là.

  Cas d'usage :
    - Nouveau service analytique qui a besoin de l'historique complet
    - Bug dans un consommateur → rejouer depuis avant le bug
    - Construire une nouvelle vue matérialisée
    """)

    broker = KafkaBroker()
    broker.creer_topic("user-events", nb_partitions=3, retention_s=float("inf"))

    # Simuler 3 jours d'historique
    evenements = ["login", "click", "purchase", "logout", "signup"]
    nb_events   = 60
    for i in range(nb_events):
        broker.produire("user-events",
                        cle=f"user_{i % 10}",
                        valeur={"type": random.choice(evenements), "seq": i})

    print(f"  {nb_events} événements produits dans 'user-events'\n")

    # Service A : a déjà consommé tout le topic
    g_a = broker.creer_consumer_group("service-analytics", "user-events")
    g_a.rejoindre("worker-1")
    msgs_a = g_a.consommer("worker-1", max_messages=1000)
    g_a.commiter_offsets("worker-1",
                         {m.partition: m.offset + 1 for m in msgs_a})
    print(f"  service-analytics (existant) a traité {len(msgs_a)} messages")
    print(f"  Lag    : {g_a.lag_total()} ✅  (à jour)")

    # Service B : nouveau — il commence depuis offset 0 (défaut)
    g_b = broker.creer_consumer_group("service-ml", "user-events")
    g_b.rejoindre("ml-worker-1")
    msgs_b = g_b.consommer("ml-worker-1", max_messages=1000)
    print(f"\n  service-ml (NOUVEAU, repart de 0) :")
    print(f"  Messages reçus : {len(msgs_b)}/{nb_events} "
          f"({'✅ tout l\'historique' if len(msgs_b) == nb_events else '⚠️'})")

    # Compter les types d'événements reçus
    from collections import Counter
    types = Counter(m.valeur["type"] for m in msgs_b)
    print(f"  Breakdown par type : {dict(types)}")

    # Reset d'offset (rejouer depuis un point précis)
    print(f"\n  Reset d'offset : rejouer les 20 derniers messages")
    t_reset = broker._topics["user-events"]
    for pid in range(t_reset.nb_partitions):
        leo = t_reset.partitions[pid].log_end_offset()
        g_b._offsets[pid] = max(0, leo - 7)  # 7 derniers par partition ≈ 20 total

    msgs_replay = g_b.consommer("ml-worker-1", max_messages=1000)
    print(f"  Messages rejoués : {len(msgs_replay)} ✅")
    print(f"""
  Kafka comme "source de vérité" :
    Les messages sont immuables et ordonnés par partition.
    N'importe quel service peut rejoindre à n'importe quel moment.
    → Event Sourcing distribué à l'échelle (cf. Jour 19)
    """)


# ─── SCÉNARIO 5 : SÉMANTIQUES DE LIVRAISON ───────────────────────────────────

def scenario_semantiques():
    titre("SCÉNARIO 5 — Sémantiques de livraison : at-least-once vs exactly-once")

    print("""
  Trois sémantiques possibles :

  AT-MOST-ONCE :
    Commit l'offset AVANT de traiter → si crash pendant traitement
    → message perdu (jamais retraité)
    Utilisation : métriques approximatives, logs non-critiques

  AT-LEAST-ONCE (défaut Kafka) :
    Commit l'offset APRÈS traitement réussi → si crash PENDANT traitement
    → message retraité (possible doublon)
    Utilisation : avec idempotence côté consommateur (clef unique = safe)

  EXACTLY-ONCE :
    Transactions Kafka (producteur + consommateur atomiques)
    → ni perte ni doublon, même en cas de crash
    Coût : latence +2-5ms, débit -10-20%
    Utilisation : paiements, déduplication critique
    """)

    broker = KafkaBroker()
    broker.creer_topic("payments", nb_partitions=2)

    NB = 20
    for i in range(NB):
        broker.produire("payments", cle=f"pay_{i:03d}",
                        valeur={"payment_id": f"pay_{i:03d}", "montant": random.randint(10, 1000)})

    # Simuler AT-LEAST-ONCE avec crash
    print(f"\n  Simulation AT-LEAST-ONCE avec crash au milieu :")
    g = broker.creer_consumer_group("payment-processor", "payments")
    g.rejoindre("worker-1")

    traites  = set()
    doublons = 0
    crash_simule = False

    msgs = g.consommer("worker-1", max_messages=100)
    for i, m in enumerate(msgs):
        pid = m.payment_id if hasattr(m, 'payment_id') else m.valeur.get("payment_id")

        # Simuler crash après 8 messages (sans commit)
        if i == 8 and not crash_simule:
            crash_simule = True
            print(f"  💥 Crash après {i} messages — offset NON commité")
            break

        if pid in traites:
            doublons += 1
        traites.add(pid)

    # Reprise depuis le dernier offset commité (= 0 car pas de commit)
    msgs_reprise = g.consommer("worker-1", max_messages=100)
    for m in msgs_reprise:
        pid = m.valeur.get("payment_id")
        if pid in traites:
            doublons += 1
        traites.add(pid)
        nouveaux_offsets = {m.partition: m.offset + 1}
        g.commiter_offsets("worker-1", nouveaux_offsets)

    print(f"  Messages traités uniques : {len(traites)}/{NB}")
    print(f"  Doublons détectés        : {doublons}")
    print(f"  → At-least-once : pas de perte, doublons possibles ✅")

    # Exactly-once : avec déduplication par payment_id
    print(f"\n  Simulation EXACTLY-ONCE avec idempotence :")
    g2 = broker.creer_consumer_group("payment-processor-eo", "payments")
    g2.rejoindre("worker-1")
    traites_eo = set()   # Clef de déduplication
    doublons_eo = 0

    msgs2 = g2.consommer("worker-1", max_messages=100)
    for m in msgs2:
        pid = m.valeur.get("payment_id")
        if pid in traites_eo:
            doublons_eo += 1
            continue  # Idempotence : skip le doublon
        traites_eo.add(pid)
        g2.commiter_offsets("worker-1", {m.partition: m.offset + 1})

    print(f"  Messages traités uniques : {len(traites_eo)}/{NB}")
    print(f"  Doublons ignorés         : {doublons_eo}")
    print(f"  → Exactly-once simulé par idempotence ✅")

    print(f"""
  Tableau récapitulatif :
    {'Sémantique':<20} {'Perte?':>7} {'Doublon?':>10} {'Latence':>8}  Usage
    {'─'*62}
    {'At-most-once':<20} {'oui':>7} {'non':>10} {'min':>8}  métriques approx.
    {'At-least-once':<20} {'non':>7} {'possible':>10} {'normal':>8}  + idempotence côté conso
    {'Exactly-once':<20} {'non':>7} {'non':>10} {'+2-5ms':>8}  paiements, finance
    """)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 22 — 'LE FLEUVE INFINI' (KAFKA)                  ║")
    print("╚" + "═"*62 + "╝")

    scenario_partitionnement()
    scenario_consumer_groups()
    scenario_lag()
    scenario_replay()
    scenario_semantiques()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  Kafka — Ce qu'il faut retenir :

    Topic     : canal de messages nommé, append-only, immuable
    Partition : subdivision pour le parallélisme, ordonné par offset
    Clef      : hash(clef) % N → même clef = même partition = ordre garanti
    Offset    : position dans la partition, retenu par chaque consumer group
    Retention : 7 jours par défaut → possibilité de replay

  Consumer Group :
    N consommateurs se partagent les partitions (1 partition ≤ 1 conso)
    Ajouter des consommateurs = scaling horizontal (jusqu'à nb_partitions)
    Lag = log_end_offset - committed_offset → surveiller en prod

  Sémantiques :
    At-least-once → offset committé après traitement → doublons possibles
    Exactly-once  → transactions Kafka + idempotence → ni perte ni doublon

  Ce que nos scénarios ont prouvé :
    Scénario 1 → clef garantit la même partition (alice toujours → p2) ✅
    Scénario 2 → 4 partitions / 6 consommateurs → 2 consommateurs inactifs ✅
    Scénario 3 → lag 100 → 0 en 10 lots de 10 messages ✅
    Scénario 4 → nouveau service reçoit 100% de l'historique depuis offset 0 ✅
    Scénario 5 → crash at-least-once → doublons, idempotence = exactly-once ✅

  Utilisé en production :
    LinkedIn  → inventé Kafka (2011), traite 7 trillions de msgs/jour
    Netflix   → pipeline d'événements, logs, recommandations
    Uber      → surge pricing en temps réel, GPS tracking
    Airbnb    → détection de fraude, synchronisation de données

  → Jour 23 — "La Fenêtre du Temps" (Flink)
    Kafka transporte les événements.
    Flink les TRAITE en temps réel avec des fenêtres temporelles.
    Tumbling windows, sliding windows, session windows, exactly-once.
  """)

if __name__ == "__main__":
    main()
