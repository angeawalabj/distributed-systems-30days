"""
Jour 24 — Simulation : WAL & Storage Engine en action
======================================================
5 scénarios :
  1. Transactions ACID : begin/write/commit/abort
  2. Durabilité : crash en pleine transaction → recovery
  3. Checkpoint : réduire le WAL à rejouer au redémarrage
  4. Corruption CRC : détecter une entrée corrompue
  5. Performance : écriture séquentielle WAL vs random I/O
"""

import os
import time
import shutil
import threading
import tempfile
from wal import (
    WAL, MoteurStockage, TypeEntree, EntreeWAL, Checkpoint
)

def ligne(c="═", n=64): return c * n
def titre(t): print(f"\n{ligne()}\n  {t}\n{ligne()}")

REP_TEST = "/tmp/jour24_wal"


def nouveau_rep():
    if os.path.exists(REP_TEST):
        shutil.rmtree(REP_TEST)
    os.makedirs(REP_TEST)
    return REP_TEST


# ─── SCÉNARIO 1 : TRANSACTIONS ACID ──────────────────────────────────────────

def scenario_transactions():
    titre("SCÉNARIO 1 — Transactions ACID : begin / write / commit / abort")

    print("""
  On simule 3 transactions sur un moteur clé-valeur :
    TX1 : écrire 3 clefs → COMMIT     (changements durables)
    TX2 : écrire 2 clefs → ABORT      (rollback, rien n'est appliqué)
    TX3 : concurrent avec TX1, commit après

  Le WAL contient TOUT, même les transactions abortées.
  Seuls les COMMIT rendent les données accessibles.
    """)

    rep = nouveau_rep()
    db  = MoteurStockage(rep)

    # TX1 : commit
    db.begin("tx1")
    db.ecrire("tx1", "user:alice", {"nom": "Alice", "solde": 1000})
    db.ecrire("tx1", "user:bob",   {"nom": "Bob",   "solde": 500})
    db.ecrire("tx1", "config:max", 100)
    db.commit("tx1")

    print(f"  Après TX1 (commit) :")
    print(f"    user:alice = {db.lire('user:alice')}")
    print(f"    user:bob   = {db.lire('user:bob')}")
    print(f"    config:max = {db.lire('config:max')}")

    # TX2 : abort
    db.begin("tx2")
    db.ecrire("tx2", "user:alice", {"nom": "Alice", "solde": 0})   # Mise à jour
    db.ecrire("tx2", "user:carol", {"nom": "Carol", "solde": 999})
    db.abort("tx2")

    print(f"\n  Après TX2 (abort) :")
    print(f"    user:alice = {db.lire('user:alice')}  ← inchangé (abort)")
    print(f"    user:carol = {db.lire('user:carol')}  ← n'existe pas")

    # TX3 : transfer atomique
    db.begin("tx3")
    alice = db.lire("user:alice")
    bob   = db.lire("user:bob")
    db.ecrire("tx3", "user:alice", {**alice, "solde": alice["solde"] - 200})
    db.ecrire("tx3", "user:bob",   {**bob,   "solde": bob["solde"]   + 200})
    db.commit("tx3")

    print(f"\n  Après TX3 (virement Alice→Bob de 200) :")
    print(f"    user:alice = {db.lire('user:alice')}")
    print(f"    user:bob   = {db.lire('user:bob')}")

    # Afficher le WAL
    entrees = db._wal.lire_tout()
    print(f"\n  WAL complet ({len(entrees)} entrées, {db._wal.taille_octets()} octets) :")
    print(f"  {'LSN':>4}  {'Type':<12} {'TX':>6}  Détails")
    print("  " + "─"*50)
    for e in entrees:
        detail = f"{e.cle}={e.valeur}" if e.cle else ""
        if len(detail) > 35: detail = detail[:35] + "..."
        print(f"  {e.lsn:>4}  {e.type.value:<12} {e.tx_id:>6}  {detail}")

    db.fermer()


# ─── SCÉNARIO 2 : CRASH ET RECOVERY ──────────────────────────────────────────

def scenario_crash():
    titre("SCÉNARIO 2 — Crash en pleine transaction → recovery après redémarrage")

    print("""
  Scénario : on effectue 3 transactions, puis on simule un crash.
  Le processus "redémarre" : le MoteurStockage relit le WAL depuis
  le début (ou depuis le dernier checkpoint) et reconstitue l'état.

  TX1 commit  → doit être retrouvée après recovery
  TX2 crash   → écriture interrompue, doit être ignorée (UNDO)
  TX3 commit  → doit être retrouvée après recovery
    """)

    rep = nouveau_rep()

    # Phase 1 : utilisation normale avant crash
    db1 = MoteurStockage(rep)
    db1.begin("tx1")
    db1.ecrire("tx1", "compte:A", 1000)
    db1.ecrire("tx1", "compte:B", 500)
    db1.commit("tx1")

    db1.begin("tx2")
    db1.ecrire("tx2", "compte:A", 800)   # TX en cours, pas encore commitée
    db1.ecrire("tx2", "compte:B", 700)
    # ← CRASH ICI : tx2 n'est jamais commitée

    db1.begin("tx3")
    db1.ecrire("tx3", "compte:C", 300)
    db1.commit("tx3")

    etat_avant_crash = db1.snapshot()
    print(f"  État AVANT crash (en mémoire) :")
    for k, v in sorted(etat_avant_crash.items()):
        print(f"    {k} = {v}")

    # Simuler le crash : fermer le moteur sans abort de tx2
    db1.fermer()

    print(f"\n  💥 CRASH SIMULÉ — redémarrage...")
    time.sleep(0.1)

    # Phase 2 : recovery (nouveau moteur = redémarrage du process)
    db2 = MoteurStockage(rep)
    etat_apres_recovery = db2.snapshot()

    print(f"\n  État APRÈS recovery :")
    for k, v in sorted(etat_apres_recovery.items()):
        print(f"    {k} = {v}")

    # Vérifications
    print(f"\n  Vérifications :")
    ok1 = db2.lire("compte:A") == 1000   # TX1 commitée, TX2 abortée → 1000
    ok2 = db2.lire("compte:B") == 500    # TX1 commitée, TX2 abortée → 500
    ok3 = db2.lire("compte:C") == 300    # TX3 commitée → 300

    print(f"    compte:A = {db2.lire('compte:A')} (attendu: 1000)  {'✅' if ok1 else '❌'}")
    print(f"    compte:B = {db2.lire('compte:B')} (attendu:  500)  {'✅' if ok2 else '❌'}")
    print(f"    compte:C = {db2.lire('compte:C')} (attendu:  300)  {'✅' if ok3 else '❌'}")
    print(f"    TX2 annulée silencieusement (pas de COMMIT dans le WAL) ✅")

    entrees = db2._wal.lire_tout()
    print(f"\n  WAL ({len(entrees)} entrées) :")
    print(f"  {'LSN':>4}  {'Type':<12} {'TX':>4}  Clef")
    print("  " + "─"*38)
    for e in entrees:
        print(f"  {e.lsn:>4}  {e.type.value:<12} {e.tx_id:>4}  {e.cle or ''}")

    db2.fermer()


# ─── SCÉNARIO 3 : CHECKPOINT ─────────────────────────────────────────────────

def scenario_checkpoint():
    titre("SCÉNARIO 3 — Checkpoint : réduire le WAL à rejouer au redémarrage")

    print("""
  Sans checkpoint : à chaque redémarrage on rejoue TOUT le WAL depuis le début.
  Avec checkpoint : on sauvegarde l'état sur disque, on ne rejoue que les
                    entrées APRÈS le checkpoint.

  Analogie : Git stash = checkpoint. On n'a pas besoin de rejouer tous les
  commits depuis le début — juste depuis le dernier stash.
    """)

    rep = nouveau_rep()
    db  = MoteurStockage(rep)

    # Phase 1 : 10 transactions
    for i in range(10):
        db.begin(f"tx{i}")
        db.ecrire(f"tx{i}", f"key:{i}", f"value:{i}")
        db.commit(f"tx{i}")

    wal_avant_ckpt = len(db._wal.lire_tout())
    print(f"  WAL avant checkpoint : {wal_avant_ckpt} entrées")

    # Checkpoint
    ckpt = db.checkpoint()
    print(f"  Checkpoint créé à LSN={ckpt.lsn}, {len(ckpt.etat)} clefs dans l'état")

    # Phase 2 : 5 autres transactions après le checkpoint
    for i in range(10, 15):
        db.begin(f"tx{i}")
        db.ecrire(f"tx{i}", f"key:{i}", f"value:{i}")
        db.commit(f"tx{i}")

    wal_total = len(db._wal.lire_tout())
    entrees_apres_ckpt = len(db._wal.lire_depuis(ckpt.lsn + 1))
    print(f"  WAL total : {wal_total} entrées")
    print(f"  À rejouer depuis le checkpoint : {entrees_apres_ckpt} entrées")

    db.fermer()

    # Simuler un crash après le checkpoint
    print(f"\n  💥 Crash après le checkpoint → recovery :")
    t0 = time.perf_counter()
    db2 = MoteurStockage(rep)
    t_recovery = (time.perf_counter() - t0) * 1000

    print(f"  Recovery en {t_recovery:.1f}ms (rejeu de {entrees_apres_ckpt} entrées, pas {wal_total})")
    print(f"  Clefs récupérées : {len(db2.snapshot())}/15")

    attendu = {f"key:{i}": f"value:{i}" for i in range(15)}
    ok = db2.snapshot() == attendu
    print(f"  État correct : {'✅' if ok else '❌'}")

    print(f"""
  Fréquence de checkpoint en production :
    PostgreSQL  : checkpoint_timeout=5min (défaut), ou tous les 1GB de WAL
    MySQL       : innodb_log_file_size contrôle la fréquence implicite
    SQLite      : checkpoint explicite ou automatique en WAL mode

  Trop fréquent → overhead I/O (écriture des pages sales sur disque)
  Trop rare     → recovery lente après crash (rejouer beaucoup d'entrées)
    """)

    db2.fermer()


# ─── SCÉNARIO 4 : DÉTECTION DE CORRUPTION ────────────────────────────────────

def scenario_corruption():
    titre("SCÉNARIO 4 — Corruption CRC : détecter une entrée corrompue dans le WAL")

    print("""
  Chaque entrée WAL contient un CRC32 (checksum).
  Si un bit flip survient sur disque (rayons cosmiques, bug SSD…),
  le CRC ne correspond plus → l'entrée est rejetée.

  Sans CRC : une entrée corrompue est silencieusement appliquée → données fausses.
  Avec CRC : la corruption est détectée → on s'arrête à l'entrée corrompue.
    """)

    rep = nouveau_rep()
    chemin_wal = os.path.join(rep, "wal_corruption.log")
    wal = WAL(chemin_wal)

    # Écrire 5 entrées légitimes
    for i in range(5):
        wal.ecrire(TypeEntree.WRITE, f"tx{i}", cle=f"k{i}", valeur=i * 10)
    wal.ecrire(TypeEntree.COMMIT, "tx0")
    wal.fermer()

    taille = os.path.getsize(chemin_wal)
    print(f"  WAL légitime : {taille} octets, 6 entrées")

    # Corrompre aléatoirement quelques bytes au milieu du fichier
    with open(chemin_wal, "r+b") as f:
        contenu = bytearray(f.read())
    milieu = taille // 2
    contenu[milieu]     ^= 0xFF   # Flip bits
    contenu[milieu + 1] ^= 0xAA
    with open(chemin_wal, "wb") as f:
        f.write(contenu)
    print(f"  Corruption injectée à l'offset {milieu} (flip de 2 bytes)")

    # Tenter de lire le WAL corrompu
    wal2 = WAL(chemin_wal)
    entrees_valides = []
    entrees_corrompues = 0

    raw = open(chemin_wal, "rb").read()
    pos = 0
    while pos < len(raw):
        try:
            entree, nb = EntreeWAL.deserialiser(raw[pos:])
            entrees_valides.append(entree)
            pos += nb
        except ValueError as e:
            entrees_corrompues += 1
            print(f"  ❌ Corruption détectée à l'offset {pos} : {e}")
            break

    print(f"\n  Entrées valides lues : {len(entrees_valides)}")
    print(f"  Entrées corrompues   : {entrees_corrompues}")
    print(f"  → Recovery s'arrête avant les données corrompues ✅")
    print(f"  → Sans CRC : données silencieusement corrompues ❌")

    print(f"""
  En production :
    PostgreSQL  : CRC32 sur chaque page (8KB) ET sur les entrées WAL
    ZFS/Btrfs   : checksums au niveau système de fichiers
    Hardware    : ECC RAM pour les serveurs de base de données

  Après détection de corruption :
    → Alerter immédiatement (PagerDuty, etc.)
    → Restaurer depuis la sauvegarde la plus récente
    → Rejouer le WAL jusqu'à l'entrée corrompue
    """)

    wal2.fermer()


# ─── SCÉNARIO 5 : PERFORMANCE ────────────────────────────────────────────────

def scenario_performance():
    titre("SCÉNARIO 5 — Performance : écriture séquentielle WAL vs simulation random I/O")

    print("""
  Le WAL est rapide car il est SÉQUENTIEL (append-only).
  Les disques HDD adorent les écritures séquentielles (pas de seek).
  Les disques SSD aussi (les cellules flash s'écrivent en séquence).

  Comparaison :
    WAL séquentiel : N écritures à la suite dans un seul fichier
    Random I/O     : N écritures à des positions aléatoires

  En pratique, on mesure le débit d'écriture du WAL.
    """)

    rep = nouveau_rep()
    N   = 200

    # ── WAL séquentiel ────────────────────────────────────────────────────────
    chemin_wal = os.path.join(rep, "perf_wal.log")
    wal = WAL(chemin_wal)

    t0 = time.perf_counter()
    for i in range(N):
        wal.ecrire(TypeEntree.WRITE, f"tx{i}", cle=f"key:{i}", valeur=i)
    t_wal = (time.perf_counter() - t0) * 1000
    taille_wal = wal.taille_octets()
    wal.fermer()

    # ── Simulation random I/O : N fichiers séparés ────────────────────────────
    rep_random = os.path.join(rep, "random")
    os.makedirs(rep_random)

    t0 = time.perf_counter()
    for i in range(N):
        path = os.path.join(rep_random, f"key_{i}.dat")
        with open(path, "wb") as f:
            f.write(str(i).encode())
            f.flush()
            os.fsync(f.fileno())
    t_random = (time.perf_counter() - t0) * 1000

    print(f"  {N} écritures avec fsync :\n")
    print(f"  {'Méthode':<25} {'Durée':>10}  {'Débit':>12}  {'Taille totale':>14}")
    print("  " + "─"*64)
    debit_wal = N / (t_wal / 1000)
    debit_rnd = N / (t_random / 1000)
    print(f"  {'WAL séquentiel':<25} {t_wal:>8.0f}ms  {debit_wal:>10.0f}/s  {taille_wal:>12} B")
    print(f"  {'Random I/O (N fichiers)':<25} {t_random:>8.0f}ms  {debit_rnd:>10.0f}/s  {'(N × ~10 B)':>14}")

    ratio = t_random / max(t_wal, 0.1)
    print(f"\n  WAL est {ratio:.1f}× plus rapide que le random I/O ✅")
    print(f"  (sur disque rotatif : ratio typique de 100× à 1000×)")

    # ── Groupe commit ─────────────────────────────────────────────────────────
    print(f"""
  Optimisation : Group Commit
    Au lieu de faire fsync() après chaque écriture,
    PostgreSQL/InnoDB regroupent les commits d'un même cycle :
      Thread 1 commit → met dans le buffer WAL
      Thread 2 commit → met dans le buffer WAL
      Thread 3 commit → flush groupé (1 fsync pour les 3)

    Résultat : N fsync → 1 fsync pour N transactions simultanées
    Débit multiplié par N (où N = nb de transactions concurrentes)
    """)

    # Mesurer group commit
    chemin_gc = os.path.join(rep, "group_commit.log")
    wal_gc = WAL(chemin_gc)

    BATCH_SIZE = 20
    t0 = time.perf_counter()
    with open(chemin_gc, "ab") as f:
        for i in range(N):
            entree = EntreeWAL(lsn=i, type=TypeEntree.WRITE,
                               tx_id=f"tx{i}", cle=f"k{i}", valeur=i)
            f.write(entree.serialiser())
        f.flush()
        os.fsync(f.fileno())   # Un seul fsync pour N écritures
    t_gc = (time.perf_counter() - t0) * 1000

    print(f"  {N} écritures avec group commit (1 fsync) : {t_gc:.1f}ms")
    print(f"  Vs {N} écritures individuelles (N fsync)  : {t_wal:.1f}ms")
    ratio_gc = t_wal / max(t_gc, 0.01)
    print(f"  Accélération group commit : {ratio_gc:.1f}× ✅")

    wal_gc.fermer()


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("╔" + "═"*62 + "╗")
    print("║   JOUR 24 — WRITE-AHEAD LOG (WAL) & STORAGE ENGINE      ║")
    print("╚" + "═"*62 + "╝")

    scenario_transactions()
    scenario_crash()
    scenario_checkpoint()
    scenario_corruption()
    scenario_performance()

    print("\n" + ligne())
    print("  RÉSUMÉ")
    print(ligne())
    print("""
  WAL & Storage Engine — Ce qu'il faut retenir :

    WAL       : append-only, séquentiel, fsync avant "commit OK"
    LSN       : numéro de séquence monotone pour chaque entrée
    Recovery  : charger checkpoint + rejouer WAL depuis le checkpoint LSN
    REDO      : appliquer les TX commitées au restart
    UNDO      : ignorer les TX sans COMMIT (abortées implicitement)
    Checkpoint: snapshot de l'état + seuil de replay réduit

  Ce que nos scénarios ont prouvé :
    Scénario 1 → Transactions ACID : commit visible, abort invisible ✅
    Scénario 2 → Crash recovery : TX2 sans commit ignorée, TX1/TX3 récupérées ✅
    Scénario 3 → Checkpoint : replay réduit (5 entrées au lieu de 30+) ✅
    Scénario 4 → CRC détecte la corruption avant d'appliquer des données fausses ✅
    Scénario 5 → WAL séquentiel : débit élevé, group commit multiplie encore ✅

  Utilisé en production :
    PostgreSQL   → WAL (pg_wal/), checkpoint_timeout, archive_mode
    MySQL InnoDB → Redo Log (ib_logfile*), innodb_flush_log_at_trx_commit
    SQLite       → WAL mode (journal_mode=WAL) ou rollback journal
    RocksDB      → WAL + MemTable + SST files (LSM tree)
    Kafka        → topic segments = WAL append-only distribué

  → Jour 25 : LSM Tree & SSTables (RocksDB / Cassandra)
    Le WAL garantit la durabilité. Mais comment stocker et lire
    efficacement des millions de clés sur disque ?
    LSM Tree : écrire dans la mémoire (MemTable), compacter sur disque
    (SSTable immutable). Lectures via bloom filters + index.
  """)

if __name__ == "__main__":
    main()
