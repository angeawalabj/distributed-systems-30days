"""
Jour 2 — Benchmark : JSON vs Protobuf
======================================
On mesure sur 10 000 messages :
  - Taille des données (octets)
  - Vitesse d'encodage (sérialisation)
  - Vitesse de décodage (désérialisation)
  - Impact de la complexité du message

Utilise notre Protobuf manuel du fichier précédent.
"""

import json
import time
import random
import string
import struct
from protobuf_manuel import encode_produit, decode_produit


# ─── DONNÉES DE TEST ──────────────────────────────────────────────────────────

CATEGORIES = ["Informatique", "Mobilier", "Audio", "Électronique", "Bureautique"]
ADJECTIFS  = ["Mécanique", "Ergonomique", "Sans-fil", "Premium", "Pro", "Compact"]
NOMS       = ["Clavier", "Souris", "Moniteur", "Casque", "Webcam", "Hub USB", "Dock"]

def generer_produit(i: int) -> dict:
    nom = f"{random.choice(ADJECTIFS)} {random.choice(NOMS)}"
    return {
        "id":        f"P{i:04d}",
        "nom":       nom,
        "prix":      round(random.uniform(9.99, 999.99), 2),
        "stock":     random.randint(0, 500),
        "categorie": random.choice(CATEGORIES),
    }


# ─── SÉRIALISEURS JSON ────────────────────────────────────────────────────────

def encode_json(produit: dict) -> bytes:
    return json.dumps(produit, ensure_ascii=False).encode("utf-8")

def decode_json(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))


# ─── UTILITAIRES DE MESURE ────────────────────────────────────────────────────

def mesurer_temps(fn, donnees, iterations=3):
    """Exécute fn sur chaque élément de donnees, retourne le temps moyen en ms."""
    meilleur = float("inf")
    resultats = None
    for _ in range(iterations):  # Plusieurs passes pour stabiliser
        debut = time.perf_counter()
        resultats = [fn(d) for d in donnees]
        fin = time.perf_counter()
        meilleur = min(meilleur, fin - debut)
    return meilleur * 1000, resultats  # en millisecondes


def afficher_comparaison(label, json_val, proto_val, unite=""):
    ratio = json_val / proto_val if proto_val else 0
    barre_json  = "█" * 40
    barre_proto = "█" * int(40 / ratio) if ratio > 1 else "█" * 40

    print(f"\n  {label}")
    print(f"  JSON   : {barre_json}  {json_val:.1f}{unite}")
    print(f"  Proto  : {barre_proto}  {proto_val:.1f}{unite}")
    print(f"  → Protobuf est {ratio:.1f}x plus {'compact' if unite == ' o' else 'rapide'}")


# ─── BENCHMARK PRINCIPAL ──────────────────────────────────────────────────────

def benchmark_taille(produits):
    print("\n" + "═" * 60)
    print("  BENCHMARK 1 — TAILLE DES MESSAGES")
    print("═" * 60)

    json_bytes  = [encode_json(p)    for p in produits]
    proto_bytes = [encode_produit(p) for p in produits]

    taille_json  = sum(len(b) for b in json_bytes)
    taille_proto = sum(len(b) for b in proto_bytes)

    print(f"\n  Sur {len(produits):,} messages :")
    print(f"  JSON total  : {taille_json:>10,} octets  ({taille_json/1024:.1f} KB)")
    print(f"  Proto total : {taille_proto:>10,} octets  ({taille_proto/1024:.1f} KB)")
    print(f"  Économie    : {taille_json - taille_proto:>10,} octets  ({(1 - taille_proto/taille_json)*100:.1f}% plus léger)")

    # Détail message par message
    tailles_json  = [len(b) for b in json_bytes]
    tailles_proto = [len(b) for b in proto_bytes]
    print(f"\n  Taille moyenne par message :")
    print(f"  JSON  : {sum(tailles_json)/len(tailles_json):.1f} octets")
    print(f"  Proto : {sum(tailles_proto)/len(tailles_proto):.1f} octets")

    # Exemples concrets
    print(f"\n  Exemples de messages encodés :")
    for i in range(3):
        p = produits[i]
        j = json_bytes[i]
        pb = proto_bytes[i]
        print(f"\n  [{p['id']}] {p['nom']} — {p['prix']}€")
        print(f"    JSON  ({len(j):2d} o): {j.decode()}")
        print(f"    Proto ({len(pb):2d} o): {' '.join(f'{b:02X}' for b in pb)}")

    return json_bytes, proto_bytes


def benchmark_vitesse(produits, json_bytes, proto_bytes):
    print("\n" + "═" * 60)
    print("  BENCHMARK 2 — VITESSE D'ENCODAGE")
    print("═" * 60)

    t_json_enc,  _ = mesurer_temps(encode_json,    produits)
    t_proto_enc, _ = mesurer_temps(encode_produit, produits)

    print(f"\n  Encodage de {len(produits):,} messages :")
    print(f"  JSON   : {t_json_enc:7.2f} ms")
    print(f"  Proto  : {t_proto_enc:7.2f} ms  (notre impl. Python manuelle, non optimisée)")
    print(f"  → Note : le vrai Protobuf en C serait 10-50x plus rapide encore")

    print("\n" + "═" * 60)
    print("  BENCHMARK 3 — VITESSE DE DÉCODAGE")
    print("═" * 60)

    t_json_dec,  _ = mesurer_temps(decode_json,    json_bytes)
    t_proto_dec, _ = mesurer_temps(decode_produit, proto_bytes)

    print(f"\n  Décodage de {len(produits):,} messages :")
    print(f"  JSON   : {t_json_dec:7.2f} ms")
    print(f"  Proto  : {t_proto_dec:7.2f} ms  (notre impl. Python manuelle)")

    return t_json_enc, t_proto_enc, t_json_dec, t_proto_dec


def benchmark_impact_champs_vides(n=1000):
    """
    Protobuf n'encode pas les champs à valeur par défaut (0, "").
    Cet avantage explose avec les messages partiels.
    """
    print("\n" + "═" * 60)
    print("  BENCHMARK 4 — CHAMPS VIDES / PARTIELS")
    print("═" * 60)

    # Message complet
    complet = {"id": "P001", "nom": "Clavier Mécanique", "prix": 89.99, "stock": 42, "categorie": "Informatique"}
    # Message minimal (seulement l'ID)
    minimal = {"id": "P001", "nom": "", "prix": 0, "stock": 0, "categorie": ""}

    for label, msg in [("Complet (5 champs)", complet), ("Minimal (1 champ)", minimal)]:
        j  = encode_json(msg)
        pb = encode_produit(msg)
        print(f"\n  {label} :")
        print(f"    JSON  : {len(j):3d} octets → {j.decode()}")
        print(f"    Proto : {len(pb):3d} octets → {' '.join(f'{b:02X}' for b in pb)}")
        if pb:
            print(f"    Ratio : Proto est {len(j)/len(pb):.1f}x plus compact")
        else:
            print(f"    Proto : 0 octets ! (tous les champs = valeurs par défaut)")


def benchmark_volume_reseau(n_messages, taille_json_moy, taille_proto_moy):
    """Projection sur des volumes réels de production."""
    print("\n" + "═" * 60)
    print("  BENCHMARK 5 — PROJECTION PRODUCTION")
    print("═" * 60)

    scenarios = [
        ("API moyenne",       1_000),
        ("API populaire",   100_000),
        ("Netflix/Google", 1_000_000),
    ]

    print(f"\n  {'Scénario':<20} {'JSON/s (MB)':<15} {'Proto/s (MB)':<15} {'Économie/s'}")
    print("  " + "─" * 65)

    for label, req_par_sec in scenarios:
        json_mb  = req_par_sec * taille_json_moy  / 1024 / 1024
        proto_mb = req_par_sec * taille_proto_moy / 1024 / 1024
        economie = json_mb - proto_mb
        print(f"  {label:<20} {json_mb:<15.1f} {proto_mb:<15.1f} -{economie:.1f} MB/s")

    print(f"\n  À 1M req/s, Protobuf économise ~{(taille_json_moy - taille_proto_moy) * 1_000_000 / 1024 / 1024:.0f} MB/s de bande passante")
    print("  = des millions de dollars d'infrastructure par an à l'échelle Google")


# ─── ANALYSE DES FIELD TAGS ──────────────────────────────────────────────────

def analyser_format_binaire():
    print("\n" + "═" * 60)
    print("  ANALYSE — ANATOMIE D'UN MESSAGE PROTOBUF")
    print("═" * 60)

    produit = {"id": "P001", "nom": "Clavier", "prix": 89.99, "stock": 42, "categorie": "Info"}
    encoded = encode_produit(produit)

    print(f"\n  Message : {produit}")
    print(f"\n  Binaire ({len(encoded)} octets) :")
    print("  " + " ".join(f"{b:02X}" for b in encoded))
    print()

    annotations = [
        ("0A",       "Tag: field=1 (id), type=LENGTH_DELIMITED"),
        ("04",       "Longueur: 4 octets"),
        ("50303031", "UTF-8: 'P001'"),
        ("12",       "Tag: field=2 (nom), type=LENGTH_DELIMITED"),
        ("07",       "Longueur: 7 octets"),
        ("436C6176696572", "UTF-8: 'Clavier'"),
        ("1D",       "Tag: field=3 (prix), type=32BIT"),
        ("E1FAB342", "IEEE 754 float: 89.99"),
        ("20",       "Tag: field=4 (stock), type=VARINT"),
        ("2A",       "VarInt: 42"),
        ("2A",       "Tag: field=5 (categorie), type=LENGTH_DELIMITED"),
        ("04",       "Longueur: 4 octets"),
        ("496E666F", "UTF-8: 'Info'"),
    ]

    print("  Décomposition octet par octet :")
    for hex_val, desc in annotations:
        print(f"    {hex_val:<16} ← {desc}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    N = 10_000
    random.seed(42)

    print("╔" + "═" * 58 + "╗")
    print("║   JOUR 2 — BENCHMARK : JSON vs PROTOBUF (BINAIRE)       ║")
    print("║   Sur {:,} messages générés aléatoirement              ║".format(N))
    print("╚" + "═" * 58 + "╝")

    # Génération des données
    print(f"\n  Génération de {N:,} produits...")
    produits = [generer_produit(i) for i in range(N)]

    # Benchmarks
    json_bytes, proto_bytes = benchmark_taille(produits)
    t_je, t_pe, t_jd, t_pd = benchmark_vitesse(produits, json_bytes, proto_bytes)
    benchmark_impact_champs_vides()

    # Projection
    taille_json_moy  = sum(len(b) for b in json_bytes)  / N
    taille_proto_moy = sum(len(b) for b in proto_bytes) / N
    benchmark_volume_reseau(N, taille_json_moy, taille_proto_moy)

    # Anatomie binaire
    analyser_format_binaire()

    # Résumé
    print("\n" + "═" * 60)
    print("  RÉSUMÉ")
    print("═" * 60)
    print(f"""
  Taille    : Protobuf est {taille_json_moy/taille_proto_moy:.1f}x plus compact que JSON
  Encodage  : mesures sur implémentation Python manuelle
  Décodage  : mesures sur implémentation Python manuelle

  ⚠️  Note importante :
  Notre implémentation est en Python pur (pédagogique).
  La vraie lib Protobuf utilise du C/C++ natif et est
  10 à 50x plus rapide encore.

  Quand utiliser quoi ?
  ┌──────────────┬────────────────────────────────────┐
  │ JSON         │ APIs publiques, débogage, config   │
  │ Protobuf     │ Services internes, IoT, temps réel │
  │ MessagePack  │ JSON binaire, compromis lisible    │
  │ Avro         │ Big Data, Kafka, compatibilité     │
  └──────────────┴────────────────────────────────────┘

  → Demain Jour 3 : Lamport Timestamps — comment ordonner
    des événements quand les horloges des machines divergent.
    """)


if __name__ == "__main__":
    main()
