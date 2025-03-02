"""
Jour 2 — Protobuf Manuel : comprendre l'encodage binaire
=========================================================
On implémente un sous-ensemble de Protobuf à la main pour
voir EXACTEMENT comment les données sont compressées.

Le vrai grpcio/protobuf fait la même chose, mais généré.
"""

import struct


# ─── TYPES DE WIRE (le "type" encodé dans chaque field tag) ──────────────────
WIRE_VARINT  = 0   # int32, int64, bool, enum
WIRE_64BIT   = 1   # fixed64, double
WIRE_LENGTH  = 2   # string, bytes, embedded messages, repeated
WIRE_32BIT   = 5   # fixed32, float


def encode_varint(value: int) -> bytes:
    """
    Variable-length integer encoding (VarInt).
    Principe : on n'utilise que les bits nécessaires.
    
    Ex: 1   → 0x01        (1 octet)
        127 → 0x7F        (1 octet)
        128 → 0x80 0x01   (2 octets !)
        300 → 0xAC 0x02   (2 octets)
    
    C'est là que Protobuf gagne : les petits entiers prennent peu de place.
    JSON encode "42" en 2 octets de texte UTF-8 + guillemets potentiels.
    """
    result = bytearray()
    while True:
        bits = value & 0x7F          # On prend 7 bits
        value >>= 7
        if value:
            result.append(bits | 0x80)  # Bit de continuation = 1
        else:
            result.append(bits)          # Dernier groupe, bit de continuation = 0
            break
    return bytes(result)


def encode_field(field_number: int, wire_type: int, data: bytes) -> bytes:
    """
    Encode un champ Protobuf : [tag][données]
    tag = (field_number << 3) | wire_type
    
    Exemple : field 1, type string → tag = (1 << 3) | 2 = 0x0A
    """
    tag = (field_number << 3) | wire_type
    return encode_varint(tag) + data


def encode_string(value: str) -> bytes:
    """String → [varint longueur][octets UTF-8]"""
    encoded = value.encode("utf-8")
    return encode_varint(len(encoded)) + encoded


def encode_float(value: float) -> bytes:
    """Float 32 bits → 4 octets IEEE 754"""
    return struct.pack("<f", value)


# ─── ENCODEUR DE PRODUIT ──────────────────────────────────────────────────────

def encode_produit(produit: dict) -> bytes:
    """
    Encode manuellement un Produit selon ce schéma :
      field 1 = id        (string)
      field 2 = nom       (string)
      field 3 = prix      (float)
      field 4 = stock     (int32 varint)
      field 5 = categorie (string)
    
    Les champs avec valeur par défaut (0, "") ne sont PAS encodés → gain de place.
    """
    result = bytearray()

    if produit.get("id"):
        result += encode_field(1, WIRE_LENGTH, encode_string(produit["id"]))

    if produit.get("nom"):
        result += encode_field(2, WIRE_LENGTH, encode_string(produit["nom"]))

    if produit.get("prix", 0) != 0:
        result += encode_field(3, WIRE_32BIT, encode_float(produit["prix"]))

    if produit.get("stock", 0) != 0:
        result += encode_field(4, WIRE_VARINT, encode_varint(produit["stock"]))

    if produit.get("categorie"):
        result += encode_field(5, WIRE_LENGTH, encode_string(produit["categorie"]))

    return bytes(result)


def decode_produit(data: bytes) -> dict:
    """
    Décode un message Protobuf binaire → dict Python.
    Illustre comment le parseur reconstitue les champs.
    """
    FIELD_NAMES = {1: "id", 2: "nom", 3: "prix", 4: "stock", 5: "categorie"}
    result = {}
    i = 0

    while i < len(data):
        # Lire le tag (varint)
        tag = 0
        shift = 0
        while True:
            byte = data[i]; i += 1
            tag |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7

        field_number = tag >> 3
        wire_type    = tag & 0x07
        field_name   = FIELD_NAMES.get(field_number, f"field_{field_number}")

        if wire_type == WIRE_VARINT:
            value = 0; shift = 0
            while True:
                byte = data[i]; i += 1
                value |= (byte & 0x7F) << shift
                if not (byte & 0x80): break
                shift += 7
            result[field_name] = value

        elif wire_type == WIRE_LENGTH:
            length = 0; shift = 0
            while True:
                byte = data[i]; i += 1
                length |= (byte & 0x7F) << shift
                if not (byte & 0x80): break
                shift += 7
            result[field_name] = data[i:i+length].decode("utf-8")
            i += length

        elif wire_type == WIRE_32BIT:
            result[field_name] = struct.unpack("<f", data[i:i+4])[0]
            i += 4

    return result


if __name__ == "__main__":
    produit = {"id": "P001", "nom": "Clavier Mécanique", "prix": 89.99, "stock": 42, "categorie": "Informatique"}

    encoded = encode_produit(produit)
    decoded = decode_produit(encoded)

    print("Produit original :", produit)
    print(f"\nEncodé ({len(encoded)} octets) :")
    print(" ".join(f"{b:02X}" for b in encoded))
    print("\nDécodé :", decoded)
    print(f"\nPrix original : {produit['prix']:.2f} / Décodé : {decoded['prix']:.2f}")
