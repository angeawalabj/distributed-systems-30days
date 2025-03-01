"""
Jour 1 — Client gRPC : Démonstration de tous les appels
Usage : python client.py
"""

import grpc
import logging
import sys

import produit_pb2
import produit_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLIENT] %(message)s"
)
log = logging.getLogger(__name__)

SERVER_ADDR = "localhost:50051"


def separateur(titre: str):
    print(f"\n{'═' * 55}")
    print(f"  {titre}")
    print('═' * 55)


def demo_get_produit(stub):
    """Test : récupérer un produit par ID"""
    separateur("RPC 1 — GetProduit (Unary)")

    # ✅ Produit existant
    try:
        produit = stub.GetProduit(produit_pb2.ProduitRequest(id="P001"))
        print(f"✅ Trouvé  : {produit.nom} — {produit.prix}€ (stock: {produit.stock})")
    except grpc.RpcError as e:
        print(f"❌ Erreur  : [{e.code()}] {e.details()}")

    # ❌ Produit inexistant
    try:
        produit = stub.GetProduit(produit_pb2.ProduitRequest(id="X999"))
        print(f"✅ Trouvé  : {produit.nom}")
    except grpc.RpcError as e:
        print(f"❌ Attendu : [{e.code().name}] {e.details()}")


def demo_liste_produits(stub):
    """Test : lister avec et sans filtre"""
    separateur("RPC 2 — ListeProduits (Unary → liste)")

    # Sans filtre
    reponse = stub.ListeProduits(produit_pb2.ListeRequest())
    print(f"📦 Tous ({reponse.total} produits) :")
    for p in reponse.produits:
        print(f"   {p.id} | {p.nom:<22} | {p.prix:>7.2f}€ | Stock: {p.stock}")

    # Avec filtre
    reponse = stub.ListeProduits(produit_pb2.ListeRequest(categorie="Informatique"))
    print(f"\n💻 Informatique ({reponse.total} produits) :")
    for p in reponse.produits:
        print(f"   {p.id} | {p.nom:<22} | {p.prix:>7.2f}€")


def demo_update_stock(stub):
    """Test : modifier le stock"""
    separateur("RPC 3 — UpdateStock (Unary → mutation)")

    cas = [
        ("P002", -5,   "Retrait de 5 unités"),
        ("P002", -5,   "Retrait de 5 unités (répété)"),
        ("P002", +20,  "Ajout de 20 unités"),
        ("P004", -100, "Retrait impossible (stock insuffisant)"),
        ("X999", -1,   "Produit inexistant"),
    ]

    for produit_id, delta, description in cas:
        try:
            rep = stub.UpdateStock(
                produit_pb2.UpdateStockRequest(id=produit_id, quantite=delta)
            )
            icone = "✅" if rep.succes else "⚠️ "
            print(f"{icone} {description}")
            print(f"   → {rep.message} | Nouveau stock: {rep.nouveau_stock}")
        except grpc.RpcError as e:
            print(f"❌ {description} : [{e.code().name}] {e.details()}")


def demo_stream_produits(stub):
    """
    Test : Server-side streaming
    Le serveur envoie les produits un à un — utile pour
    les grandes collections ou les mises à jour en temps réel.
    """
    separateur("RPC 4 — StreamProduits (Server-side Streaming)")

    print("🔄 Réception du flux (avec délai simulé de 0.5s/produit) :\n")

    try:
        # La réponse est un itérateur — on reçoit chaque produit au fur et à mesure
        stream = stub.StreamProduits(produit_pb2.ListeRequest(categorie="Informatique"))
        for i, produit in enumerate(stream, 1):
            print(f"   #{i} reçu → {produit.nom} ({produit.prix}€)")
    except grpc.RpcError as e:
        print(f"❌ Erreur stream : {e.details()}")


def main():
    separateur("DÉMO — Communication gRPC vs REST")
    print("""
  gRPC vs REST — Différences clés :
  ┌─────────────────┬──────────────┬─────────────────┐
  │ Critère         │ REST/HTTP    │ gRPC            │
  ├─────────────────┼──────────────┼─────────────────┤
  │ Format données  │ JSON (texte) │ Protobuf (binaire)│
  │ Taille payload  │ 100%         │ ~30% (3x moins) │
  │ Typage          │ Dynamique    │ Statique + fort  │
  │ Streaming       │ SSE/WebSocket│ Natif (4 types) │
  │ Contrat API     │ OpenAPI opt. │ .proto imposé   │
  │ Navigateur      │ ✅ Natif      │ ❌ Besoin proxy  │
  └─────────────────┴──────────────┴─────────────────┘
    """)

    # Connexion au serveur (insecure = sans TLS, pour le dev)
    with grpc.insecure_channel(SERVER_ADDR) as channel:
        stub = produit_pb2_grpc.ProduitServiceStub(channel)

        demo_get_produit(stub)
        demo_liste_produits(stub)
        demo_update_stock(stub)
        demo_stream_produits(stub)

    print("\n✅ Démo terminée !")


if __name__ == "__main__":
    main()
