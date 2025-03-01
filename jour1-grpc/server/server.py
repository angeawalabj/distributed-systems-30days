"""
Jour 1 — Serveur gRPC : ProduitService
Commande pour générer les stubs depuis le proto :
  python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. protos/produit.proto
"""

import grpc
import time
import logging
from concurrent import futures

# Stubs générés par protoc (après la commande ci-dessus)
import produit_pb2
import produit_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SERVER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


# ─── BASE DE DONNÉES EN MÉMOIRE ───────────────────────────────────────────────

PRODUITS_DB = {
    "P001": {"id": "P001", "nom": "Clavier Mécanique",  "prix": 89.99,  "stock": 42, "categorie": "Informatique"},
    "P002": {"id": "P002", "nom": "Souris Ergonomique", "prix": 49.99,  "stock": 15, "categorie": "Informatique"},
    "P003": {"id": "P003", "nom": "Moniteur 4K",        "prix": 399.99, "stock": 8,  "categorie": "Informatique"},
    "P004": {"id": "P004", "nom": "Bureau Debout",      "prix": 649.99, "stock": 3,  "categorie": "Mobilier"},
    "P005": {"id": "P005", "nom": "Casque Audio",       "prix": 129.99, "stock": 27, "categorie": "Audio"},
}


# ─── IMPLÉMENTATION DU SERVICE ────────────────────────────────────────────────

class ProduitServicer(produit_pb2_grpc.ProduitServiceServicer):
    """
    Implémentation concrète du service gRPC.
    Chaque méthode correspond à un RPC défini dans le .proto.
    """

    def GetProduit(self, request, context):
        """Unary RPC : le client envoie 1 requête, reçoit 1 réponse."""
        log.info(f"GetProduit appelé → id={request.id}")

        produit_data = PRODUITS_DB.get(request.id)

        if not produit_data:
            # On renvoie un code d'erreur gRPC standard (comme un 404 HTTP)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Produit '{request.id}' introuvable")
            return produit_pb2.Produit()

        return produit_pb2.Produit(**produit_data)

    def ListeProduits(self, request, context):
        """Unary RPC : renvoie une liste filtrée."""
        log.info(f"ListeProduits appelé → categorie={request.categorie or 'toutes'}")

        resultats = list(PRODUITS_DB.values())

        # Filtre par catégorie si spécifié
        if request.categorie:
            resultats = [p for p in resultats if p["categorie"] == request.categorie]

        # Limite le nombre de résultats
        limite = request.limite if request.limite > 0 else 100
        resultats = resultats[:limite]

        produits_pb = [produit_pb2.Produit(**p) for p in resultats]
        return produit_pb2.ListeResponse(produits=produits_pb, total=len(produits_pb))

    def UpdateStock(self, request, context):
        """
        Unary RPC : modifie le stock.
        CONCEPT CLÉ — Idempotence :
          Cette opération N'EST PAS idempotente par défaut.
          Pour la rendre idempotente, on pourrait accepter un 'request_id'
          et mémoriser les requêtes déjà traitées (voir jour 5).
        """
        log.info(f"UpdateStock → id={request.id}, delta={request.quantite:+d}")

        if request.id not in PRODUITS_DB:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Produit '{request.id}' introuvable")
            return produit_pb2.UpdateStockResponse(succes=False, message="Produit introuvable")

        produit = PRODUITS_DB[request.id]
        nouveau_stock = produit["stock"] + request.quantite

        if nouveau_stock < 0:
            return produit_pb2.UpdateStockResponse(
                succes=False,
                message=f"Stock insuffisant (actuel: {produit['stock']})",
                nouveau_stock=produit["stock"]
            )

        PRODUITS_DB[request.id]["stock"] = nouveau_stock
        log.info(f"  Stock mis à jour : {produit['stock'] - request.quantite} → {nouveau_stock}")

        return produit_pb2.UpdateStockResponse(
            succes=True,
            message="Stock mis à jour avec succès",
            nouveau_stock=nouveau_stock
        )

    def StreamProduits(self, request, context):
        """
        Server-side Streaming RPC :
        Le serveur envoie les produits UN PAR UN, simulant
        un flux de données en temps réel.
        """
        log.info(f"StreamProduits démarré → categorie={request.categorie or 'toutes'}")

        resultats = list(PRODUITS_DB.values())
        if request.categorie:
            resultats = [p for p in resultats if p["categorie"] == request.categorie]

        for produit_data in resultats:
            if context.is_active():  # Vérifie que le client est toujours connecté
                log.info(f"  Streaming → {produit_data['nom']}")
                yield produit_pb2.Produit(**produit_data)
                time.sleep(0.5)  # Simule un délai de traitement
            else:
                log.warning("Client déconnecté, arrêt du stream")
                break


# ─── DÉMARRAGE DU SERVEUR ─────────────────────────────────────────────────────

def serve():
    """
    Démarre le serveur gRPC sur le port 50051.
    ThreadPoolExecutor(10) = 10 requêtes en parallèle max.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    produit_pb2_grpc.add_ProduitServiceServicer_to_server(ProduitServicer(), server)

    port = "50051"
    server.add_insecure_port(f"[::]:{port}")  # En prod: utiliser TLS avec add_secure_port()
    server.start()

    log.info(f"✅ Serveur gRPC démarré sur le port {port}")
    log.info("   Produits disponibles : " + ", ".join(PRODUITS_DB.keys()))
    log.info("   En attente de connexions... (Ctrl+C pour arrêter)")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("Arrêt du serveur...")
        server.stop(grace=5)  # 5 secondes pour finir les requêtes en cours


if __name__ == "__main__":
    serve()
