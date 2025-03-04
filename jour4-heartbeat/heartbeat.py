"""
Jour 4 — Heartbeat : Détection de pannes
==========================================
Problème : Dans un réseau distribué, comment savoir si un nœud
est mort ou juste lent ? Un nœud silencieux est ambigu.

Solution : Le Heartbeat — chaque nœud envoie régulièrement
un signal "je suis vivant". Si on ne reçoit plus ce signal
pendant un certain délai → on le suspecte, puis on le déclare mort.

Machine à états d'un nœud :

  ┌─────────┐   heartbeat reçu    ┌─────────┐
  │  ALIVE  │ ◄────────────────── │  ALIVE  │
  └────┬────┘                     └─────────┘
       │ timeout dépassé
       ▼
  ┌─────────┐   2ème timeout      ┌─────────┐
  │ SUSPECT │ ──────────────────► │  DEAD   │
  └─────────┘                     └────┬────┘
       ▲                               │ heartbeat reçu
       └──────── RESURRECTION ─────────┘

Défis :
  - Timeout trop court  → faux positifs (nœud déclaré mort par erreur)
  - Timeout trop long   → détection lente (le système attend trop)
  - Solution : timeout ADAPTATIF basé sur l'historique des latences
"""

import time
import threading
import random
from enum import Enum
from dataclasses import dataclass, field
from collections import deque
from typing import Optional, Callable


# ─── ÉTATS D'UN NŒUD ─────────────────────────────────────────────────────────

class EtatNoeud(Enum):
    ALIVE   = "🟢 ALIVE"
    SUSPECT = "🟡 SUSPECT"
    DEAD    = "🔴 DEAD"


# ─── HEARTBEAT MESSAGE ────────────────────────────────────────────────────────

@dataclass
class Heartbeat:
    expediteur:  str
    timestamp:   float
    sequence:    int       # numéro de séquence (détecte les sauts)
    charge_cpu:  float     # % CPU du nœud (metadata utile)
    nb_connexions: int     # connexions actives


# ─── ENREGISTREMENT D'UN NŒUD ────────────────────────────────────────────────

@dataclass
class EnregistrementNoeud:
    """
    Ce que le moniteur sait d'un nœud distant.
    """
    nom:             str
    etat:            EtatNoeud = EtatNoeud.ALIVE
    dernier_heartbeat: float   = field(default_factory=time.time)
    sequence_attendu:  int     = 0
    heartbeats_recus:  int     = 0
    heartbeats_manques: int    = 0

    # Historique des délais inter-heartbeat pour timeout adaptatif
    # On garde les 10 derniers intervalles
    historique_delais: deque   = field(default_factory=lambda: deque(maxlen=10))

    # Métadonnées du dernier heartbeat
    derniere_charge_cpu:    float = 0.0
    dernieres_connexions:   int   = 0

    def intervalle_moyen(self) -> float:
        """Délai moyen entre les heartbeats reçus."""
        if not self.historique_delais:
            return 1.0
        return sum(self.historique_delais) / len(self.historique_delais)

    def timeout_adaptatif(self, facteur: float = 3.0) -> float:
        """
        Timeout = moyenne des délais × facteur de sécurité
        
        Si les heartbeats arrivent en moyenne toutes les 1.0s,
        on attend 3.0s avant de suspecter (facteur=3).
        
        Plus l'historique est stable, plus le timeout est précis.
        Si pas d'historique → timeout par défaut conservateur.
        """
        if len(self.historique_delais) < 3:
            return 3.0  # Pas assez de données → timeout conservateur
        moyenne = self.intervalle_moyen()
        ecart = max(self.historique_delais) - min(self.historique_delais)
        # On ajoute l'écart-type approximatif pour absorber la variabilité
        return (moyenne + ecart) * facteur


# ─── MONITEUR DE HEARTBEAT ────────────────────────────────────────────────────

class MoniteurHeartbeat:
    """
    Composant central : surveille tous les nœuds du cluster.
    
    Reçoit les heartbeats entrants, tourne une boucle de détection
    en arrière-plan, et appelle des callbacks sur changement d'état.
    """

    def __init__(
        self,
        nom: str,
        intervalle_check: float = 0.5,   # Fréquence de vérification (secondes)
        on_suspect: Optional[Callable]   = None,
        on_mort:    Optional[Callable]   = None,
        on_resurrection: Optional[Callable] = None,
    ):
        self.nom = nom
        self.intervalle_check = intervalle_check
        self._noeuds: dict[str, EnregistrementNoeud] = {}
        self._lock = threading.RLock()
        self._actif = False
        self._thread_detection: Optional[threading.Thread] = None

        # Callbacks événements
        self.on_suspect     = on_suspect     or (lambda n: None)
        self.on_mort        = on_mort        or (lambda n: None)
        self.on_resurrection = on_resurrection or (lambda n: None)

        # Journal des événements
        self.journal: list[tuple[float, str]] = []
        self._journal_lock = threading.Lock()

    def _log(self, message: str):
        with self._journal_lock:
            self.journal.append((time.time(), message))

    def enregistrer_noeud(self, nom: str):
        """Ajoute un nœud à surveiller."""
        with self._lock:
            if nom not in self._noeuds:
                self._noeuds[nom] = EnregistrementNoeud(nom=nom)
                self._log(f"Nœud enregistré : {nom}")

    def recevoir_heartbeat(self, hb: Heartbeat):
        """
        Appelé quand un heartbeat arrive d'un nœud distant.
        Met à jour l'enregistrement et gère la résurrection.
        """
        with self._lock:
            if hb.expediteur not in self._noeuds:
                self.enregistrer_noeud(hb.expediteur)

            noeud = self._noeuds[hb.expediteur]
            maintenant = time.time()

            # Calcul du délai depuis le dernier heartbeat
            if noeud.heartbeats_recus > 0:
                delai = maintenant - noeud.dernier_heartbeat
                noeud.historique_delais.append(delai)

            # Détection de sauts de séquence (heartbeats perdus)
            saut = hb.sequence - noeud.sequence_attendu
            if saut > 1:
                noeud.heartbeats_manques += saut - 1
                self._log(f"⚠️  {hb.expediteur} : {saut-1} heartbeat(s) manqué(s) (seq {noeud.sequence_attendu}→{hb.sequence})")

            # Résurrection ?
            etait_mort = noeud.etat in (EtatNoeud.DEAD, EtatNoeud.SUSPECT)

            # Mise à jour
            noeud.dernier_heartbeat     = maintenant
            noeud.sequence_attendu      = hb.sequence + 1
            noeud.heartbeats_recus     += 1
            noeud.derniere_charge_cpu   = hb.charge_cpu
            noeud.dernieres_connexions  = hb.nb_connexions

            if etait_mort:
                ancien_etat = noeud.etat
                noeud.etat = EtatNoeud.ALIVE
                self._log(f"💚 RÉSURRECTION : {hb.expediteur} ({ancien_etat.value} → ALIVE)")
                self.on_resurrection(hb.expediteur)

    def _boucle_detection(self):
        """
        Boucle de fond : vérifie périodiquement si des nœuds
        ont dépassé leur timeout.
        """
        while self._actif:
            time.sleep(self.intervalle_check)
            maintenant = time.time()

            with self._lock:
                for nom, noeud in self._noeuds.items():
                    silence = maintenant - noeud.dernier_heartbeat
                    timeout = noeud.timeout_adaptatif()

                    if noeud.etat == EtatNoeud.ALIVE and silence > timeout:
                        noeud.etat = EtatNoeud.SUSPECT
                        self._log(f"🟡 SUSPECT : {nom} (silence={silence:.2f}s > timeout={timeout:.2f}s)")
                        self.on_suspect(nom)

                    elif noeud.etat == EtatNoeud.SUSPECT and silence > timeout * 2:
                        noeud.etat = EtatNoeud.DEAD
                        self._log(f"🔴 MORT : {nom} (silence={silence:.2f}s)")
                        self.on_mort(nom)

    def demarrer(self):
        self._actif = True
        self._thread_detection = threading.Thread(
            target=self._boucle_detection, daemon=True, name=f"monitor-{self.nom}"
        )
        self._thread_detection.start()
        self._log(f"Moniteur '{self.nom}' démarré")

    def arreter(self):
        self._actif = False

    def etat_cluster(self) -> dict[str, EtatNoeud]:
        with self._lock:
            return {nom: n.etat for nom, n in self._noeuds.items()}

    def rapport(self) -> str:
        with self._lock:
            lignes = [f"\n  État du cluster (moniteur: {self.nom})", "  " + "─"*55]
            for nom, n in sorted(self._noeuds.items()):
                timeout = n.timeout_adaptatif()
                silence = time.time() - n.dernier_heartbeat
                lignes.append(
                    f"  {n.etat.value:<16} {nom:<16} "
                    f"reçus={n.heartbeats_recus:<5} "
                    f"manqués={n.heartbeats_manques:<3} "
                    f"silence={silence:.2f}s "
                    f"timeout={timeout:.2f}s"
                )
            return "\n".join(lignes)


# ─── NŒUD ÉMETTEUR ───────────────────────────────────────────────────────────

class NoeudReseau:
    """
    Simule un nœud qui envoie des heartbeats régulièrement.
    Peut simuler des pannes, des lenteurs, des redémarrages.
    """

    def __init__(
        self,
        nom: str,
        moniteur: MoniteurHeartbeat,
        intervalle: float = 1.0,   # Envoie un heartbeat toutes les X secondes
        gigue: float = 0.1,        # ±gigue aléatoire (simule la variabilité réseau)
    ):
        self.nom       = nom
        self.moniteur  = moniteur
        self.intervalle = intervalle
        self.gigue     = gigue
        self._sequence = 0
        self._actif    = False
        self._en_panne = False
        self._thread: Optional[threading.Thread] = None
        self._charge_cpu = random.uniform(10, 40)

    def _boucle_envoi(self):
        while self._actif:
            if not self._en_panne:
                self._sequence += 1
                hb = Heartbeat(
                    expediteur    = self.nom,
                    timestamp     = time.time(),
                    sequence      = self._sequence,
                    charge_cpu    = self._charge_cpu + random.uniform(-5, 5),
                    nb_connexions = random.randint(10, 200),
                )
                self.moniteur.recevoir_heartbeat(hb)

            # Délai variable (simule la variabilité réseau)
            delai = self.intervalle + random.uniform(-self.gigue, self.gigue)
            time.sleep(max(0.05, delai))

    def demarrer(self):
        self._actif = True
        self._thread = threading.Thread(
            target=self._boucle_envoi, daemon=True, name=f"node-{self.nom}"
        )
        self._thread.start()

    def tomber_en_panne(self):
        """Simule une panne : stoppe l'envoi de heartbeats."""
        self._en_panne = True

    def redemarrer(self):
        """Simule un redémarrage : reprend l'envoi."""
        self._sequence = 0  # Reset la séquence au redémarrage
        self._en_panne = False

    def arreter(self):
        self._actif = False
