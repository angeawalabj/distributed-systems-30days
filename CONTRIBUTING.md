# Contributing

Ce repo est un challenge personnel de 30 jours — il n'est pas ouvert aux PRs de code.

Mais les issues sont les bienvenues pour :

- **Corrections** : bug dans une simulation, assertion incorrecte, résultat trompeur
- **Questions** : un concept mal expliqué dans un README
- **Suggestions** : un module bonus à ajouter (jour 31 ?)

## Structure d'un module

Chaque module suit cette convention stricte :

```
jour-N-concept/
├── concept.py      # Implémentation pure, zéro dépendance externe
├── simulation.py   # 5 scénarios mesurés, python3 simulation.py suffit
└── README.md       # Problème → Solution → Résultats → Production tips
```

## Contraintes non négociables

- **Stdlib Python uniquement** — pas de `pip install`
- **Chaque propriété est prouvée** — pas d'assertions sans chiffres réels
- **`python3 simulation.py` fonctionne** — aucune configuration préalable

## Lancer tous les modules

```bash
for d in jour*/; do
  echo "=== $d ==="
  python3 "$d/simulation.py" 2>&1 | tail -5
done
```
