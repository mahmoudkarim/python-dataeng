# Projet de Traitement de Données

Ce projet utilise Apache Beam pour traiter et analyser des données provenant de différentes sources.

## Prérequis

- Python 3.10
- Environnement virtuel (recommandé)

## Installation

1. Clonez ce dépôt.
2. Créez et activez un environnement virtuel (recommandé).
3. Installez les dépendances :
   ```bash
   pip install -r requirements.txt

## Résultats

Les résultats du traitement sont déjà pré-générés et disponibles dans le dossier `src/data`.
**le json du graph est dans le dossier output.**

## Réinitialisation et Relancement

Pour réinitialiser le projet et relancer le traitement depuis le début :

1. Supprimez le dossier de données existant :
    ```bash
    rm -rf src/data

2. Recréez le dossier de données brutes et copiez les fichiers originaux :
    ```bash
    mkdir -p src/data/raw_data
    cp -r raw_data/* src/data/raw_data/
    ```

3. Relancez le script principal :
    ```bash 
    cd src
    python main.py 


## Traitement ad-hoc (Bonus)

Depuis le json produit par la data pipeline, vous devez aussi mettre en place deux fonctions permettant de
répondre à la problématique suivante :
• Extraire le nom du journal qui mentionne le plus de médicaments différents.
• Pour un médicament donné, trouver l’ensemble des médicaments mentionnés par les mêmes
journaux référencés par les publications scientifiques (PubMed) mais non les tests cliniques (Clinical
Trials)


Assurez-vous que votre fichier JSON de données est disponible à l'emplacement data/output/graph.json.

Exécutez le script en ligne de commande:

```bash
python ad-hoc.py [mode] [nom_du_médicament]
```

mode : soit journal_with_most_drugs (pour obtenir le journal avec le plus de médicaments) ou related (pour trouver les médicaments liés).

nom_du_médicament : le nom du médicament pour lequel vous souhaitez trouver des médicaments liés (nécessaire uniquement si mode est related).
Si aucun nom de médicament n'est fourni, le script utilisera par défaut 'tetracycline'.

Exemples
Pour obtenir le journal mentionnant le plus de médicaments:

```bash
python ad-hoc.py journal_with_most_drugs
```

Pour trouver les médicaments liés à 'tetracycline':

```bash
python ad-hoc.py related tetracycline
```

Cela affichera le journal avec le plus de médicaments ou une liste des médicaments liés au médicament cible.

