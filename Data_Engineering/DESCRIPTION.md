# Explication de ma reflexion

Ce document a pour objectif de présenter et d'expliquer la réflexion menée autour de l'analyse de ce projet.

# Analyse des fichiers

## 1. Clinical trial

Le fichier CSV contient les colonnes suivantes :

- **id** : Identifiant unique
- **scientific_title** : Titre scientifique de l'étude.
- **date** : Date de l'étude.
- **journal** : Journal de publication.

### Observations et Problèmes Potentiels

- **Données manquantes** :
  - La ligne avec l'ID `NCT04237090` a un titre vide.
  - Une ligne n'a pas d'ID (`""`).
  - La ligne avec l'ID `NCT03490942` a un nom de journal vide.
  
- **Format de Date** :
  - Les dates sont dans différents formats : `"1 January 2020"` et `"25/05/2020"`. Il est nécessaire de normaliser les formats de date.
  
- **Encodage des Caractères** :
  - La ligne avec l'ID `NCT04153396` contient des caractères encodés (`\xc3\xb1`).
  - La ligne avec l'ID `NCT04188184` contient des caractères encodés (`\xc3\x28`).

- **Charactères Speciaux**
 - Le fichier contient des charactères speciaux  ™, ô

- **Doublons Potentiels** :
  - Il y a une ligne sans ID mais avec un titre identique à celui de l'ID `NCT03490942`.

---

## 2. Drugs

Le fichier CSV contient les colonnes suivantes :

- **atccode** : Code ATC pour chaque médicament.
- **drug** : Nom du médicament.

### Observations et Problèmes Potentiels

- **Consistance des Données** :
  - Assurer que les noms des médicaments sont correctement orthographiés et formatés de manière cohérente ici on a tous en majuscule

---

## 3. Pubmed CSV

Le fichier CSV contient les colonnes suivantes :

- **id** : Identifiant unique pour chaque article.
- **title** : Titre de l'article.
- **date** : Date de publication de l'article.
- **journal** : Journal ou l'article est publié.

### Observations et Problèmes Potentiels

- **Espace a la fin** :
  - Des espaces ont été ajoutées à la fin du fichier

- **Format de Date Inconsistant** :
  - Les dates sont dans différents formats : `"01/01/2019"` et `"2020-01-01"`. Il est nécessaire de normaliser les formats de date.

---

## 4. Pubmed JSON

Le fichier JSON contient les attributs suivants :

- **id** : Identifiant unique pour chaque article.
- **title** : Titre de l'article.
- **date** : Date de publication de l'article.
- **journal** : Journal ou l'article est publié.

### Observations et Problèmes Potentiels

- **Virgule a la fin**
 - Une virgule est de trop a la fin du fichier

- **Format de Date Inconsistant** :
  - Les dates sont dans le format `"01/01/2020"`

- **Données Manquantes** :
  - Une entrée a un ID vide.

# Etape de data processing

Notre pipeline est composé de plusieurs étapes : 

## 1. Nettoyage
La première étape consiste à nettoyer les données brutes stockées dans `data/raw_data`.

Nous utilisons la fonction `run_cleaning()` du module `pipeline.data_cleaning` pour :
- Éliminer les virgules superflues dans les fichiers JSON et ainsi pouvoir les lires sans problèmes
- S'assurer que c'est un JSON valide
- Supprimer les caractères hexadécimaux indésirables
- Corriger les erreurs d'encodage fréquentes
- Tout convertire en string dans un premier temps
- Eliminer les lignes avec uniquement des espaces

Les fichier de `raw_data` sont placer dans `raw_data/old` et les nouveaux fichier sont placer dans un dossier `pre-processing`.
On sait immédiatement quels fichiers ont déjà été traités et lesquels sont nouveaux, évitant ainsi les confusions ou les doublons.
Les anciens fichiers sont archivés dans `raw_data/old`, ce qui permet de garder une trace des versions précédentes si besoin.

## 2. File processing
Une fois nettoyées, les données subissent une transformation pour uniformiser leur format. La fonction `run_file_processing()` du module `pipeline.file_processor` est utilisé pour :
- Convertir tous les fichiers en format JSON
- Standardiser la structure des données :
  - Le Json est plus lisible et simple à lire
  - Structure  idéale pour organiser des données de type graph
  - Facile à manipuler et leger

Les fichiers générés sont disposé dans un dossier `tmp`, ces fichiers attendront d'etre formaté.
Ils sont organisé dans des dossier horodaté et on y ajoute un id pour rendre unique le nom des fichiers et eviter les conflits lors des étapes suivantes du processus.

Dans notre cas certain fichiers ont le meme nom, il faudrait avoir des règles de nommage comme un horodatage sur les fichiers, il est intéressent de les archiver et de les separé dans differents espace de stockage selon leur états

## 3. Formatage spécifique
Le formatage des données s'effectue selon les spécifications définies dans notre fichier `config.yaml`. La fonction `run_data_formatting()` du module `pipeline.data_formatter` permet de :
- Convertir les types de données selon nos règles établies
- Ajuster les formats
- En imposant des règles de formatage strictes, on réduit les risques d'erreurs liées à des formats incohérents ou à des types de données mal définis.

On aurait aussi pu avoir un seul fichier Json pour chaque "table" dans un dossier de config
Les fichiers son deposés dans `formated_data`

## 4. Validation rigoureuse
La validation des données est une étape critique pour assurer l'intégrité et la fiabilité de nos données. Nous utilisons `run_data_validation()` du module `pipeline.data_validation` pour :
- Vérifier la complétude des données
- Identifier et écarter les données inexploitables ou incohérentes
- Séparer les données valides des invalides

Les données non validé sont isolé dans un dossier rejected, elles devront etre analysé par la suite


## 5. Consolidation et déduplication
La dernière phase vise à consolider les informations validées. La fonction `run_data_collection()` du module `pipeline.data_collector` est utilisée pour :
- Regrouper les données validées
- Éliminer les doublons
- Préparer les données pour la construction de notre graphe

Chaque étape de ce processus a été  conçue pour assurer une transformation efficace de nos données brutes en informations exploitables, prêtes pour l'analyse et la construction de notre graphe.

On place les fichiers sous le dossier graph, ils sont pret à etre exploité pleinement


# Reponse aux Hypothèses de travail

## 1. Réutilisation des étapes du pipeline

- **Composants réutilisables** : Chaque étape du pipeline est séparé est peux etre reutilisé (sauf la partie du graph)
- **Génériques** : S'adapte a differents type de données (JSON, CSV) et de structure, tout ca en utilisant la meme classe !
- **Utilisation de Beam** S'adapte à différents environnements, ce qui en fait un framework puissant

## 2. Intégration dans un orchestrateur de jobs
- **Sequences claires** : La séquence des opérations dans notre pipeline définit un flux de données linéaire, qui peut être facilement traduit en un DAG où chaque étape dépend de la précédente.
- **Utilisation de Beam** Beam gère l'état et le flux de données entre les étapes, ce qui est crucial pour maintenir la cohérence dans un DAG complexe.

# Pour aller plus loin

## Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ?

## Pourriez-vous décrire les modifications qu’il faudrait apporter, s’il y en a, pour prendre en considération de telles volumétries ?

- **Traitement distribué** :
Utiliser un runner distribué d'Apache Beam comme Dataflow ou Spark au lieu du DirectRunner actuel.
Ces technologies sont conçus pour gérer de grandes quantités de données. Ils peuvent scaler horizontalement ce qui permet de traiter d'énormes volumes de données de manière plus efficace. Ce sont des technologies plus robuste et taillé pour la production.
Les runners distribués exploite le traitement parallèle et la répartition des tâches sur plusieurs noeuds et réduise considérablement le temps d'exécution pour des tâches complexes.

- **Le Cloud** :
En utilisant des ressources cloud pour le traitement et le stockage des données, facilitant ainsi les pipelines de traitement de données. Il y'a cerainement des moyen d'optimisé les couts et les ressources avec les services cloud.

- **Organisation** : 
Il est possible de diviser les données en partitions logiques ou temporelles pour traiter efficacement des volumes massifs (avoir des folder avec des sous dossier horodater ou sont deposé les fichiers). Choisir la bonne fréquence d'execution de notre pipeline
