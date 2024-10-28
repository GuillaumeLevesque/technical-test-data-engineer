# Réponses du test

### Étape 0: Notes du candidat
Bonjour, j'aimerais mentionner quelques notes par rapport à ma candidature et ce test:
- Je ne prétends pas déjà être un expert en MLOps. je désire vous joindre pour le devenir!
- En ce sens, mes réponses à ce test sont plus du côté 'simple' et je désire en apprendre plus avec vous.
- Merci pour ce test, j'ai aimé la mise en situation pratique.

## Utilisation de la solution (étape 1 à 3)

### Étape 1: setup
0. Creer et activer un environnement virtuel puis executer `pip install requirements.txt`.
1. Déplacez-vous dans le dossier `src/moovitamix_fastapi` puis exécuter la commande `python -m uvicorn main:app`. 
2. Ouvrir un autre terminal, déplacez-vous dans le dossier `src/data_flow` puis exécuter la commande `python main.py`.

### Étape 2: flux de données
le script extrait les données des 3 endpoints selon la 'schedule' quotidienne.
- Les données brutes sont enregistrées dans le dossier /raw
- Les données sont nettoyées (doublons, entrées vides, format de date)
- Les données nettoyées sont enregistrées dans le dossier /clean
- Ma prochaine étape aurait été la containerization (cron + docker + docker-compose) pour permettre de facilement déployer. Pour garder ca simple, j'ai choisit le package 'schedule' à la place.

### Étape 3: Tests
executer `pytest` dans le projet.

Les éléments principaux du flux de donnée sont testé, avec un focus sur la validation / nettoyage. Je crois que mon nettoyage et validation ne sont pas très forts en ce moment. 
Avoir plus de temps, j'aimerais les améliorer ou utiliser une librairie plus puissante.

## Questions (étapes 4 à 7)

### Étape 4

Une base de donnée SQL répond aux besoins en raison de :
- support de clé étrangère et valeur "unique"
- recovery en cas de perte de données
- s'intègre bien aux infrastructure cloud
- performances avec du parallel processing
- "scalabilité" horizontal (partitioning) & vertical 

Selon le setup courant du client, on pourrait utiliser
- postgreSQL
- MariaDB
- SQL Server
- ...

Outre les types évident comme 'TIMESTAMP' pour 'createdAt' ou 'VARCHAR' pour 'gender',
voici les types importants du schema:
- table users
  - id SERIAL PRIMARY KEY
  - email VARCHAR(100) UNIQUE
  - ...
- table tracks
  - id SERIAL PRIMARY KEY
  - ...
- table listen_history
  - id SERIAL PRIMARY KEY
  - user_id INT REFERENCES users(id)
  - ...
- table de jonction listen_history_tracks
  - PRIMARY KEY (listen_history_id, track_id)
  - listen_history_id INT REFERENCES listen_history(id)
  - track_id INT REFERENCES tracks(id)

La 4e table est créée en raison de la relation many-to-many entre listen_history et tracks.
Cela permet de rapidement récupérer des données pertinentes


### Étape 5

Puisque je m'y connais mieux avec l'ecosysteme microsoft, 
je choisi ici des outils intégré Azure. 
Toutefois, il existe certainement des outils correspondants pour d'autres provider 
ou 'on-premise'

Je regarde les métriques suivantes:
- tests unitaires
  - Azure devops, pipelines en YAML
  - diminuer les changes de bug regression
- utilisation des ressources matérielles (CPU, memory)
  - Azure Monitor
  - vérifier si on peut économiser en diminuant les ressources allouées
- Execution du pipeline (success/failure, latence, duration)
  - Azure Data Factory
  - savoir si on doit optimiser le code pour réduire les échec / temps
- Qualité des données (mismatch, duplicates, rapport d'erreur)
  - Azure Log Analytics
  - pour savoir si on doit mieux traiter les données

En ce qui concerne les alertes, envoyer des courriels aux admins et/ou devs lorsqu'une des métriques echoue


### Étape 6

Je ne suis pas certain de comprendre la question. Je suppose que la question est synonyme de "comment automatiser l'accès des recommendations les plus récentes par l'utilisateur".

Puisque les données sont recues chaque jour, on pourrait ajouter un microservice (Azure Cloud) au pipeline, après l'ingestion des données. Le microservice doit avoir accès à:
- le modèle entraîné sélectionné
- les données par utilisateur (tirées des tables décrites en Question 4)

Ce microservice se sert des 2 accès ci-haut pour créer une liste des top 'n' recommendations.

Cette nouvelle table de recommendations est accessible par les différents front-ends de l'utilisateur.

### Étape 7

On peut ajouter un trigger de réentraienement dans le pipeline, entre l'ingestion de données et le microservice de l'étape 6. triggers possibles:
- chaque jour avec les nouvelles données.
- lorsque que le système détecte une diminution de la qualité des résultats.

La première approche coutera moins chèr en ressources de développement, mais il est possible de faire du travail sans amélioration du modèle..
La 2e est moins chère en ressources materielles, mais demande plus de développement au début.

Dans les 2 cas, je suggère de déployer le modèle avec Azure Machine Learning pour avoir accès facilement via un endpoint sécurisé.
