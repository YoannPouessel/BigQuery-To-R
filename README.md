## BigQuery-To-R
### Extraction des données de Big Query à partir de R
La fonction "extractionBigQuery" envoie une requête SQL à exécuter sous Big Query pour extraire les données. Ces dernières sont stockées dans une table Big Query, puis transférées sous Google Cloud Storage. À partir de cette emplacement, la fonction télécharge les données au format .csv dans le répertoire courant. La fonction importe ces données sous R et les enregistre au format .rds. Enfin, la fonction concatène l'ensemble des données dans un seul data frame.


### Avertissement
Le package est utilisé dans un contexte professionnel précis. Ainsi, certains paramètres sont stockés directement au sein de la fonction.
