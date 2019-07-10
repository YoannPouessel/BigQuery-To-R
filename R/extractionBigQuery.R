#' Extraction des données de Big Query
#'
#' À partir de R, envoi d'une requête SQL à Big Query, puis extraction et récupération des données sous R
#'
#' La fonction "extractionBigQuery" envoie une requête SQL à exécuter sous Big Query pour extraire les données.
#' Ces dernières sont stockées dans une table Big Query, puis transférées sous Google Cloud Storage. À partir
#' de cette emplacement, la fonction télécharge les données au format .csv dans le répertoire courant. La fonction
#' importe ces données sous R et les enregistre au format .rds. Enfin, la fonction concatène l'ensemble des données
#' dans un seul data frame.
#'
#' @author Yoann Pouëssel
#'
#' @param bdd_nom Nom de la base de données finale
#' @param projet_id Identifiant Big Queru du projet
#' @param query Requête SQL à exécuter sous Big Query
#'
#' @import bigQueryR
#' @import googleAuthR
#' @import googleCloudStorageR
#'
#' @return Un \code{data.frame} des données extraites de Big Query
#' @export extractionBigQuery

extractionBigQuery <- function(query, projet_id, bdd_nom) {

  # --> Initialisation de l'environnement
  if (Sys.getenv("mode") == "container") {
    gar_auth_service(json_file = '/auth/auth.json',
                     scope = 'https://www.googleapis.com/auth/cloud-platform')

    dataset_id <- "r_shiny"
    table_rds <- "/data/"

  } else if (Sys.getenv("mode") == "") {
    gar_auth_service(json_file = '/auth/auth.json',
                     scope = 'https://www.googleapis.com/auth/cloud-platform')

    dataset_id <- "data_science"
    table_rds <- "D:/Projets/01. Logiciels/R/Tables_RDS/"
  }

  # --> Extraction des données sous Big Query
  extraction <- bqr_query_asynch(projectId = projet_id,
                                 datasetId = dataset_id,
                                 destinationTableId = bdd_nom,
                                 query = query,
                                 useLegacySql = TRUE,
                                 writeDisposition = "WRITE_TRUNCATE")

  nb_minute <- 0
  while (bqr_get_job(projet_id, jobId = extraction$jobReference$jobId)$status$state != 'DONE') {
    Sys.sleep(60)
    nb_minute <- nb_minute + 1
    if (nb_minute > 60) {
      stop("Le temps d'extraction des données sous Big Query est supérieur à une heure")
    }
  }

  print("Extraction des données sous Big Query : Terminé")

  # --> Transfert des données sous Google Cloud Storage
  transfert <- bqr_extract_data(projectId = projet_id,
                                datasetId = dataset_id,
                                tableId = bdd_nom,
                                cloudStorageBucket = paste0("gs://ofm_yoann/"),
                                filename = paste0(bdd_nom, "_*.csv"),
                                destinationFormat = "CSV",
                                compression = "GZIP",
                                fieldDelimiter = ",")

  nb_minute <- 0
  while (bqr_get_job(projet_id, jobId = transfert$jobReference$jobId)$status$state != 'DONE') {
    Sys.sleep(60)
    nb_minute <- nb_minute + 1
    if (nb_minute > 60) {
      stop("Le temps de transfert des données vers Google Cloud Storage est supérieur à une heure")
    }
  }

  nb_fichier <- as.integer(bqr_get_job(projet_id, jobId = transfert$jobReference$jobId)$statistics$extract$destinationUriFileCounts)
  print("Transfert des données vers Google Cloud Storage : Terminé")

  # --> Téléchargement des données au format RDS
  for (num_fichier in 0:(nb_fichier-1)) {
    nom_fichier <- paste0(bdd_nom, "_", gsub(" ","0", sprintf("%12d", num_fichier)), ".csv")

    gcs_get_object(bucket = "ofm_yoann",
                   object_name = nom_fichier,
                   saveToDisk = paste0(table_rds, nom_fichier),
                   overwrite = TRUE)

    bdd_import <- read.csv(paste0(table_rds, nom_fichier))
    saveRDS(bdd_import, paste0(table_rds, paste0(bdd_nom, "_", num_fichier), ".rds"))
    file.remove(paste0(table_rds, nom_fichier))
  }

  print("Téléchargement des données au format RDS : Terminé")

  # --> Importation des données sous R
  bdd <- data.frame()

  for (num_fichier in 0:(nb_fichier-1)) {
    nom_fichier <- paste0(table_rds, paste0(bdd_nom, "_", num_fichier), ".rds")
    bdd_import <- readRDS(nom_fichier)
    file.remove(nom_fichier)

    # --> Construction de la base de données finale
    bdd <- rbind(bdd, bdd_import)
    gc()
  }

  print("Construction de la base de données : Terminé")

  # --> Sauvegarde de la base de données
  assign(bdd_nom, bdd, envir = globalenv())
  print("Sauvegarde de la base de données : Terminé")
}
