# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is an adaptation of ResultsDataModel.R found in of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#' Get specifications for results data model
#'
#' @return
#' A tibble data frame object with specifications
#'
#' @export
getResultsDataModelSpecifications <- function(pathToCsv = "resultsDataModelSpecification.csv") {
  resultsDataModelSpecifications <-
    readr::read_csv(file = pathToCsv, col_types = readr::cols())
  return(resultsDataModelSpecifications)
}

#' Get a list of vocabulary table names
#'
#' @return
#' Get a list of vocabulary table names in results data model
#'
#' @export
getDefaultVocabularyTableNames <- function() {
  getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$isVocabularyTable == "Yes") %>%
    dplyr::pull(.data$tableName) %>%
    unique() %>%
    sort() %>%
    SqlRender::snakeCaseToCamelCase()
}

appendNewRows <-
  function(data,
           newData,
           tableName,
           specifications = getResultsDataModelSpecifications()) {
    if (nrow(data) > 0) {
      primaryKeys <- specifications %>%
        dplyr::filter(.data$tableName == !!tableName &
          .data$primaryKey == "Yes") %>%
        dplyr::select(.data$fieldName) %>%
        dplyr::pull()
      newData <- newData %>%
        dplyr::anti_join(data, by = primaryKeys)
    }
    return(dplyr::bind_rows(data, newData))
  }


#' Create the results data model data definition language (DDL)
#'
#' @param schema         The schema that will hold the results
#' @param specifications The results schema table specifications
#' 
#' @export
createResultsDataModelDDL <- function(schema,
                                      specifications = getResultsDataModelSpecifications()) {
  tableList <- unique(specifications$tableName)
  checkmate::assert_count(length(tableList))
  ddl <- ""
  for (t in 1:length(tableList)) {
    tableName <- tableList[t]
    dataModelSubset <- specifications[specifications$tableName == tableName, ]
    
    # Loop through the columns to create the column DDL
    columns <- c()
    primaryKey <- c()
    for (i in 1:nrow(dataModelSubset)) {
      columns <- c(columns, paste(dataModelSubset$fieldName[i],
                                  dataModelSubset$type[i],
                                  ifelse(toupper(dataModelSubset$isRequired[i]) == "YES", "NOT NULL", "NULL")))
      if (toupper(dataModelSubset$primaryKey[i]) == "YES") {
        primaryKey <- c(primaryKey, dataModelSubset$fieldName[i])
      }
    }
    
    sql <- SqlRender::readSql("inst/sql/sql_server/CreateResultTable.sql")
    renderedSql <- SqlRender::render(sql = sql,
                                     results_schema = schema,
                                     table = tableName,
                                     columns = columns,
                                     primary_key = primaryKey)
    ddl <- paste(ddl, renderedSql, sep = "\n")
  }
  invisible(ddl)
}

#' Create the results data model tables on a database server.
#'
#' @details
#' Only PostgreSQL servers are supported.
#'
#' @template Connection
#' @param schema         The schema to hold the results
#' @param specifications The results data model specification
#'
#' @export
createResultsDataModel <- function(connection = NULL,
                                   connectionDetails = NULL,
                                   schema,
                                   specifications = getResultsDataModelSpecifications()) {
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  if (connection@dbms == "sqlite" & schema != "main") {
    stop("Invalid schema for sqlite, use schema = 'main'")
  }

  ddlSql <- createResultsDataModelDDL(schema = schema,
                                      specifications = specifications)
  sql <- SqlRender::translate(sql = ddlSql,
                              targetDialect = connection@dbms)
  DatabaseConnector::executeSql(connection, sql)
}

naToEmpty <- function(x) {
  x[is.na(x)] <- ""
  return(x)
}

naToZero <- function(x) {
  x[is.na(x)] <- 0
  return(x)
}

uploadTable <- function(connection,
                            schema,
                            databaseId,
                            tableName,
                            resultsFolder,
                            purgeSiteDataBeforeUploading,
                            specifications = getResultsDataModelSpecifications()) {
  ParallelLogger::logInfo("Uploading table ", tableName)
  
  primaryKey <- specifications %>%
    filter(.data$tableName == !!tableName &
             .data$primaryKey == "Yes") %>%
    select(.data$fieldName) %>%
    pull()
  
  if (purgeSiteDataBeforeUploading &&
      "database_id" %in% primaryKey) {
    deleteAllRecordsForDatabaseId(
      connection = connection,
      schema = schema,
      tableName = tableName,
      databaseId = databaseId
    )
  }
  
  csvFileName <- paste0(tableName, ".csv")
  if (csvFileName %in% list.files(resultsFolder)) {
    env <- new.env()
    env$schema <- schema
    env$tableName <- tableName
    env$primaryKey <- primaryKey
    if (purgeSiteDataBeforeUploading &&
        "database_id" %in% primaryKey) {
      env$primaryKeyValuesInDb <- NULL
    } else if (length(primaryKey) > 0) {
      sql <- "SELECT DISTINCT @primary_key FROM @schema.@table_name;"
      sql <- SqlRender::render(
        sql = sql,
        primary_key = primaryKey,
        schema = schema,
        table_name = tableName
      )
      primaryKeyValuesInDb <-
        DatabaseConnector::querySql(connection, sql)
      colnames(primaryKeyValuesInDb) <-
        tolower(colnames(primaryKeyValuesInDb))
      env$primaryKeyValuesInDb <- primaryKeyValuesInDb
    }
    
    uploadChunk <- function(chunk, pos) {
      ParallelLogger::logInfo(
        "- Preparing to upload rows ",
        pos,
        " through ",
        pos + nrow(chunk) - 1
      )
      
      # chunk <- checkFixColumnNames(
      #   table = chunk,
      #   tableName = env$tableName,
      #   zipFileName = zipFileName,
      #   specifications = specifications
      # )
      # chunk <- checkAndFixDataTypes(
      #   table = chunk,
      #   tableName = env$tableName,
      #   zipFileName = zipFileName,
      #   specifications = specifications
      # )
      # chunk <- checkAndFixDuplicateRows(
      #   table = chunk,
      #   tableName = env$tableName,
      #   zipFileName = zipFileName,
      #   specifications = specifications
      # )
      
      # Primary key fields cannot be NULL, so for some tables convert NAs to empty or zero:
      toEmpty <- specifications %>%
        filter(
          .data$tableName == env$tableName &
            .data$emptyIsNa == "No" & grepl("varchar", .data$type)
        ) %>%
        select(.data$fieldName) %>%
        pull()
      if (length(toEmpty) > 0) {
        chunk <- chunk %>%
          dplyr::mutate_at(toEmpty, naToEmpty)
      }
      
      tozero <- specifications %>%
        filter(
          .data$tableName == env$tableName &
            .data$emptyIsNa == "No" &
            .data$type %in% c("int", "bigint", "float")
        ) %>%
        select(.data$fieldName) %>%
        pull()
      if (length(tozero) > 0) {
        chunk <- chunk %>%
          dplyr::mutate_at(tozero, naToZero)
      }
      
      # Check if inserting data would violate primary key constraints:
      if (!is.null(env$primaryKeyValuesInDb)) {
        primaryKeyValuesInChunk <- unique(chunk[env$primaryKey])
        duplicates <- inner_join(env$primaryKeyValuesInDb,
                                 primaryKeyValuesInChunk,
                                 by = env$primaryKey
        )
        if (nrow(duplicates) != 0) {
          if ("database_id" %in% env$primaryKey ||
              forceOverWriteOfSpecifications) {
            ParallelLogger::logInfo(
              "- Found ",
              nrow(duplicates),
              " rows in database with the same primary key ",
              "as the data to insert. Deleting from database before inserting."
            )
            deleteFromServer(
              connection = connection,
              schema = env$schema,
              tableName = env$tableName,
              keyValues = duplicates
            )
          } else {
            ParallelLogger::logInfo(
              "- Found ",
              nrow(duplicates),
              " rows in database with the same primary key ",
              "as the data to insert. Removing from data to insert."
            )
            chunk <- chunk %>%
              anti_join(duplicates, by = env$primaryKey)
          }
          # Remove duplicates we already dealt with:
          env$primaryKeyValuesInDb <- env$primaryKeyValuesInDb %>%
            anti_join(duplicates, by = env$primaryKey)
        }
      }
      if (nrow(chunk) == 0) {
        ParallelLogger::logInfo("- No data left to insert")
      } else {
        DatabaseConnector::insertTable(
          connection = connection,
          tableName = env$tableName,
          databaseSchema = env$schema,
          data = chunk,
          dropTableIfExists = FALSE,
          createTable = FALSE,
          tempTable = FALSE,
          progressBar = TRUE
        )
      }
    }
    readr::read_csv_chunked(
      file = file.path(resultsFolder, csvFileName),
      callback = uploadChunk,
      chunk_size = 1e7,
      col_types = readr::cols(),
      guess_max = 1e6,
      progress = FALSE
    )
    
    # chunk <- readr::read_csv(file = file.path(resultsFolder, csvFileName),
    # col_types = readr::cols(),
    # guess_max = 1e6)
  }
  else {
    ParallelLogger::logError(csvFileName, " not found")
  }
}


deleteFromServer <-
  function(connection, schema, tableName, keyValues) {
    createSqlStatement <- function(i) {
      sql <- paste0(
        "DELETE FROM ",
        schema,
        ".",
        tableName,
        "\nWHERE ",
        paste(paste0(
          colnames(keyValues), " = '", keyValues[i, ], "'"
        ), collapse = " AND "),
        ";"
      )
      return(sql)
    }
    batchSize <- 1000
    for (start in seq(1, nrow(keyValues), by = batchSize)) {
      end <- min(start + batchSize - 1, nrow(keyValues))
      sql <- sapply(start:end, createSqlStatement)
      sql <- paste(sql, collapse = "\n")
      DatabaseConnector::executeSql(
        connection,
        sql,
        progressBar = FALSE,
        reportOverallTime = FALSE,
        runAsBatch = TRUE
      )
    }
  }

deleteAllRecordsForDatabaseId <- function(connection,
                                          schema,
                                          tableName,
                                          databaseId) {
  sql <-
    "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
  sql <- SqlRender::render(
    sql = sql,
    schema = schema,
    table_name = tableName,
    database_id = databaseId
  )
  databaseIdCount <-
    DatabaseConnector::renderTranslateQuerySql(connection, sql)[, 1]
  if (databaseIdCount != 0) {
    ParallelLogger::logInfo(
      sprintf(
        "- Found %s rows in  database with database ID '%s'. Deleting all before inserting.",
        databaseIdCount,
        databaseId
      )
    )
    sql <-
      "DELETE FROM @schema.@table_name WHERE database_id = '@database_id';"
    sql <- SqlRender::render(
      sql = sql,
      schema = schema,
      table_name = tableName,
      database_id = databaseId
    )
    DatabaseConnector::renderTranslateExecuteSql(connection,
      sql,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
}



# Original uploadResults function from CohortDiagnostics --------------
#' #' Upload results to the database server.
#' #'
#' #' @description
#' #' Requires the results data model tables have been created using the \code{\link{createResultsDataModel}} function.
#' #'
#' #' Set the POSTGRES_PATH environmental variable to the path to the folder containing the psql executable to enable
#' #' bulk upload (recommended).
#' #'
#' #' @param connectionDetails   An object of type \code{connectionDetails} as created using the
#' #'                            \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#' #'                            DatabaseConnector package.
#' #' @param schema         The schema on the postgres server where the tables have been created.
#' #' @param zipFileName    The name of the zip file.
#' #' @param forceOverWriteOfSpecifications  If TRUE, specifications of the phenotypes, cohort definitions, and analysis
#' #'                       will be overwritten if they already exist on the database. Only use this if these specifications
#' #'                       have changed since the last upload.
#' #' @param purgeSiteDataBeforeUploading If TRUE, before inserting data for a specific databaseId all the data for
#' #'                       that site will be dropped. This assumes the input zip file contains the full data for that
#' #'                       data site.
#' #' @param tempFolder     A folder on the local file system where the zip files are extracted to. Will be cleaned
#' #'                       up when the function is finished. Can be used to specify a temp folder on a drive that
#' #'                       has sufficient space if the default system temp space is too limited.
#' #'
#' #' @export
#' uploadResults <- function(connectionDetails = NULL,
#'                           schema,
#'                           zipFileName,
#'                           forceOverWriteOfSpecifications = FALSE,
#'                           purgeSiteDataBeforeUploading = TRUE,
#'                           tempFolder = tempdir()) {
#'   if (connectionDetails$dbms == "sqlite" & schema != "main") {
#'     stop("Invalid schema for sqlite, use schema = 'main'")
#'   }
#' 
#'   start <- Sys.time()
#'   connection <- DatabaseConnector::connect(connectionDetails)
#'   on.exit(DatabaseConnector::disconnect(connection))
#' 
#'   unzipFolder <- tempfile("unzipTempFolder", tmpdir = tempFolder)
#'   dir.create(path = unzipFolder, recursive = TRUE)
#'   on.exit(unlink(unzipFolder, recursive = TRUE), add = TRUE)
#' 
#'   ParallelLogger::logInfo("Unzipping ", zipFileName)
#'   zip::unzip(zipFileName, exdir = unzipFolder)
#' 
#'   specifications <- getResultsDataModelSpecifications()
#' 
#'   if (purgeSiteDataBeforeUploading) {
#'     database <-
#'       readr::read_csv(
#'         file = file.path(unzipFolder, "database.csv"),
#'         col_types = readr::cols()
#'       )
#'     colnames(database) <-
#'       SqlRender::snakeCaseToCamelCase(colnames(database))
#'     databaseId <- database$databaseId
#'   }
#' 
#'   uploadTable <- function(tableName) {
#'     ParallelLogger::logInfo("Uploading table ", tableName)
#' 
#'     primaryKey <- specifications %>%
#'       filter(.data$tableName == !!tableName &
#'         .data$primaryKey == "Yes") %>%
#'       select(.data$fieldName) %>%
#'       pull()
#' 
#'     if (purgeSiteDataBeforeUploading &&
#'       "database_id" %in% primaryKey) {
#'       deleteAllRecordsForDatabaseId(
#'         connection = connection,
#'         schema = schema,
#'         tableName = tableName,
#'         databaseId = databaseId
#'       )
#'     }
#' 
#'     csvFileName <- paste0(tableName, ".csv")
#'     if (csvFileName %in% list.files(unzipFolder)) {
#'       env <- new.env()
#'       env$schema <- schema
#'       env$tableName <- tableName
#'       env$primaryKey <- primaryKey
#'       if (purgeSiteDataBeforeUploading &&
#'         "database_id" %in% primaryKey) {
#'         env$primaryKeyValuesInDb <- NULL
#'       } else if (length(primaryKey) > 0) {
#'         sql <- "SELECT DISTINCT @primary_key FROM @schema.@table_name;"
#'         sql <- SqlRender::render(
#'           sql = sql,
#'           primary_key = primaryKey,
#'           schema = schema,
#'           table_name = tableName
#'         )
#'         primaryKeyValuesInDb <-
#'           DatabaseConnector::querySql(connection, sql)
#'         colnames(primaryKeyValuesInDb) <-
#'           tolower(colnames(primaryKeyValuesInDb))
#'         env$primaryKeyValuesInDb <- primaryKeyValuesInDb
#'       }
#' 
#'       uploadChunk <- function(chunk, pos) {
#'         ParallelLogger::logInfo(
#'           "- Preparing to upload rows ",
#'           pos,
#'           " through ",
#'           pos + nrow(chunk) - 1
#'         )
#' 
#'         chunk <- checkFixColumnNames(
#'           table = chunk,
#'           tableName = env$tableName,
#'           zipFileName = zipFileName,
#'           specifications = specifications
#'         )
#'         chunk <- checkAndFixDataTypes(
#'           table = chunk,
#'           tableName = env$tableName,
#'           zipFileName = zipFileName,
#'           specifications = specifications
#'         )
#'         chunk <- checkAndFixDuplicateRows(
#'           table = chunk,
#'           tableName = env$tableName,
#'           zipFileName = zipFileName,
#'           specifications = specifications
#'         )
#' 
#'         # Primary key fields cannot be NULL, so for some tables convert NAs to empty or zero:
#'         toEmpty <- specifications %>%
#'           filter(
#'             .data$tableName == env$tableName &
#'               .data$emptyIsNa == "No" & grepl("varchar", .data$type)
#'           ) %>%
#'           select(.data$fieldName) %>%
#'           pull()
#'         if (length(toEmpty) > 0) {
#'           chunk <- chunk %>%
#'             dplyr::mutate_at(toEmpty, naToEmpty)
#'         }
#' 
#'         tozero <- specifications %>%
#'           filter(
#'             .data$tableName == env$tableName &
#'               .data$emptyIsNa == "No" &
#'               .data$type %in% c("int", "bigint", "float")
#'           ) %>%
#'           select(.data$fieldName) %>%
#'           pull()
#'         if (length(tozero) > 0) {
#'           chunk <- chunk %>%
#'             dplyr::mutate_at(tozero, naToZero)
#'         }
#' 
#'         # Check if inserting data would violate primary key constraints:
#'         if (!is.null(env$primaryKeyValuesInDb)) {
#'           primaryKeyValuesInChunk <- unique(chunk[env$primaryKey])
#'           duplicates <- inner_join(env$primaryKeyValuesInDb,
#'             primaryKeyValuesInChunk,
#'             by = env$primaryKey
#'           )
#'           if (nrow(duplicates) != 0) {
#'             if ("database_id" %in% env$primaryKey ||
#'               forceOverWriteOfSpecifications) {
#'               ParallelLogger::logInfo(
#'                 "- Found ",
#'                 nrow(duplicates),
#'                 " rows in database with the same primary key ",
#'                 "as the data to insert. Deleting from database before inserting."
#'               )
#'               deleteFromServer(
#'                 connection = connection,
#'                 schema = env$schema,
#'                 tableName = env$tableName,
#'                 keyValues = duplicates
#'               )
#'             } else {
#'               ParallelLogger::logInfo(
#'                 "- Found ",
#'                 nrow(duplicates),
#'                 " rows in database with the same primary key ",
#'                 "as the data to insert. Removing from data to insert."
#'               )
#'               chunk <- chunk %>%
#'                 anti_join(duplicates, by = env$primaryKey)
#'             }
#'             # Remove duplicates we already dealt with:
#'             env$primaryKeyValuesInDb <- env$primaryKeyValuesInDb %>%
#'               anti_join(duplicates, by = env$primaryKey)
#'           }
#'         }
#'         if (nrow(chunk) == 0) {
#'           ParallelLogger::logInfo("- No data left to insert")
#'         } else {
#'           DatabaseConnector::insertTable(
#'             connection = connection,
#'             tableName = env$tableName,
#'             databaseSchema = env$schema,
#'             data = chunk,
#'             dropTableIfExists = FALSE,
#'             createTable = FALSE,
#'             tempTable = FALSE,
#'             progressBar = TRUE
#'           )
#'         }
#'       }
#'       readr::read_csv_chunked(
#'         file = file.path(unzipFolder, csvFileName),
#'         callback = uploadChunk,
#'         chunk_size = 1e7,
#'         col_types = readr::cols(),
#'         guess_max = 1e6,
#'         progress = FALSE
#'       )
#' 
#'       # chunk <- readr::read_csv(file = file.path(unzipFolder, csvFileName),
#'       # col_types = readr::cols(),
#'       # guess_max = 1e6)
#'     }
#'   }
#'   invisible(lapply(unique(specifications$tableName), uploadTable))
#'   delta <- Sys.time() - start
#'   writeLines(paste("Uploading data took", signif(delta, 3), attr(delta, "units")))
#' }
