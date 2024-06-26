# Copyright 2024 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGeneratorModule
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

# Adding library references that are required for Strategus
library(CohortGenerator)
library(DatabaseConnector)
library(keyring)
library(ParallelLogger)
library(SqlRender)

# Adding RSQLite so that we can test modules with Eunomia
library(RSQLite)

# Module methods -------------------------
execute <- function(jobContext) {
  # Setting the readr.num_threads=1 to prevent multi-threading for reading
  # and writing csv files which sometimes causes the module to hang on
  # machines with multiple processors. This option is only overridden
  # in the scope of this function.
  withr::local_options(list(readr.num_threads=1))
  
  rlang::inform("Validating inputs")
  checkmate::assert_list(x = jobContext)
  if (is.null(jobContext$settings)) {
    stop("Analysis settings not found in job context")
  }
  if (is.null(jobContext$sharedResources)) {
    stop("Shared resources not found in job context")
  }
  if (is.null(jobContext$moduleExecutionSettings)) {
    stop("Execution settings not found in job context")
  }

  # Create the cohort definition set
  cohortDefinitionSet <- createCohortDefinitionSetFromJobContext(
    sharedResources = jobContext$sharedResources,
    settings = jobContext$settings
  )

  rlang::inform("Executing")
  # Establish the connection and ensure the cleanup is performed
  connection <- DatabaseConnector::connect(jobContext$moduleExecutionSettings$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))


  # Create the cohort tables
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    cohortTableNames = jobContext$moduleExecutionSettings$cohortTableNames,
    incremental = jobContext$settings$incremental
  )

  # Generate the cohorts
  cohortsGenerated <- CohortGenerator::generateCohortSet(
    connection = connection,
    cohortDefinitionSet = cohortDefinitionSet,
    cdmDatabaseSchema = jobContext$moduleExecutionSettings$cdmDatabaseSchema,
    cohortDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    cohortTableNames = jobContext$moduleExecutionSettings$cohortTableNames,
    incremental = jobContext$settings$incremental,
    incrementalFolder = jobContext$moduleExecutionSettings$workSubFolder
  )

  # Export the results
  rlang::inform("Export data")
  resultsFolder <- jobContext$moduleExecutionSettings$resultsSubFolder
  if (!dir.exists(resultsFolder)) {
    dir.create(resultsFolder, recursive = TRUE)
  }

  # Save the generation information
  if (nrow(cohortsGenerated) > 0) {
    cohortsGenerated$databaseId <- jobContext$moduleExecutionSettings$databaseId
    # Remove any cohorts that were skipped
    cohortsGenerated <- cohortsGenerated[toupper(cohortsGenerated$generationStatus) != "SKIPPED", ]
    cohortsGeneratedFileName <- file.path(resultsFolder, "cohort_generation.csv")
    if (jobContext$settings$incremental) {
      # Format the data for saving
      names(cohortsGenerated) <- SqlRender::camelCaseToSnakeCase(names(cohortsGenerated))
      CohortGenerator::saveIncremental(
        data = cohortsGenerated,
        fileName = cohortsGeneratedFileName,
        cohort_id = cohortsGenerated$cohort_id
      )
    } else {
      CohortGenerator::writeCsv(
        x = cohortsGenerated,
        file = cohortsGeneratedFileName
      )
    }
  }

  cohortCounts <- CohortGenerator::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    cohortTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortTable,
    cohortDefinitionSet = cohortDefinitionSet,
    databaseId = jobContext$moduleExecutionSettings$databaseId
  )
  
  # Filter to columns in the results data model
  cohortCounts <- filterCohortCountsColumns(cohortCounts)

  CohortGenerator::writeCsv(
    x = cohortCounts,
    file = file.path(resultsFolder, "cohort_count.csv")
  )

  # Insert the inclusion rule names before exporting the stats tables
  CohortGenerator::insertInclusionRuleNames(
    connection = connection,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    cohortInclusionTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortInclusionTable
  )

  CohortGenerator::exportCohortStatsTables(
    connection = connection,
    cohortTableNames = jobContext$moduleExecutionSettings$cohortTableNames,
    cohortDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    cohortStatisticsFolder = resultsFolder,
    snakeCaseToCamelCase = FALSE,
    fileNamesInSnakeCase = TRUE,
    incremental = jobContext$settings$incremental,
    databaseId = jobContext$moduleExecutionSettings$databaseId
  )

  # Massage and save the cohort definition set
  colsToRename <- c("cohortId", "cohortName", "sql", "json")
  colInd <- which(names(cohortDefinitionSet) %in% colsToRename)
  cohortDefinitions <- cohortDefinitionSet
  names(cohortDefinitions)[colInd] <- c("cohortDefinitionId", "cohortName", "sqlCommand", "json")
  cohortDefinitions$description <- ""
  CohortGenerator::writeCsv(
    x = cohortDefinitions,
    file = file.path(resultsFolder, "cohort_definition.csv")
  )

  # Generate any negative controls
  if (jobContextHasNegativeControlOutcomeSharedResource(jobContext)) {
    negativeControlOutcomeSettings <- createNegativeControlOutcomeSettingsFromJobContext(jobContext)

    CohortGenerator::generateNegativeControlOutcomeCohorts(
      connection = connection,
      cdmDatabaseSchema = jobContext$moduleExecutionSettings$cdmDatabaseSchema,
      cohortDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
      cohortTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = negativeControlOutcomeSettings$cohortSet,
      tempEmulationSchema = jobContext$moduleExecutionSettings$tempEmulationSchema,
      occurrenceType = negativeControlOutcomeSettings$occurrenceType,
      detectOnDescendants = negativeControlOutcomeSettings$detectOnDescendants,
      incremental = jobContext$settings$incremental,
      incrementalFolder = jobContext$moduleExecutionSettings$workSubFolder      
    )

    cohortCountsNegativeControlOutcomes <- CohortGenerator::getCohortCounts(
      connection = connection,
      cohortDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
      cohortTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortTable,
      databaseId = jobContext$moduleExecutionSettings$databaseId,
      cohortIds = negativeControlOutcomeSettings$cohortSet$cohortId
    )
    
    CohortGenerator::writeCsv(
      x = cohortCountsNegativeControlOutcomes,
      file = file.path(resultsFolder, "cohort_count_neg_ctrl.csv")
    )
  }


  # Set the table names in resultsDataModelSpecification.csv
  moduleInfo <- getModuleInfo()
  resultsDataModel <- CohortGenerator::readCsv(
    file = "resultsDataModelSpecification.csv",
    warnOnCaseMismatch = FALSE
  )
  newTableNames <- paste0(moduleInfo$TablePrefix, resultsDataModel$tableName)
  file.rename(
    file.path(resultsFolder, paste0(unique(resultsDataModel$tableName), ".csv")),
    file.path(resultsFolder, paste0(unique(newTableNames), ".csv"))
  )
  resultsDataModel$tableName <- newTableNames
  CohortGenerator::writeCsv(
    x = resultsDataModel,
    file = file.path(resultsFolder, "resultsDataModelSpecification.csv"),
    warnOnCaseMismatch = FALSE,
    warnOnFileNameCaseMismatch = FALSE,
    warnOnUploadRuleViolations = FALSE
  )

  # Zip the results
  zipFile <- file.path(resultsFolder, "cohortGeneratorResults.zip")
  resultFiles <- list.files(resultsFolder,
    pattern = ".*\\.csv$"
  )
  oldWd <- setwd(resultsFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(
    zipFile = zipFile,
    files = resultFiles
  )
  rlang::inform(paste("Results available at:", zipFile))
}

createDataModelSchema <- function(jobContext) {
  checkmate::assert_class(jobContext$moduleExecutionSettings$resultsConnectionDetails, "ConnectionDetails")
  checkmate::assert_string(jobContext$moduleExecutionSettings$resultsDatabaseSchema)
  connectionDetails <- jobContext$moduleExecutionSettings$resultsConnectionDetails
  moduleInfo <- getModuleInfo()
  tablePrefix <- moduleInfo$TablePrefix
  resultsDatabaseSchema <- jobContext$moduleExecutionSettings$resultsDatabaseSchema
  # Workaround for issue https://github.com/tidyverse/vroom/issues/519:
  readr::local_edition(1)
  resultsDataModel <- ResultModelManager::loadResultsDataModelSpecifications(
    filePath = "resultsDataModelSpecification.csv"
  )
  resultsDataModel$tableName <- paste0(tablePrefix, resultsDataModel$tableName)
  sql <- ResultModelManager::generateSqlSchema(
    schemaDefinition = resultsDataModel
  )
  sql <- SqlRender::render(
    sql = sql,
    database_schema = resultsDatabaseSchema
  )
  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(DatabaseConnector::disconnect(connection))
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql
  )
}

# Private methods -------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}

# This private function makes testing the call bit easier
.getCohortDefinitionSetFromSharedResource <- function(cohortDefinitionSharedResource, settings) {
  cohortDefinitions <- cohortDefinitionSharedResource$cohortDefinitions
  if (length(cohortDefinitions) <= 0) {
    stop("No cohort definitions found")
  }
  cohortDefinitionSet <- CohortGenerator::createEmptyCohortDefinitionSet()
  for (i in 1:length(cohortDefinitions)) {
    cohortJson <- cohortDefinitions[[i]]$cohortDefinition
    cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
    cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = settings$generateStats))
    cohortDefinitionSet <- rbind(cohortDefinitionSet, data.frame(
      cohortId = as.double(cohortDefinitions[[i]]$cohortId),
      cohortName = cohortDefinitions[[i]]$cohortName,
      sql = cohortSql,
      json = cohortJson,
      stringsAsFactors = FALSE
    ))
  }

  if (length(cohortDefinitionSharedResource$subsetDefs)) {
    subsetDefinitions <- lapply(cohortDefinitionSharedResource$subsetDefs, CohortGenerator::CohortSubsetDefinition$new)
    for (subsetDef in subsetDefinitions) {
      ind <- which(sapply(cohortDefinitionSharedResource$cohortSubsets, function(y) subsetDef$definitionId %in% y$subsetId))
      targetCohortIds <- unlist(lapply(cohortDefinitionSharedResource$cohortSubsets[ind], function(y) y$targetCohortId))
      cohortDefinitionSet <- CohortGenerator::addCohortSubsetDefinition(
        cohortDefinitionSet = cohortDefinitionSet,
        cohortSubsetDefintion = subsetDef,
        targetCohortIds = targetCohortIds
      )
    }
  }

  return(cohortDefinitionSet)
}

filterCohortCountsColumns <- function(cohortCounts) {
  # Filter to columns in the results data model
  return(cohortCounts[c("databaseId", "cohortId", "cohortEntries", "cohortSubjects")])
}

createCohortDefinitionSetFromJobContext <- function(sharedResources, settings) {
  cohortDefinitions <- list()
  if (length(sharedResources) <= 0) {
    stop("No shared resources found")
  }
  cohortDefinitionSharedResource <- getSharedResourceByClassName(
    sharedResources = sharedResources,
    class = "CohortDefinitionSharedResources"
  )
  if (is.null(cohortDefinitionSharedResource)) {
    stop("Cohort definition shared resource not found!")
  }

  if ((is.null(cohortDefinitionSharedResource$subsetDefs) && !is.null(cohortDefinitionSharedResource$cohortSubsets)) ||
    (!is.null(cohortDefinitionSharedResource$subsetDefs) && is.null(cohortDefinitionSharedResource$cohortSubsets))) {
    stop("Cohort subset functionality requires specifying cohort subset definition & cohort subset identifiers.")
  }

  cohortDefinitionSet <- .getCohortDefinitionSetFromSharedResource(
    cohortDefinitionSharedResource = cohortDefinitionSharedResource,
    settings = settings
  )
  return(cohortDefinitionSet)
}

jobContextHasNegativeControlOutcomeSharedResource <- function(jobContext) {
  ncSharedResource <- getSharedResourceByClassName(
    sharedResources = jobContext$sharedResources,
    className = "NegativeControlOutcomeSharedResources"
  )
  hasNegativeControlOutcomeSharedResource <- !is.null(ncSharedResource)
  invisible(hasNegativeControlOutcomeSharedResource)
}

createNegativeControlOutcomeSettingsFromJobContext <- function(jobContext) {
  negativeControlSharedResource <- getSharedResourceByClassName(
    sharedResources = jobContext$sharedResources,
    className = "NegativeControlOutcomeSharedResources"
  )
  if (is.null(negativeControlSharedResource)) {
    stop("Negative control outcome shared resource not found!")
  }
  negativeControlOutcomes <- negativeControlSharedResource$negativeControlOutcomes$negativeControlOutcomeCohortSet
  if (length(negativeControlOutcomes) <= 0) {
    stop("No negative control outcomes found")
  }
  negativeControlOutcomeCohortSet <- CohortGenerator::createEmptyNegativeControlOutcomeCohortSet()
  for (i in 1:length(negativeControlOutcomes)) {
    nc <- negativeControlOutcomes[[i]]
    negativeControlOutcomeCohortSet <- rbind(
      negativeControlOutcomeCohortSet,
      data.frame(
        cohortId = bit64::as.integer64(nc$cohortId),
        cohortName = nc$cohortName,
        outcomeConceptId = bit64::as.integer64(nc$outcomeConceptId)
      )
    )
  }
  invisible(list(
    cohortSet = negativeControlOutcomeCohortSet,
    occurrenceType = negativeControlSharedResource$negativeControlOutcomes$occurrenceType,
    detectOnDescendants = negativeControlSharedResource$negativeControlOutcomes$detectOnDescendants
  ))
}

getSharedResourceByClassName <- function(sharedResources, className) {
  returnVal <- NULL
  for (i in 1:length(sharedResources)) {
    if (className %in% class(sharedResources[[i]])) {
      returnVal <- sharedResources[[i]]
      break
    }
  }
  invisible(returnVal)
}
