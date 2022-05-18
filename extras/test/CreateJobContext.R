library(Strategus)
library(dplyr)
source("SettingsFunctions.R")

# Generic Helpers ----------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}

# Sample Data Helpers ----------------------------
getSampleCohortDefintionSet <- function() {
  sampleCohorts <- CohortGenerator::createEmptyCohortDefinitionSet()
  cohortJsonFiles <- list.files(path = system.file("testdata/name/cohorts", package = "CohortGenerator"), full.names = TRUE)
  for (i in 1:length(cohortJsonFiles)) {
    cohortJsonFileName <- cohortJsonFiles[i]
    cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
    cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
    sampleCohorts <- rbind(sampleCohorts, data.frame(
      cohortId = i,
      cohortName = cohortName,
      cohortDefinition = cohortJson,
      stringsAsFactors = FALSE
    ))
  }
  sampleCohorts <- apply(sampleCohorts, 1, as.list)
  return(sampleCohorts)
}

createCohortSharedResource <- function(cohortDefinitionSet) {
  # Fill the cohort set using  cohorts included in this
  # package as an example
  sharedResource <- list(cohortDefinitions = cohortDefinitionSet)
  class(sharedResource) <- c("CohortDefinitionSharedResources", "SharedResources")
  return(sharedResource)
}

analysisSpecifications <- createEmptyAnalysisSpecificiations() %>%
  addSharedResources(createCohortSharedResource(getSampleCohortDefintionSet())) %>%
  addModuleSpecifications(createCohortGeneratorModuleSpecifications())

ParallelLogger::saveSettingsToJson(analysisSpecifications, "extras/test/testAnalysisSpecifications.json")

# Module Settings Spec ----------------------------
# Note: Need to do only once: store connection details in keyring:
connectionDetailsReference <- "Eunomia"
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

Strategus::storeConnectionDetails(
  connectionDetails = connectionDetails,
  connectionDetailsReference = connectionDetailsReference
)

executionSettings <- Strategus::createExecutionSettings(
  connectionDetailsReference = connectionDetailsReference,
  workDatabaseSchema = "main",
  cdmDatabaseSchema = "main",
  cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "strategus_test"),
  workFolder = file.path(getwd(), "extras/test/output/work"),
  resultsFolder = file.path(getwd(), "extras/test/output/results"),
  minCellCount = 5
)

ParallelLogger::saveSettingsToJson(executionSettings, "extras/test/testExecutionSettings.json")

# Job Context ----------------------------
module <- "CohortGeneratorModule"
moduleIndex <- 1
moduleExecutionSettings <- executionSettings
moduleExecutionSettings$workSubFolder <- file.path(executionSettings$workFolder, sprintf("%s_%d", module, moduleIndex))
moduleExecutionSettings$resultsSubFolder <- file.path(executionSettings$resultsFolder, sprintf("%s_%d", module, moduleIndex))
moduleExecutionSettings$databaseId <- 123
jobContext <- list(
  sharedResources = analysisSpecifications$sharedResources,
  settings = analysisSpecifications$moduleSpecifications[[moduleIndex]]$settings,
  moduleExecutionSettings = moduleExecutionSettings
)
saveRDS(jobContext, "extras/test/jobContext.rds")
