source("ResultsDataModel.R")

# Test Harness -------
resultsDatabaseConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                               server = keyring::key_get("ASSURE_RESULTS_SERVER"),
                                                                               user = keyring::key_get("ASSURE_RESULTS_USER"),
                                                                               password = keyring::key_get("ASSURE_RESULTS_PASSWORD"))
conn <- DatabaseConnector::connect(connectionDetails = resultsDatabaseConnectionDetails)
resultsDataModelSpecifications <- getResultsDataModelSpecifications(pathToCsv = "resultsDataModelSpecification.csv")

# TODO: Who/what is responsible for setting up the "cohort_generator" schema?
# Results data model is now created by the contents of the CSV.
createResultsDataModel(connection = conn,
                       schema = "cohort_generator",
                       specifications = resultsDataModelSpecifications)

# Test the upload
library(dplyr)
resultsFolder <- "D:/git/anthonysena/CohortGeneratorModule/extras/output/results/CohortGeneratorModule_1"
files <- list.files(path = resultsFolder, pattern = ".csv")
for (i in 1:length(files)) {
  tableName <- tools::file_path_sans_ext(files[i])
  uploadTable(connection = conn,
              schema = "cohort_generator",
              databaseId = "Eunomia",
              tableName = tableName,
              resultsFolder = resultsFolder,
              purgeSiteDataBeforeUploading = TRUE,
              specifications = resultsDataModelSpecifications)
  
}

# Other Notes --------------
# 1. The original code from CohortDiagnostics uses a parameter
#    forceOverWriteOfSpecifications = FALSE. Should Strategus automatically
#    save the JSON for a given run as opposed to each module? The modules
#    should have some reference to the specifications for the results viewer...
# 2. We need standards for the ways in which we capture output. Some proposed
#    rules:
#    - Snake case for CSV file names and column names
#    - CSV file names represent the table name
#    - Table names will be singular (i.e. cohort_count not cohort_counts)
