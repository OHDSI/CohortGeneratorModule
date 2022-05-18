# Code for running the module using the job context created using extras/test/CreateJobContext.R

source("Main.R")
jobContext <- readRDS("extras/test/jobContext.rds")
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
jobContext$moduleExecutionSettings$connectionDetails <- connectionDetails

# Force the recreation of the subfolders

if (dir.exists(jobContext$moduleExecutionSettings$workSubFolder)) {
  unlink(jobContext$moduleExecutionSettings$workSubFolder, recursive = TRUE)
}
if (dir.exists(jobContext$moduleExecutionSettings$resultsSubFolder)) {
  unlink(jobContext$moduleExecutionSettings$resultsSubFolder, recursive = TRUE)
}

dir.create(jobContext$moduleExecutionSettings$workSubFolder, recursive = TRUE)
dir.create(jobContext$moduleExecutionSettings$resultsSubFolder, recursive = TRUE)

ParallelLogger::addDefaultFileLogger(file.path(jobContext$moduleExecutionSettings$resultsSubFolder, "log.txt"))
ParallelLogger::addDefaultErrorReportLogger(file.path(jobContext$moduleExecutionSettings$resultsSubFolder, "errorReport.R"))

#debugonce(execute)
execute(jobContext)

ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE)
ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE)
