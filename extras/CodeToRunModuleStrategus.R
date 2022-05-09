source('Main.R')
jobContext <- readRDS("extras/jobContext.rds")
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
jobContext$moduleExecutionSettings$connectionDetails <- connectionDetails

# Force the recreation of the subfolders

if (dir.exists(jobContext$moduleExecutionSettings$workSubFolder)) {
  unlink(jobContext$moduleExecutionSettings$workSubFolder)
}
if (dir.exists(jobContext$moduleExecutionSettings$resultsSubFolder)) {
  unlink(jobContext$moduleExecutionSettings$resultsSubFolder)
}

dir.create(jobContext$moduleExecutionSettings$workSubFolder, recursive = TRUE)
dir.create(jobContext$moduleExecutionSettings$resultsSubFolder, recursive = TRUE)

ParallelLogger::addDefaultFileLogger(file.path(jobContext$moduleExecutionSettings$resultsSubFolder, 'log.txt'))
ParallelLogger::addDefaultErrorReportLogger(file.path(jobContext$moduleExecutionSettings$resultsSubFolder, 'errorReport.R'))

#debug(validate)
validate(jobContext)
debugonce(execute)
execute(jobContext)

ParallelLogger::unregisterLogger('DEFAULT_FILE_LOGGER', silent = TRUE)
ParallelLogger::unregisterLogger('DEFAULT_ERRORREPORT_LOGGER', silent = TRUE)