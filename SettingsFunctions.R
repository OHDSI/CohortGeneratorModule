createCohortGeneratorModuleSpecifications <- function(incremental = TRUE,
                                                      generateStats = TRUE) {
  analysis <- list()
  for (name in names(formals(createCohortGeneratorModuleSpecifications))) {
    analysis[[name]] <- get(name)
  }

  specifications <- list(module = "CohortGeneratorModule",
                         version = "0.0.6",
                         remoteRepo = "github.com",
                         remoteUsername = "ohdsi",
                         settings = analysis)
  class(specifications) <- c("CohortGeneratorModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}