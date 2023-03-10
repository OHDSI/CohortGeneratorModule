# Copyright 2023 Observational Health Data Sciences and Informatics
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

#' Create specifications for the CohortGeneratorModule
#'
#' @param incremental Should the CohortGenerator module run in incremental mode?
#'
#' @param generateStats Should the CohortGenerator module generate cohort statistics?
#'
#'
#' @return
#' An object of type `CohortGeneratorModuleSpecifications`.
#'
#' @export
createCohortGeneratorModuleSpecifications <- function(incremental = TRUE,
                                                      generateStats = TRUE) {
  analysis <- list()
  for (name in names(formals(createCohortGeneratorModuleSpecifications))) {
    analysis[[name]] <- get(name)
  }

  specifications <- list(
    module = "%module%",
    version = "%version%",
    remoteRepo = "github.com",
    remoteUsername = "ohdsi",
    settings = analysis
  )
  class(specifications) <- c("CohortGeneratorModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}

#' Create shared specifications for the cohort definition set
#'
#' @param cohortDefinitionSet The cohortDefintionSet holds the cohortId, cohortName and json
#'                            specification for the cohorts of interest.
#'
#' @return
#' An object of type `CohortDefinitionSharedResources`.
#'
#' @export
createCohortSharedResourceSpecifications <- function(cohortDefinitionSet) {
  if (!CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet)) {
    stop("cohortDefinitionSet is not properly defined")
  }

  subsetDefinitions <- CohortGenerator::getSubsetDefinitions(cohortDefinitionSet)
  if (length(subsetDefinitions) > 0) {
    # Filter the cohort definition set to the "parent" cohorts.
    parentCohortDefinitionSet <- cohortDefinitionSet[!cohortDefinitionSet$isSubset, ]
  } else {
    parentCohortDefinitionSet <- cohortDefinitionSet
  }

  sharedResource <- list()

  listafy <- function(df) {
    mylist <- list()
    for (i in 1:nrow(df)) {
      cohortData <- list(
        cohortId = df$cohortId[i],
        cohortName = df$cohortName[i],
        cohortDefinition = df$json[i]
      )
      mylist[[i]] <- cohortData
    }
    return(mylist)
  }

  cohortDefinitionSetFiltered <- listafy(parentCohortDefinitionSet)
  sharedResource["cohortDefinitions"] <- list(cohortDefinitionSetFiltered)

  if (length(subsetDefinitions)) {
    # Subset definitions
    subsetDefinitionsJson <- lapply(subsetDefinitions, function(x) {
      x$toJSON()
    })
    sharedResource["subsetDefs"] <- list(subsetDefinitionsJson)

    # Filter to the subsets
    subsetCohortDefinitionSet <- cohortDefinitionSet[cohortDefinitionSet$isSubset, ]
    subsetIdMapping <- list()
    for (i in 1:nrow(subsetCohortDefinitionSet)) {
      idMapping <- list(
        cohortId = subsetCohortDefinitionSet$cohortId[i],
        subsetId = subsetCohortDefinitionSet$subsetDefinitionId[i],
        targetCohortId = subsetCohortDefinitionSet$subsetParent[i]
      )
      subsetIdMapping[[i]] <- idMapping
    }
    sharedResource["cohortSubsets"] <- list(subsetIdMapping)
  }


  class(sharedResource) <- c("CohortDefinitionSharedResources", "SharedResources")
  return(sharedResource)
}

#' Create shared specifications for the negative control outcome
#' cohort set
#'
#' @param negativeControlOutcomeCohortSet	The negativeControlOutcomeCohortSet argument
#' must be a data frame with the following columns: cohortId, cohortName, outcomeConceptId
#'
#' @param occurrenceType The occurrenceType will detect either: the first time an
#'                       outcomeConceptId occurs or all times the outcomeConceptId
#'                       occurs for a person. Values accepted: 'all' or 'first'.
#'
#' @param detectOnDescendants When set to TRUE, detectOnDescendants will use the vocabulary
#'                            to find negative control outcomes using the outcomeConceptId and all
#'                            descendants via the concept_ancestor table. When FALSE, only the exact
#'                            outcomeConceptId will be used to detect the outcome.
#'
#' @return
#' An object of type `CohortDefinitionSharedResources`.
#'
#' @export
createNegativeControlOutcomeCohortSharedResourceSpecifications <- function(negativeControlOutcomeCohortSet,
                                                                           occurrenceType,
                                                                           detectOnDescendants) {
  negativeControlOutcomeCohortSet <- apply(negativeControlOutcomeCohortSet, 1, as.list)
  sharedResource <- list(
    negativeControlOutcomes = list(
      negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet,
      occurrenceType = occurrenceType,
      detectOnDescendants = detectOnDescendants
    )
  )
  class(sharedResource) <- c("NegativeControlOutcomeSharedResources", "SharedResources")
  return(sharedResource)
}
