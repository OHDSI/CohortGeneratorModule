CohortGeneratorModule 0.0.11
=======================

- Update code to ensure inclusion rule names are included.
- Upgrade to use CohortGenerator v0.6.0

CohortGeneratorModule 0.0.10
=======================

- Update resultsDataModelSpecification.csv to use database_id as varchar
- Bug fix: json and sqlCommand were swapped

CohortGeneratorModule 0.0.9
=======================

- Use DatabaseConnector v5.0.4
- Update resultsDataModelSpecification.csv to use database_id as bigint and to 
use new format for OhdsiSharing

CohortGeneratorModule 0.0.8
=======================

Fix auto-generated SettingsFunction.R to use proper function name
`createCohortGeneratorModuleSpecifications`

CohortGeneratorModule 0.0.7
=======================

Using an experimental version of DatabaseConnector

CohortGeneratorModule 0.0.6
=======================

Adding cohort_definition information to the output and adding unit tests
to ensure all renv dependencies are available across platforms.

CohortGeneratorModule 0.0.5
=======================

Fixing bug with renv.lock file which had an incorrect reference to the 
CohortGenerator package.

CohortGeneratorModule 0.0.4
=======================

Fixing bug with to use jobContext$moduleExecutionSettings$databaseId for the
databaseId in the results. This value will be set by Strategus.

CohortGeneratorModule 0.0.3
=======================

Fixing bug with cohort_generation.csv which was not written in snake_case
format.

CohortGeneratorModule 0.0.2
=======================

Using released version of CohortGenerator v0.5.0 and adding CreateModules.Rmd
to help guide development of new modules.

CohortGeneratorModule 0.0.1
=======================

Initial version