CohortGeneratorModule 0.2.1
=======================
- Update to use CohortGenerator v0.8.1

CohortGeneratorModule 0.2.0
=======================
- Updated module to use HADES wide lock file and updated to use renv v1.0.2
- Added functions and tests for creating the results data model for use by Strategus upload functionality
- Added additional GitHub Action tests to unit test the module functionality on HADES supported R version (v4.2.3) and the latest release of R

CohortGeneratorModule 0.1.1
=======================

Bump dependencies and add keyring, Strategus as dependencies
Bug fixes for Google BigQuery - thanks @ablack3

CohortGeneratorModule 0.1.0
=======================

- Use CohortGenerator v0.8.0 to support cohort subsets

CohortGeneratorModule 0.0.16-1
=======================

- Bump CohortGenerator to develop branch to test subset functionality
- Bump DatabaseConnector & SqlRender to fix #15

CohortGeneratorModule 0.0.16
=======================

- Make inclusion rule description optional (#14)

CohortGeneratorModule 0.0.15
=======================

- Add explicit reference to aws.s3 to renv.lock file

CohortGeneratorModule 0.0.14
=======================

- Updated referenced to CirceR v1.2.1

CohortGeneratorModule 0.0.13
=======================

- Using released version of CohortGenerator v0.7.0

CohortGeneratorModule 0.0.12
=======================

- Using preview version of CohortGenerator v0.7.0

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