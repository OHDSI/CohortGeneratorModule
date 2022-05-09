{DEFAULT @primary_key = ''}


IF OBJECT_ID('@results_schema.@table', 'U') IS NOT NULL 
DROP TABLE @results_schema.@table;


--Table @table
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @results_schema.@table (
    @columns
    {@primary_key != ''}?{
      , PRIMARY KEY(@primary_key)
    }
);

