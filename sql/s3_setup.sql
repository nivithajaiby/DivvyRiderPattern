-- ============================================================
-- S3 BRONZE LAYER SETUP  —  DivvyRiderPatterns
-- Loads raw files from AWS S3 into Snowflake RAW via a secure
-- storage integration (no access keys stored in Snowflake).
--
-- Run order: 1 -> 2 -> (configure AWS trust) -> 3 -> 4 -> 5
-- Requires ACCOUNTADMIN for the storage integration.
-- ============================================================


-- ------------------------------------------------------------
-- STEP 1.  Create the storage integration  (account-level)
-- Links Snowflake to the AWS IAM role that can read the bucket.
-- ------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION divvy_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::479925391576:role/divvy-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://divvy-rider-pattern/');


-- ------------------------------------------------------------
-- STEP 2.  Read back the values needed for the AWS trust policy
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into
-- the IAM role's trust relationship in AWS, then continue.
-- ------------------------------------------------------------
DESC INTEGRATION divvy_s3_integration;


-- ------------------------------------------------------------
-- STEP 3.  Create the external stage  (schema-level)
-- A pointer to the S3 bucket, using the integration above.
-- ------------------------------------------------------------
USE DATABASE DIVVY;
USE SCHEMA RAW;

CREATE OR REPLACE STAGE divvy_s3_stage
  STORAGE_INTEGRATION = divvy_s3_integration
  URL = 's3://divvy-rider-pattern/'
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

-- Prove connectivity: should list the uploaded CSVs.
LIST @divvy_s3_stage;


-- ------------------------------------------------------------
-- STEP 4.  Load RAW tables from S3
-- TRUNCATE first so a re-run does not append duplicates, then
-- COPY INTO from the matching S3 folder. Data now flows through
-- S3 (Bronze) into Snowflake RAW.
-- ------------------------------------------------------------

-- Trips  (folder: divvytrips/ ; 6 monthly files load together)
TRUNCATE TABLE DIVVY.RAW.DIVVY_TRIPS;
COPY INTO DIVVY.RAW.DIVVY_TRIPS
  FROM @divvy_s3_stage/divvytrips/
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
  ON_ERROR = 'CONTINUE';

-- Weather  (folder: weather/)
TRUNCATE TABLE DIVVY.RAW.WEATHER_HOURLY;
COPY INTO DIVVY.RAW.WEATHER_HOURLY
  FROM @divvy_s3_stage/weather/
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
  ON_ERROR = 'CONTINUE';

-- Holidays  (folder: holidays/)
TRUNCATE TABLE DIVVY.RAW.HOLIDAYS;
COPY INTO DIVVY.RAW.HOLIDAYS
  FROM @divvy_s3_stage/holidays/
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
  ON_ERROR = 'CONTINUE';


-- ------------------------------------------------------------
-- STEP 5.  Verify row counts
-- ------------------------------------------------------------
SELECT COUNT(*) AS trips    FROM DIVVY.RAW.DIVVY_TRIPS;      -- ~2,278,732
SELECT COUNT(*) AS weather  FROM DIVVY.RAW.WEATHER_HOURLY;   -- hourly readings
SELECT COUNT(*) AS holidays FROM DIVVY.RAW.HOLIDAYS;         -- ~6

-- After this, run `dbt run` + `dbt test` to rebuild Silver/Gold
-- from the S3-sourced RAW data.