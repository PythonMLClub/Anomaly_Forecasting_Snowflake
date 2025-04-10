## Anomaly_Forecasting_Snowflake
This repository provides a comprehensive Snowflake SQL script for performing anomaly detection and time-series forecasting using Snowflake's built-in machine learning functions (SNOWFLAKE.ML.ANOMALY_DETECTION and SNOWFLAKE.ML.FORECAST). The script demonstrates how to set up a Snowflake environment, load data, train models, detect anomalies, and generate forecasts for a dataset containing building-related payment data.

## 📌 Project Objectives
Detect anomalies in payment amounts (PAID_AMT) across time for buildings.
Forecast future payment amounts using historical trends.
Compare model performance with and without known anomaly labels.
Store and evaluate predictions in Snowflake tables for further analysis.

## 🧱 Step-by-Step Instructions for GitHub Users
Follow these steps to set up and run the anomaly detection and forecasting project in Snowflake using this repository.

### Step 1: Clone the Repository
1. Open a terminal or command prompt.
2. Run the following command to clone the repository:
git clone https://github.com/<your-username>/Anomaly_Forecasting_Snowflake.git
3. Navigate into the project directory:
cd Anomaly_Forecasting_Snowflake

### Step 2: Prepare Your Snowflake Environment
1. Log in to your Snowflake account using a user with ACCOUNTADMIN privileges.
2. Open the provided script file (e.g., anomaly_forecasting.sql) in a text editor or Snowflake worksheet.
3. Execute the commands under -- Step 1: Setting Up the Environment:
Creates a role (ANOMALY_ROLE) and assigns it to a user (replace ANOMALYFORCASTING with your username).
Sets up a database (anomaly_db), schema (anomaly_schema), and warehouse (anomaly_wh).

- USE ROLE accountadmin;
- CREATE OR REPLACE ROLE anomaly_role;
- GRANT CREATE DATABASE ON ACCOUNT TO ROLE anomaly_role;
-  GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE anomaly_role;
- GRANT ROLE anomaly_role TO USER <your-username>;
- USE ROLE anomaly_role;
- CREATE OR REPLACE DATABASE anomaly_db;
- CREATE OR REPLACE SCHEMA anomaly_db.anomaly_schema;
- CREATE OR REPLACE WAREHOUSE anomaly_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;
- GRANT USAGE ON DATABASE anomaly_db TO ROLE anomaly_role;
- GRANT USAGE ON SCHEMA anomaly_db.anomaly_schema TO ROLE anomaly_role;
- GRANT CREATE SNOWFLAKE.ML.ANOMALY_DETECTION ON SCHEMA anomaly_db.anomaly_schema TO ROLE anomaly_role;

### Step 3: Load Your Data
1. Prepare a CSV file named anomaly.csv with the required columns (e.g., BUILDING_ID, PAID_YEAR_MONTH_TM, PAID_AMT, etc.). See the CLM_FACT table structure in the script for details.
2. Upload the CSV file to Snowflake’s internal stage:
Use the Snowflake UI (navigate to Databases > anomaly_db > anomaly_schema > Stages > anomaly_stage) or the CLI:
snowsql -a <your-account> -u <your-username> -q "PUT file://path/to/anomaly.csv @anomaly_stage"
3. Run the commands under -- Step 2: Loading and Preparing the Data to create the stage, define the file format, and load data:

- CREATE OR REPLACE STAGE anomaly_stage;
- GRANT READ ON STAGE anomaly_db.anomaly_schema.anomaly_stage TO ROLE anomaly_role;
- GRANT WRITE ON STAGE anomaly_db.anomaly_schema.anomaly_stage TO ROLE anomaly_role;
- CREATE OR REPLACE TABLE anomaly_db.anomaly_schema.CLM_FACT (...); -- Full table definition in script
- CREATE OR REPLACE FILE FORMAT anomaly_csv_format TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' PARSE_HEADER = TRUE TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS' FIELD_DELIMITER = ',';
- COPY INTO anomaly_db.anomaly_schema.CLM_FACT FROM @anomaly_stage/anomaly.csv FILE_FORMAT = anomaly_csv_format MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = 'CONTINUE';
- SELECT * FROM anomaly_db.anomaly_schema.CLM_FACT; -- Verify data

### Step 4: Create Training and Testing Views
Execute the commands under -- Step 3 and -- Step 4 to split data into training and testing views:

- CREATE OR REPLACE VIEW anomaly_db.anomaly_schema.anomaly_vw_train AS
SELECT * FROM anomaly_db.anomaly_schema.CLM_FACT WHERE PAID_YEAR_MONTH_TM < '2016-01-14 21:00:00.000';
- CREATE OR REPLACE VIEW anomaly_db.anomaly_schema.anomaly_vw_test AS
SELECT * FROM anomaly_db.anomaly_schema.CLM_FACT WHERE PAID_YEAR_MONTH_TM >= '2016-01-16 21:00:00.000';

### Step 5: Run Unsupervised Anomaly Detection
1. Train the unsupervised model by running -- Step 6:

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION admodel_withoutlabel (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'anomaly_vw_train'),
    TIMESTAMP_COLNAME => 'PAID_YEAR_MONTH_TM',
    TARGET_COLNAME => 'PAID_AMT',
    LABEL_COLNAME => ''
);
2. Detect anomalies on the test data and save results with -- Step 7:

BEGIN
    CALL admodel_withoutlabel!DETECT_ANOMALIES(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'anomaly_vw_test'),
        TIMESTAMP_COLNAME => 'PAID_YEAR_MONTH_TM',
        TARGET_COLNAME => 'PAID_AMT',
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    LET x := SQLID;
    CREATE OR REPLACE TABLE anomaly_detection_without_label AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

SELECT * FROM anomaly_detection_without_label;

### Step 6: Run Supervised Anomaly Detection (Optional)
1. If your data includes an ANOMALY column with labels, train the supervised model with -- Step 8:

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION admodel_withlabel (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'anomaly_vw_train'),
    TIMESTAMP_COLNAME => 'PAID_YEAR_MONTH_TM',
    TARGET_COLNAME => 'PAID_AMT',
    LABEL_COLNAME => 'ANOMALY'
);
2 . Detect anomalies and save results with -- Step 9:

BEGIN
    CALL admodel_withlabel!DETECT_ANOMALIES(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'anomaly_vw_test'),
        TIMESTAMP_COLNAME => 'PAID_YEAR_MONTH_TM',
        TARGET_COLNAME => 'PAID_AMT',
        CONFIG_OBJECT => {'prediction_interval': 0.9999}
    );
    LET x := SQLID;
    CREATE OR REPLACE TABLE anomaly_detection_with_label AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

SELECT * FROM anomaly_detection_with_label;

### Step 7: Compare Anomaly Detection Models
1 . Run -- Step 10 to compare the number of anomalies detected:

SELECT 'Without Label' AS model, COUNT(*) AS anomalies_detected
FROM anomaly_detection_without_label WHERE IS_ANOMALY = TRUE
UNION ALL
SELECT 'With Label' AS model, COUNT(*) AS anomalies_detected
FROM anomaly_detection_with_label WHERE IS_ANOMALY = TRUE;

### Step 8: Generate Forecasts
1 . Train the forecasting model under -- CREATE PREDICTIONS:

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST model_forecast (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ANOMALY_VW_TRAIN'),
    SERIES_COLNAME => 'BUILDING_ID',
    TIMESTAMP_COLNAME => 'PAID_YEAR_MONTH_TM',
    TARGET_COLNAME => 'PAID_AMT',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);
2 . Generate and save forecasts:

CREATE OR REPLACE TABLE forecast_prediction AS
SELECT * FROM TABLE(model_forecast!FORECAST(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ANOMALY_VW_TEST'),
    SERIES_COLNAME => 'BUILDING_ID',
    TIMESTAMP_COLNAME => 'PAID_YEAR_MONTH_TM'
));
SELECT * FROM forecast_prediction;

### Step 9: Evaluate Models
1. Check model performance with -- Step 11:

- CALL admodel_withoutlabel!SHOW_EVALUATION_METRICS();
- CALL admodel_withoutlabel!EXPLAIN_FEATURE_IMPORTANCE();
- CALL model_forecast!SHOW_EVALUATION_METRICS();
- CALL model_forecast!EXPLAIN_FEATURE_IMPORTANCE();

## 📊 Example Output
![image](https://github.com/user-attachments/assets/4030f509-450f-447d-84f3-33f3653bd452)

## Notes
Replace <your-username> with your GitHub username and Snowflake username where applicable.
Ensure your Snowflake account has access to SNOWFLAKE.ML functions.
Adjust the CSV file path and column names as needed to match your data.
