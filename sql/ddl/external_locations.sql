-- Unity Catalog external location setup template
-- Read-only is disabled by default because READ ONLY is not specified.
-- Replace <storage_credential_name> and <principal_name> before running.
-- NOTE: This file uses Databricks SQL syntax. If your editor/parser is not Databricks-aware,
-- keep statements commented and execute in Databricks SQL Warehouse/Notebook.

-- 1) Storage credential should already exist (platform/IDNAP owned)
-- Example:
-- CREATE STORAGE CREDENTIAL IF NOT EXISTS <storage_credential_name>
-- WITH AZURE_MANAGED_IDENTITY
-- COMMENT 'Credential for ENG511 dropzone access';

-- 2) Create external locations for containers (connect, pace, pia, bisa1, mmia)
-- DEV
-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_dev_connect
-- URL 'abfss://connect@stcbrpubdzeng511dev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 DEV connect container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_dev_pace
-- URL 'abfss://pace@stcbrpubdzeng511dev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 DEV pace container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_dev_pia
-- URL 'abfss://pia@stcbrpubdzeng511dev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 DEV pia container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_dev_bisa1
-- URL 'abfss://bisa1@stcbrpubdzeng511dev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 DEV bisa1 container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_dev_mmia
-- URL 'abfss://mmia@stcbrpubdzeng511dev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 DEV mmia container';

-- TST
-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_tst_connect
-- URL 'abfss://connect@stcbrpubdzeng511tst.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 TST connect container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_tst_pace
-- URL 'abfss://pace@stcbrpubdzeng511tst.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 TST pace container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_tst_pia
-- URL 'abfss://pia@stcbrpubdzeng511tst.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 TST pia container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_tst_bisa1
-- URL 'abfss://bisa1@stcbrpubdzeng511tst.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 TST bisa1 container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_tst_mmia
-- URL 'abfss://mmia@stcbrpubdzeng511tst.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 TST mmia container';

-- PRD
-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_prd_connect
-- URL 'abfss://connect@stcbrpubdzeng511prd.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 PRD connect container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_prd_pace
-- URL 'abfss://pace@stcbrpubdzeng511prd.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 PRD pace container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_prd_pia
-- URL 'abfss://pia@stcbrpubdzeng511prd.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 PRD pia container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_prd_bisa1
-- URL 'abfss://bisa1@stcbrpubdzeng511prd.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 PRD bisa1 container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_prd_mmia
-- URL 'abfss://mmia@stcbrpubdzeng511prd.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 PRD mmia container';

-- 3) Grants (example; replace principal names)
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_connect TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_pace TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_pia TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_bisa1 TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_mmia TO <principal_name>;

-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_tst_connect TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_tst_pace TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_tst_pia TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_tst_bisa1 TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_tst_mmia TO <principal_name>;

-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_prd_connect TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_prd_pace TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_prd_pia TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_prd_bisa1 TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_prd_mmia TO <principal_name>;

-- Copy/paste one statement block at a time into Databricks SQL after replacing tokens.

-- Optional: create external tables directly with location
-- CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table_name>
-- USING DELTA
-- LOCATION 'abfss://connect@stcbrpubdzeng511dev.dfs.core.windows.net/<path>';
