-- IDNAP REQUEST PACK (DEV) - External Locations
-- Purpose: ready-to-run statement set for ENG511 DEV container external locations.
-- Requester context: connect + pace requested explicitly (read_only=false), with optional pia/bisa1/mmia.
-- IMPORTANT: Replace tokens before execution:
--   <storage_credential_name>
--   <principal_name>

-- Connect + Pace (requested)
-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_dev_connect
-- URL 'abfss://connect@stcbrpubdzeng511dev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 DEV connect container';

-- CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_dev_pace
-- URL 'abfss://pace@stcbrpubdzeng511dev.dfs.core.windows.net/'
-- WITH (STORAGE CREDENTIAL <storage_credential_name>)
-- COMMENT 'ENG511 DEV pace container';

-- Optional containers (if ticket scope includes pia/bisa1/mmia)
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

-- Access grants (read_only=false => READ FILES + WRITE FILES)
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_connect TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_pace TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_pia TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_bisa1 TO <principal_name>;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_dev_mmia TO <principal_name>;

-- Validation queries
-- SHOW EXTERNAL LOCATIONS LIKE 'ext_loc_dev_%';
-- SHOW GRANTS ON EXTERNAL LOCATION ext_loc_dev_connect;
-- SHOW GRANTS ON EXTERNAL LOCATION ext_loc_dev_pace;
