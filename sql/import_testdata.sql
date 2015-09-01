/*******************************************************************************
**
**  Filename:           import_testdata.sql
**  Customer:           Beach
**  Project:            Pivotal Blog Analysis
**  Created:            August 20th, 2015
**  Updated:
**  Contact:            jvawdrey@pivotal.io
**  Description:        Import script for sample of 57 Sprinkl URLs for testing
**                      * /data/TEST_SAMPLE.csv
**
*******************************************************************************/

-- *****************************************************************************

-- Table DDL
-- DROP TABLE IF EXISTS public.testdata;
CREATE TABLE public.testdata (
  url text
);
-- DISTRIBUTED RANDOMLY;

-- Import data
COPY public.testdata
FROM '/Users/jvawdrey/code/url-redirect-capture/data/TEST_SAMPLE.csv'
DELIMITERS ','
HEADER
NULL  ''
CSV QUOTE '"';
