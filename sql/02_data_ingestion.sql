-- 1. Create a file format for CSV
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null');

create or replace stage customer_ext_stage
  url='s3://bucket-nifi-asjal/'
  credentials=(aws_key_id='*************' aws_secret_key='**********************')
  file_format = csv_format;
  
show stages;
list @customer_ext_stage;


-- create or replace pipe customer_s3_pipe
--   auto_ingest = true
--   as
--   copy into customer_raw
--   from @customer_ext_stage/customer_20210806183233.csv
--   file_format = CSV
--   ;

CREATE OR REPLACE PIPE customer_s3_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = (FORMAT_NAME = csv_format)
  PATTERN = '.*customer_.*\\.csv';
  
show pipes;
select SYSTEM$PIPE_STATUS('customer_s3_pipe');

select count(*) from customer_raw;
select * FROM CUSTOMER_RAW;

show integrations;

drop pipe customer_s3_pipe;