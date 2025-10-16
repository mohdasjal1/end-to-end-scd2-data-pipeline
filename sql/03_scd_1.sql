select * from customer;
select * from customer_raw;

CREATE OR REPLACE PROCEDURE pdr_scd_demo()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
  var cmd = `
    MERGE INTO customer c
    USING customer_raw cr
      ON c.customer_id = cr.customer_id
    WHEN MATCHED AND (
         c.first_name  <> cr.first_name OR
         c.last_name   <> cr.last_name  OR
         c.email       <> cr.email      OR
         c.street      <> cr.street     OR
         c.city        <> cr.city       OR
         c.state       <> cr.state      OR
         c.country     <> cr.country
    )
    THEN UPDATE SET
         c.first_name  = cr.first_name,
         c.last_name   = cr.last_name,
         c.email       = cr.email,
         c.street      = cr.street,
         c.city        = cr.city,
         c.state       = cr.state,
         c.country     = cr.country,
         c.update_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (customer_id, first_name, last_name, email, street, city, state, country)
      VALUES (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country);
  `;

  var cmd1 = "TRUNCATE TABLE SCD_DEMO.SCD2.customer_raw;";
  var sql = snowflake.createStatement({sqlText: cmd});
  var sql1 = snowflake.createStatement({sqlText: cmd1});
  sql.execute();
  sql1.execute();
  return 'MERGE and TRUNCATE executed successfully.';
$$;

call pdr_scd_demo();

--SELECT customer_id, COUNT(*)
--FROM customer_raw
--GROUP BY customer_id
--HAVING COUNT(*) > 1;



--Set up TASKADMIN role
use role securityadmin;
create or replace role taskadmin;
-- Set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to TASKADMIN
use role accountadmin;
grant execute task on account to role taskadmin;

-- Set the active role to SECURITYADMIN to show that this role can grant a role to another role 
use role securityadmin;
grant role taskadmin to role sysadmin;
use role sysadmin;

create or replace task tsk_scd_raw 
    warehouse = COMPUTE_WH
    schedule = '1 minute'
    ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
    call pdr_scd_demo();

    
show tasks;
alter task tsk_scd_raw suspend;--resume --suspend
show tasks;

select 
    timestampdiff(second, current_timestamp, scheduled_time) as next_run,
    scheduled_time,
    current_timestamp,
    name,
    state 
from table(information_schema.task_history()) 
where state = 'SCHEDULED'
order by completed_time desc;


select * from customer where customer_id=1;