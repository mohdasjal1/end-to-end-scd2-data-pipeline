use role sysadmin;
create warehouse if not exists COMPUTE_WH with warehouse_size = 'XSMALL' auto_suspend = 120;

create database if not exists scd_demo;
use database scd_demo;
create schema if not exists scd2;
use schema scd2;
show tables;

create or replace table customer (
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar,
     update_timestamp timestamp_ntz default current_timestamp());

select * from customer;
     
create or replace table customer_history (
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar,
     start_time timestamp_ntz default current_timestamp(),
     end_time timestamp_ntz default current_timestamp(),
     is_current boolean
     );

select * from customer_history;
     
create or replace table customer_raw (
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar);

select * from customer_raw;     
     
create or replace stream customer_table_changes on table customer;
CREATE OR REPLACE STREAM customer_table_changes ON TABLE customer;

select * from customer_table_changes;
