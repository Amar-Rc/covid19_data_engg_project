-- Databricks notebook source
create database if not exists covid_processed;

-- COMMAND ----------

create or replace table covid_processed.fact_covid 
as
select enigma_jhu.fips, enigma_jhu.province_state, enigma_jhu.country_region, enigma_jhu.confirmed, enigma_jhu.deaths, enigma_jhu.recovered, enigma_jhu.active, rearc_covid19_testing_data_states_daily.date, rearc_covid19_testing_data_states_daily.positive, rearc_covid19_testing_data_states_daily.negative, rearc_covid19_testing_data_states_daily.hospitalizedcurrently, rearc_covid19_testing_data_states_daily.hospitalized, rearc_covid19_testing_data_states_daily.hospitalizeddischarged
from covid_raw.rearc_covid19_testing_data_states_daily inner join
covid_raw.enigma_jhu on enigma_jhu.fips = rearc_covid19_testing_data_states_daily.fips;

-- COMMAND ----------

create or replace table covid_processed.dim_region 
as
select enigma.fips, enigma.province_state, enigma.country_region, enigma.latitude, enigma.longitude, us_county.county, us_county.state 
from
covid_raw.enigma_jhu enigma inner join 
covid_raw.nytimes_data_in_usa_us_county us_county on
enigma.fips = us_county.fips;

-- COMMAND ----------

create or replace table covid_processed.dim_hospital 
as
select fips, state_name, latitude, longtitude, hq_address, hospital_name, hospital_type, hq_city, hq_state
from covid_raw.rearc_usa_hospital_beds;

-- COMMAND ----------

create or replace table covid_processed.dim_date
as
select fips, date, year(date) as year, month(date) as month, case when dayofweek(date) in (1,7) then 'Yes' else 'no' end as is_weekend
from
(select fips, to_date(cast(date as string), 'yyyyMMdd') as date from covid_raw.rearc_covid19_testing_data_states_daily);

