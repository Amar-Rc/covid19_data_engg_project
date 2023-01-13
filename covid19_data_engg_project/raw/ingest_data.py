# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists covid_raw;

# COMMAND ----------

enigma_jhu = spark.read.csv('s3://covid19-lake/enigma-jhu/csv/', header=True, inferSchema=True)

# COMMAND ----------

enigma_jhu.write.format('delta').saveAsTable('covid_raw.enigma_jhu')

# COMMAND ----------

nytimes_data_in_usa_us_county = spark.read.csv('s3://covid19-lake/enigma-nytimes-data-in-usa/csv/us_county/', header=True, inferSchema=True)

# COMMAND ----------

nytimes_data_in_usa_us_county.write.format('delta').saveAsTable('covid_raw.nytimes_data_in_usa_us_county')

# COMMAND ----------

nytimes_data_in_usa_us_states = spark.read.csv('s3://covid19-lake/enigma-nytimes-data-in-usa/csv/us_states/', header=True, inferSchema=True)

# COMMAND ----------

nytimes_data_in_usa_us_states.write.format('delta').saveAsTable('covid_raw.nytimes_data_in_usa_us_states')

# COMMAND ----------

static_data_state_abv = spark.read.csv('s3://covid19-lake/static-datasets/csv/state-abv/', header=True, inferSchema=True)

# COMMAND ----------

static_data_state_abv.write.format('delta').saveAsTable('covid_raw.static_data_state_abv')

# COMMAND ----------

static_data_cntry_cd = spark.read.csv('s3://covid19-lake/static-datasets/csv/countrycode/', header=True, inferSchema=True)

# COMMAND ----------

static_data_cntry_cd = static_data_cntry_cd.withColumnRenamed('Alpha-2 code', 'Alpha2_code')\
                                            .withColumnRenamed('Alpha-3 code', 'Alpha3_code')\
                                            .withColumnRenamed('Numeric code', 'Numeric_code')

# COMMAND ----------

static_data_cntry_cd.write.format('delta').saveAsTable('covid_raw.static_data_cntry_cd')

# COMMAND ----------

static_data_county_population = spark.read.csv('s3://covid19-lake/static-datasets/csv/CountyPopulation/', header=True, inferSchema=True)

# COMMAND ----------

static_data_county_population = static_data_county_population.withColumnRenamed('Population Estimate 2018', 'Population_Estimate_2018')

# COMMAND ----------

static_data_county_population.write.format('delta').saveAsTable('covid_raw.static_data_county_population')

# COMMAND ----------

rearc_covid19_testing_data_states_daily = spark.read.csv('s3://covid19-lake/rearc-covid-19-testing-data/csv/states_daily/', header=True, inferSchema=True)

# COMMAND ----------

rearc_covid19_testing_data_states_daily.write.format('delta').saveAsTable('covid_raw.rearc_covid19_testing_data_states_daily')

# COMMAND ----------

rearc_covid19_testing_data_us_total = spark.read.csv('s3://covid19-lake/rearc-covid-19-testing-data/csv/us-total-latest/', header=True, inferSchema=True)

# COMMAND ----------

rearc_covid19_testing_data_us_total.write.format('delta').saveAsTable('covid_raw.rearc_covid19_testing_data_us_total')

# COMMAND ----------

rearc_covid19_testing_data_us_daily = spark.read.csv('s3://covid19-lake/rearc-covid-19-testing-data/csv/us_daily/', header=True, inferSchema=True)

# COMMAND ----------

rearc_covid19_testing_data_us_daily.write.format('delta').saveAsTable('covid_raw.rearc_covid19_testing_data_us_daily')

# COMMAND ----------

rearc_usa_hospital_beds = spark.read.json('s3://covid19-lake/rearc-usa-hospital-beds/json/')

# COMMAND ----------

rearc_usa_hospital_beds.write.format('delta').saveAsTable('covid_raw.rearc_usa_hospital_beds')

# COMMAND ----------

# MAGIC %sql
# MAGIC use database covid_raw;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc covid_raw.rearc_usa_hospital_beds