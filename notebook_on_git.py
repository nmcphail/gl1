# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog development1;
# MAGIC select current_user();
# MAGIC create schema if not exists gl1_b comment 'GL1 Bronse Layer';
# MAGIC create schema if not exists gl1_s comment 'GL1 Silver Layer';
# MAGIC create schema if not exists gl1_g comment 'GL1 Gold Layer';
# MAGIC create table if not exists gl1_b.s4_acdoca (c1 int);
# MAGIC
# MAGIC insert into gl1_b.s4_acdoca (c1 ) values (1);
# MAGIC
# MAGIC
# MAGIC --drop table gl1_b.s4_acdoca_shallow_clone;
# MAGIC create table if not exists gl1_b.s4_acdoca_shallow_clone shallow clone gl1_b.s4_acdoca;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #Hello World 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Hello again 
# MAGIC ## Hello
# MAGIC
