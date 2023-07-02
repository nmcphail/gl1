# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog development1;
# MAGIC select current_user();
# MAGIC create schema if not exists gl1_b comment 'GL1 Bronse Layer';
# MAGIC create schema if not exists gl1_s comment 'GL1 Silver Layer';
# MAGIC create schema if not exists gl1_g comment 'GL1 Gold Layer';
# MAGIC
# MAGIC create table if not exists gl1_b.s4_acdoca (c1 int);
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
