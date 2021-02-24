# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake で構築する臨床データレイク
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/04/health-blog-delta.png" />
# MAGIC <ol>
# MAGIC   <li> **Data**: We use a realistic simulation of patient EHR data using **[synthea](https://github.com/synthetichealth/synthea)**, for ~10,000 patients in Massachusetts </li>
# MAGIC   <li> **Ingestion and De-identification**: We use **pyspark** to read data from csv files, de-identify patient PII and write to Delta Lake</li>
# MAGIC   <li> **Database creation**: We then use delta tables to create a database of pateint recprds for subsequent data analysis</li>
# MAGIC </ol>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 準備

# COMMAND ----------

import re

# ユーザ名の取得と処理
username_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
username = re.sub('[^A-Za-z0-9]+', '', username_raw).lower()

# データベース名の生成
database_name = f"ehrdemo_{username}"

# データベースの再作成
spark.sql(f"drop database if exists {database_name} cascade")
spark.sql(f"create database {database_name}")
spark.sql(f"use {database_name}")
print(f"database_name: {database_name}")

# Delta Lake テーブルのルートパスの生成
delta_path = f"/tmp/{database_name}"

# Delta Lake テーブルのルートパスの削除
dbutils.fs.rm(delta_path, True)
print(f"delta_path: {delta_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ファイルの確認

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/rwe/ehr/csv/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ブロンズテーブルの作成

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV ファイルの読み取り

# COMMAND ----------

# 患者データの CSV ファイルの読み取り
df_patients_raw = spark.read.csv("/databricks-datasets/rwe/ehr/csv/patients.csv", header=True, inferSchema=True)

# 診察データの CSV ファイルの読み取り
df_encounters_raw = spark.read.csv("/databricks-datasets/rwe/ehr/csv/encounters.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの確認

# COMMAND ----------

# 患者データの確認
display(df_patients_raw)

# COMMAND ----------

# 診察データの確認
display(df_encounters_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ブロンズテーブルの作成

# COMMAND ----------

# 患者データのブロンズテーブルの作成
(df_patients_raw
 .write
 .format("delta")
 .save(f"{delta_path}/patients_raw")
)

# 診察データのブロンズテーブルの作成
(df_encounters_raw
 .write
 .format("delta")
 .save(f"{delta_path}/encounters_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## シルバーテーブルの作成

# COMMAND ----------

# MAGIC %md
# MAGIC ### ブロンズテーブルからのデータの取得

# COMMAND ----------

# 患者データのブロンズテーブルのデータの読み取り
df_patients = (spark
               .read
               .format("delta")
               .load(f"{delta_path}/patients_raw"))

# 診察データのブロンズテーブルのデータの読み取り
df_encounters = (spark
                 .read
                 .format("delta")
                 .load(f"{delta_path}/encounters_raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの整形、加工

# COMMAND ----------

# MAGIC %md
# MAGIC #### 患者データの整形、加工

# COMMAND ----------

# 患者データから、データ分析に不要となる個人に関する情報を除去。
df_patients = df_patients.drop("SSN", "DRIVERS", "PASSPORT", "PREFIX", "FIRST", "LAST", "SUFFIX", "MAIDEN", "BIRTHPLACE", "ADDRESS")
　
# 結果を表示。
display(df_patients)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 診療データの整形、加工

# COMMAND ----------

# 必要となる Python モジュールをインポート。
from pyspark.sql.functions import col

# REASONDESCRIPTION カラムの値が Null であるレコードを除去。
df_encounters = df_encounters.filter(col("REASONDESCRIPTION").isNotNull())

# 結果を表示。
display(df_encounters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### シルバーテーブルの作成

# COMMAND ----------

# 患者データのシルバーテーブルの作成
(df_patients
 .write
 .format("delta")
 .save(f"{delta_path}/patients")
)

# 診察データのシルバーテーブルの作成
(df_encounters
 .write
 .format("delta")
 .save(f"{delta_path}/encounters")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ゴールドテーブルの作成

# COMMAND ----------

# MAGIC %md
# MAGIC ### シルバーテーブルからのデータの取得

# COMMAND ----------

# 患者データのブロンズテーブルのデータの読み取り
df_patients = (spark
               .read
               .format("delta")
               .load(f"{delta_path}/patients"))

# 診察データのブロンズテーブルのデータの読み取り
df_encounters = (spark
                 .read
                 .format("delta")
                 .load(f"{delta_path}/encounters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 診療データと患者データの結合

# COMMAND ----------

# 結合キーのカラム名を PATIENT_ID へ統一。
df_encounters = df_encounters.withColumnRenamed("PATIENT", "PATIENT_ID")
df_patients = df_patients.withColumnRenamed("Id", "PATIENT_ID")

# 結合を実施。
df_encounter_patients = df_encounters.join(df_patients, "PATIENT_ID")

# 結果を表示。
display(df_encounter_patients)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ゴールドテーブルの作成

# COMMAND ----------

# 診療データと患者データの結合結果をゴールとテーブルとして作成。
(df_encounter_patients
 .write
 .format("delta")
 .save(f"{delta_path}/encounter_patients")
)

# Spark SQL で参照できるようにするため、メタデータを作成。
spark.sql(f"""
  create table encounter_patients
  using delta
  location '{delta_path}/encounter_patients'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL でのゴールドテーブルデータの確認

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   encounter_patients

# COMMAND ----------

display(dbutils.fs.ls(f"{delta_path}/encounter_patients"))

# COMMAND ----------

# MAGIC %md
# MAGIC We can now use Delta's features for performance optimization. See this for more information see [ Delta Lake on Databricks ](https://docs.databricks.com/spark/latest/spark-sql/language-manual/optimize.html#optimize--delta-lake-on-databricks)

# COMMAND ----------

# MAGIC %sql OPTIMIZE rwd.patients ZORDER BY (BIRTHDATE, ZIP, GENDER, RACE)

# COMMAND ----------

# MAGIC %sql OPTIMIZE rwd.patient_encounters ZORDER BY (REASONDESCRIPTION, START_TIME, ZIP, PATIENT)

# COMMAND ----------

# MAGIC %md
# MAGIC We can set this ETL notebook [as a job](https://docs.databricks.com/jobs.html#create-a-job), that runs according to a given schedule.
# MAGIC Now proceed we create dashboard for quick data visualization. In the next notebook (`./01-rwe-dashboard.R`) we create a simple dashboard in `R`