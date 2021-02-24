# Databricks notebook source
# MAGIC %md
# MAGIC # はじめに

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 創薬パイプライン
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/Nets_Drug_Development.jpg" alt=default width=50%><img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/Databricks_Unified_Analytics_Platform.png" alt=default width=50%>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks QSAR リファレンスアーキテクチャ

# COMMAND ----------

# DBTITLE 0,Databricks QSAR Reference Architectures
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/QSAR%20Architecture.png" alt=default width=50%><img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/QSAR%20Architecture_in_depth.png" alt=default width=50%>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 対象の結果（ドッキングされたリガンドでの 6COX）
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/6cox_docked_3.png" width = "33%" /img><img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/6cox_docked_4.png" width = "33%" /img><img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/6cox_docked_5.png" width = "33%"/img>

# COMMAND ----------

# MAGIC %md
# MAGIC # 環境のセットアップ

# COMMAND ----------

# MAGIC %md
# MAGIC ## 小分子リガンドから標的タンパク質へのインシリコ分子ドッキングを行うためのプログラム (Autodock Vina) のインストール
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/autodock_vina.png" alt="drawing" width="500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ML ランタイムでの Conda の使用

# COMMAND ----------

# DBTITLE 1,Install Docking Libraries
# MAGIC %conda install -c bioconda autodock-vina biopython

# COMMAND ----------

# DBTITLE 1,Install OpenBabel for PDB Management
# MAGIC %conda install -c conda-forge openbabel

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Install In Silico Protein Surface Mapper (Concavity)
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/Concavity.png" alt="drawing" width="500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compile C++ Code

# COMMAND ----------

# DBTITLE 1,Download and Compile Concavity to do Protein Surface Probing
# MAGIC %sh
# MAGIC 
# MAGIC sudo apt install --assume-yes libglu1-mesa-dev                              ## install libgl headers
# MAGIC wget -nc https://compbio.cs.princeton.edu/concavity/concavity_distr.tar.gz  ## grab concavity source code
# MAGIC tar -xf concavity_distr.tar.gz                                              ## unpack concavity source code
# MAGIC 
# MAGIC ## compile concavity source code
# MAGIC mv concavity_distr /tmp/
# MAGIC cd /tmp/concavity_distr
# MAGIC make clean
# MAGIC make DESTDIR=.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribute Libraries to Worker Nodes

# COMMAND ----------

# DBTITLE 1,Install Needed GLX Concavity Library on Workers
# MAGIC %scala
# MAGIC 
# MAGIC import scala.sys.process.Process
# MAGIC 
# MAGIC var numWorkers : Int = sc.getExecutorMemoryStatus.size
# MAGIC sc.parallelize(0 to 2*numWorkers).map(_int => Process("sudo apt install --assume-yes libglu1-mesa-dev").!)
# MAGIC                                  .collect()

# COMMAND ----------

# DBTITLE 1,Distribute Concavity Binary to Worker Nodes
# MAGIC %fs cp -r file:/tmp/concavity_distr dbfs:/tmp/concavity

# COMMAND ----------

# DBTITLE 1,Have Workers Download Concavity from DBFS Due to Limitations in Databricks Community Edition
# MAGIC %scala
# MAGIC 
# MAGIC import scala.sys.process.Process
# MAGIC 
# MAGIC var numWorkers : Int = sc.getExecutorMemoryStatus.size
# MAGIC sc.parallelize(0 to 2*numWorkers).map(_int => dbutils.fs.cp("dbfs:/tmp/concavity/bin/x86_64/concavity", "file:/tmp/concavity")).map(_int => Process("chmod a+x /tmp/concavity").!)
# MAGIC                                  .collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Load Zinc15 Small Molecule Public Repository
# MAGIC <img src ="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/zinc15_license.png" width="500"/>
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/Zinc_3D.png" alt="drawing" width="500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Architecture Step
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/QSAR%20Architecture_ingest.png", width = "100%" /img>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retrieve Zinc 15 250K Data Set

# COMMAND ----------

# DBTITLE 1,Retrieve Zinc15 Experimental Dataset
# MAGIC %sh
# MAGIC 
# MAGIC ## The file contains 250K smile strings
# MAGIC 
# MAGIC wget -nc https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/zinc15_250K_2D.tar.gz
# MAGIC tar -xf zinc15_250K_2D.tar.gz
# MAGIC mv zinc15_250K_2D.csv /tmp/

# COMMAND ----------

# DBTITLE 1,Copy the Zinc15 Experimental Dataset onto DBFS
# MAGIC %fs cp file:/tmp/zinc15_250K_2D.csv dbfs:/tmp/

# COMMAND ----------

# DBTITLE 1,Drop Table to Begin SMILES Loading
# MAGIC %sql drop table if exists zinc15_deepchem;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Zinc15 SMILES Into Delta

# COMMAND ----------

# DBTITLE 1,Load the ZINC15 SMILES Data into Zinc15_DeepChem Table
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types.{StructField, StructType, StringType}
# MAGIC 
# MAGIC /*
# MAGIC   Create a read stream for loading Zinc15 smiles data
# MAGIC */
# MAGIC 
# MAGIC var zinc15_headers = StructType(List(StructField("smiles" ,StringType,true),
# MAGIC                                      StructField("zinc_id",StringType,true),
# MAGIC                                      StructField("mwt" ,StringType,true),
# MAGIC                                      StructField("logp" ,StringType,true),
# MAGIC                                      StructField("reactive" ,StringType,true),
# MAGIC                                      StructField("purchasable" ,StringType,true),
# MAGIC                                      StructField("tranche_name" ,StringType,true)));
# MAGIC 
# MAGIC /* Write the csv file into a Delta Lake table */
# MAGIC spark.read
# MAGIC     .format("csv")
# MAGIC     .option("header", true)
# MAGIC     .schema(zinc15_headers)
# MAGIC     .load("dbfs:/tmp/zinc15_250K_2D.csv")
# MAGIC     .write
# MAGIC     .mode("overwrite")
# MAGIC     .format("delta")
# MAGIC     .saveAsTable("zinc15_deepchem")

# COMMAND ----------

# DBTITLE 1,Load the Zinc15 250K Data from Delta
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.monotonically_increasing_id
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC 
# MAGIC 
# MAGIC /* Create a spark read stream */
# MAGIC var zinc15_df : DataFrame = spark.read
# MAGIC                                  .format("delta")
# MAGIC                                  .table("zinc15_deepchem")
# MAGIC                                  .withColumn("index", monotonically_increasing_id());
# MAGIC 
# MAGIC display(zinc15_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retrieve ZINC15 SDFs via Distributed ZINC15 API Calls and Ingest into Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ligand API UDF

# COMMAND ----------

# DBTITLE 1,Scala UDF for Retrieving SDFs
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.{udf, col};
# MAGIC 
# MAGIC /*
# MAGIC   Retrieve SDF files for ZINC15 subset
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC var getSDF = (zincID  : String) => {
# MAGIC   /*
# MAGIC     Download a SDF file using ZINCID
# MAGIC     
# MAGIC     @param zincID   |   String corresponding to a valid ZincID
# MAGIC   */
# MAGIC   try {
# MAGIC     scala.io.Source.fromURL(s"http://zinc15.docking.org/substances/${zincID}.sdf").mkString : String
# MAGIC   } catch {
# MAGIC     case e : Exception => "N/A"
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC var getSDF_UDF = udf((zinc_id : String) => {getSDF(zinc_id)});
# MAGIC 
# MAGIC spark.udf.register("getSDF", getSDF_UDF);

# COMMAND ----------

# DBTITLE 1,Drop Table to Begin the SDF Loading Process
# MAGIC %sql drop table if exists zinc_15_250K;

# COMMAND ----------

# DBTITLE 1,Batch Job with Loop to Download Zinc Data from APIs
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Obtain the 
# MAGIC */
# MAGIC 
# MAGIC /* get the length of the dataframe */
# MAGIC var zinc15_df_size : Int = zinc15_df.count().toInt;
# MAGIC 
# MAGIC /* chunk the file into groups of 100 */
# MAGIC // var chunks : Seq[Int] = 1 to zinc15_df_size by 100;
# MAGIC var chunks : Seq[Int] = 1 to 100 by 100;
# MAGIC 
# MAGIC /* run a sequence for each */
# MAGIC  chunks.foreach {
# MAGIC    case index : Int => zinc15_df.filter($"index" >= index)
# MAGIC                                 .filter($"index" < index + 100)
# MAGIC                                 .withColumn("SDF", getSDF_UDF($"zinc_id"))
# MAGIC                                 .write
# MAGIC                                 .format("delta")
# MAGIC                                 .mode("append")
# MAGIC                                 .saveAsTable("ZINC_15_250K")
# MAGIC  }

# COMMAND ----------

# DBTITLE 1,Zinc15 250KSDFs
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Check the loaded SDFs 
# MAGIC --
# MAGIC 
# MAGIC SELECT * FROM ZINC_15_250K;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Examine Ligands for Class Imbalance

# COMMAND ----------

# DBTITLE 1,Look at the Molecular Weight Distributions of Ligands
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Look at the molecular weight distribution of ligands
# MAGIC --
# MAGIC 
# MAGIC SELECT mwt,
# MAGIC        count(*) as counts
# MAGIC FROM ZINC_15_250K
# MAGIC GROUP BY mwt
# MAGIC LIMIT 10000

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Optimize Table Reads with Delta

# COMMAND ----------

# DBTITLE 1,Get Details on Delta Lake Table
# MAGIC %sql DESCRIBE EXTENDED ZINC_15_250K;

# COMMAND ----------

# DBTITLE 1,Optimize the Table to Compact Small Files
# MAGIC %sql OPTIMIZE ZINC_15_250K;

# COMMAND ----------

# DBTITLE 1,Get File Parquet File Distributions for the Zinc_15_250K Table
# MAGIC %fs ls dbfs:/user/hive/warehouse/zinc_15_250k

# COMMAND ----------

# DBTITLE 1,Run the Same Query with Optimized Parquet Files for ZINC_15_250K
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Look at the molecular weight distribution of ligands
# MAGIC --
# MAGIC 
# MAGIC SELECT mwt,
# MAGIC        count(*) as counts
# MAGIC FROM ZINC_15_250K
# MAGIC GROUP BY mwt
# MAGIC LIMIT 10000

# COMMAND ----------

# DBTITLE 1,Look at the logP Distribution of Ligands
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Check the logP distribution of ligands
# MAGIC --
# MAGIC 
# MAGIC SELECT logp,
# MAGIC        count(*) as counts
# MAGIC FROM ZINC_15_250K
# MAGIC GROUP BY logp;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Check for a correlation between molecular weight and logp
# MAGIC --
# MAGIC 
# MAGIC WITH _DATA AS (
# MAGIC   SELECT CAST(mwt as float),
# MAGIC          CAST(logp as float)
# MAGIC   FROM ZINC_15_250K
# MAGIC   )
# MAGIC   SELECT * FROM _DATA;

# COMMAND ----------

# MAGIC %md 
# MAGIC # Load 6COX Receptor

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load 6COX PDB From RCSB
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/rcsb_pdb.png" alt="drawing" width="500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Receptor PDB UDF

# COMMAND ----------

# DBTITLE 1,Create a UDF to Retrieve PDB Files
# MAGIC %python
# MAGIC 
# MAGIC from Bio import PDB
# MAGIC from pyspark.sql.types import StringType
# MAGIC from pyspark.sql.functions import udf
# MAGIC import os
# MAGIC 
# MAGIC def retrieve_pdb(name : str,
# MAGIC                  pdir : str = '/tmp/',
# MAGIC                  file_format : str = 'pdb',
# MAGIC                  pdb_list : PDB.PDBList = PDB.PDBList()) -> str:
# MAGIC   """
# MAGIC   Retrieve a SDF/PDF/ENT file
# MAGIC   
# MAGIC   @param    name        |    ID of PDB to retrieve <6COX>
# MAGIC   @param    pdir        |    Location of temporary directory to download PDB to
# MAGIC   @param    file_format |    File format of PDb <PDB>
# MAGIC   """
# MAGIC   pdb_list.retrieve_pdb_file(name, pdir = pdir, file_format = file_format)
# MAGIC   with open(f"/tmp/pdb{name.lower()}.ent") as _file:
# MAGIC     _data = _file.read()
# MAGIC     os.remove(f"/tmp/pdb{name.lower()}.ent")
# MAGIC     return _data
# MAGIC   
# MAGIC retrieve_pdb_udf = udf(lambda x : retrieve_pdb(x), StringType())
# MAGIC 
# MAGIC spark.udf.register("retrieve_pdb", retrieve_pdb_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Store 6COX PDB In Receptor Table

# COMMAND ----------

# DBTITLE 1,Download the 6COX PDB Structure and Store it in a Table
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Retrieve the 6COX PDB file
# MAGIC --
# MAGIC 
# MAGIC DROP TABLE IF EXISTS RECEPTOR_TABLE;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS RECEPTOR_TABLE
# MAGIC USING DELTA
# MAGIC AS
# MAGIC   WITH _PDBS AS (
# MAGIC     SELECT '6COX' AS NAME
# MAGIC     )
# MAGIC     SELECT NAME,
# MAGIC            retrieve_pdb(NAME) AS PDB
# MAGIC     FROM _PDBS;

# COMMAND ----------

# DBTITLE 1,View the stored PDBs
# MAGIC %sql
# MAGIC 
# MAGIC SElECT * FROM RECEPTOR_TABLE;

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Docking Assay
# MAGIC 
# MAGIC * Determine Binding Box on the Surface of 6COX Protein
# MAGIC * Convert 6COX Protein to Rigid Molecule
# MAGIC * Convert Ligand SDF to PDBQT using OpenBabel

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Architecture Step
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/QSAR%20Architecture_clean.png" width="100%" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bounding Box Generation And Making the Receptor Rigid

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concavity UDF

# COMMAND ----------

# DBTITLE 1,Concavity Bounding Box UDF
# MAGIC %python
# MAGIC 
# MAGIC from typing import List, Dict, Tuple
# MAGIC import os, subprocess
# MAGIC import numpy as np
# MAGIC from pyspark.sql.functions import udf, struct
# MAGIC from pyspark.sql.types import MapType, StringType, FloatType
# MAGIC 
# MAGIC def run_concavity(pdb_name : str,
# MAGIC                   pdb_string : str,
# MAGIC                   concavity_path : str) -> List[str]:
# MAGIC   """
# MAGIC   Generate a bounding box for a PDB file
# MAGIC   
# MAGIC   @param pdb_name        | Name of PDB <6COX>
# MAGIC   @param pdb_string      | String containing pdb information
# MAGIC   @param concavity_path  | Path of the concavity binary </dbfs/tmp/concavity>
# MAGIC   """
# MAGIC   def readPDB(path) -> List[List[float]]:
# MAGIC     """
# MAGIC     Extract the Cartesian coordinates from a Concavity PDB File
# MAGIC 
# MAGIC     @param   path    |    Concavity PDB File
# MAGIC     """
# MAGIC     ## do for file
# MAGIC     cartesian_array = [[]]
# MAGIC     cavity = 0
# MAGIC     with open(path) as file:
# MAGIC       for line in file:
# MAGIC         ## split the line into an array based on tabs
# MAGIC         splitLine = line.split()
# MAGIC         if not cavity == int(splitLine[5]):
# MAGIC           cartesian_array.append([[splitLine[6], splitLine[7], splitLine[8]]])
# MAGIC           cavity = int(splitLine[5])
# MAGIC         else:
# MAGIC           cartesian_array[-1].append([splitLine[6], splitLine[7], splitLine[8]])
# MAGIC       ## return the sample array
# MAGIC     return cartesian_array[0]
# MAGIC   
# MAGIC   def getBox(cartesian_array) -> Dict[str, float]:
# MAGIC     """
# MAGIC     Get a rectangular bounding box
# MAGIC 
# MAGIC     @param cartesian_array  |   cartesian coordinates from concavity PDB file
# MAGIC     """
# MAGIC     def getCoord(array, posn):
# MAGIC       return([float(b[posn]) for b in array])
# MAGIC     x_range, y_range, z_range = [[min(coords), max(coords)] for coords in [getCoord(cartesian_array, a) for a in range(3)]]
# MAGIC     center = [np.mean(a) for a in [x_range, y_range, z_range]]
# MAGIC     center_x = float(center[0])
# MAGIC     center_y = float(center[1])
# MAGIC     center_z = float(center[2])
# MAGIC     x_side, y_side, z_side = [abs(a[1] - a[0]) for a in [x_range, y_range, z_range]]
# MAGIC     return {"center_x" : center_x,
# MAGIC             "center_y" : center_y,
# MAGIC             "center_z" : center_z,
# MAGIC             "size_x" : x_side + 3,
# MAGIC             "size_y" : y_side + 3,
# MAGIC             "size_z" : z_side + 3}
# MAGIC   
# MAGIC   _temp_file = f"/tmp/{pdb_name}.sdf"
# MAGIC   
# MAGIC   with open(_temp_file, "w") as _pdb:
# MAGIC     _pdb.write(pdb_string)
# MAGIC     try:
# MAGIC       _res = subprocess.run([concavity_path, "-print_grid_pdb", "1",
# MAGIC                                              '-max_cavities', '5',
# MAGIC                                              _temp_file,
# MAGIC                                              "concavity"],
# MAGIC                             stderr = subprocess.STDOUT,
# MAGIC                             check=True)
# MAGIC       _cartesian_array = readPDB(f"{pdb_name}_concavity_pocket.pdb")
# MAGIC       _bounding_boxes = getBox(_cartesian_array)
# MAGIC     except Exception as err:
# MAGIC #       return str(err)
# MAGIC #       return {"msg" : f"Failed to run concavity at {concavity_path}",
# MAGIC #               "error" : err}
# MAGIC         return err
# MAGIC   os.remove(_temp_file)
# MAGIC   os.remove(f"{pdb_name}_concavity_pocket.pdb")
# MAGIC   return _bounding_boxes
# MAGIC 
# MAGIC run_concavity_udf = udf(lambda pdb_name, pdb_string: run_concavity(pdb_name = pdb_name, pdb_string = pdb_string, concavity_path = '/tmp/concavity'),
# MAGIC                         MapType(StringType(), FloatType()))
# MAGIC 
# MAGIC spark.udf.register("run_concavity", run_concavity_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OpenBabel Rigid Transformer UDF

# COMMAND ----------

# DBTITLE 1,UDF to make a rigid PDBQT
# MAGIC %python
# MAGIC 
# MAGIC import subprocess
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import StringType
# MAGIC 
# MAGIC """
# MAGIC Strip PDB of salts and make a rigid PDBQT for the receptor
# MAGIC """
# MAGIC 
# MAGIC def babel_convert_pdb(name : str,
# MAGIC                       pdb_string : str) -> str:
# MAGIC   """
# MAGIC   Convert a PDB into a rigid PDBQT string
# MAGIC   """
# MAGIC   
# MAGIC   _temp_file = f"/tmp/{name}.pdb"
# MAGIC   _out_file = f"/tmp/{name}.pqdbqt"
# MAGIC   
# MAGIC   with open(_temp_file, "w") as _pdb:
# MAGIC     _pdb.write(pdb_string)
# MAGIC     _res = subprocess.run(["obabel", "-i", "pdb", _temp_file,
# MAGIC                                      "-o", "pdbqt",
# MAGIC                                      "-O", _out_file,
# MAGIC                                      "-xr"],
# MAGIC                           stderr = subprocess.STDOUT,
# MAGIC                           check=True)
# MAGIC     with open(_out_file) as _pdbqt:
# MAGIC       _out_string = _pdbqt.read()
# MAGIC   os.remove(_temp_file)
# MAGIC   os.remove(_out_file)
# MAGIC   return _out_string
# MAGIC 
# MAGIC babel_convert_pdb_udf = udf(lambda name, pdb_string : babel_convert_pdb(name, pdb_string), StringType())
# MAGIC 
# MAGIC spark.udf.register("babel_convert_pdb", babel_convert_pdb_udf)

# COMMAND ----------

# DBTITLE 1,Generate the bounding box for 6COX and the Receptor PDBQT
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Create the bounding box table for each PDB
# MAGIC --
# MAGIC 
# MAGIC DROP TABLE IF EXISTS RECEPTOR_SAMPLES;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS RECEPTOR_SAMPLES
# MAGIC USING DELTA
# MAGIC AS
# MAGIC   SELECT NAME,
# MAGIC          PDB,
# MAGIC          run_concavity(name, PDB) as bounding_box,
# MAGIC          babel_convert_pdb(NAME, PDB) as receptor_pdbqt
# MAGIC   FROM RECEPTOR_TABLE
# MAGIC   WHERE NAME = '6COX';

# COMMAND ----------

# DBTITLE 1,Read from the Receptor Samples Table
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Look at the receptor and bounding box calculations
# MAGIC --
# MAGIC 
# MAGIC DESCRIBE RECEPTOR_SAMPLES;

# COMMAND ----------

# MAGIC %md ## Convert Ligand PDB to PDBQT By Adding Surface Charges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Openbabel Ligand Charger

# COMMAND ----------

# DBTITLE 1,OpenBabel UDF To Convert Ligands to PDBQT
# MAGIC %python
# MAGIC 
# MAGIC import openbabel
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import StringType
# MAGIC 
# MAGIC """
# MAGIC Convert a SDF into a PDBQT file
# MAGIC """
# MAGIC 
# MAGIC def babel_convert(input_string : str) -> str:
# MAGIC   """
# MAGIC   Convert a SDF string into a PDBQT string
# MAGIC   """
# MAGIC   _mol = openbabel.OBMol()
# MAGIC   _ob_conversion = openbabel.OBConversion()
# MAGIC   _ob_conversion.SetInAndOutFormats("sdf", "pdbqt")
# MAGIC   _ob_conversion.ReadString(_mol, input_string)
# MAGIC   _pdbqt_string = _ob_conversion.WriteString(_mol)
# MAGIC   
# MAGIC   return _pdbqt_string
# MAGIC 
# MAGIC babel_convert_udf = udf(lambda input_string : babel_convert(input_string), StringType())
# MAGIC 
# MAGIC spark.udf.register("babel_convert", babel_convert_udf)

# COMMAND ----------

# DBTITLE 1,Create a Small Sample Set of PDBQT Ligands
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS LIGAND_SAMPLES;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS LIGAND_SAMPLES
# MAGIC USING DELTA
# MAGIC AS
# MAGIC   SELECT *,
# MAGIC          BABEL_CONVERT(SDF) AS PDBQT
# MAGIC   FROM ZINC_15_250k
# MAGIC   LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Read the PDBQT Ligand Samples Table
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Look at the small sample set of converted PDBQT ligands
# MAGIC -- This table is used for the ligand docking studies
# MAGIC --
# MAGIC 
# MAGIC SELECT * FROM LIGAND_SAMPLES;

# COMMAND ----------

# MAGIC %md
# MAGIC # Vina Docking of Ligands to 6COX

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Architecture Step
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/QSAR%20Architecture_vina.png", width="100%" />

# COMMAND ----------

# MAGIC %md ## Create the Vina Ligand Docking Table

# COMMAND ----------

# DBTITLE 1,Create the Ligand Docking Table
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Generate the ligand + pdb table for docking studies
# MAGIC --
# MAGIC 
# MAGIC DROP TABLE IF EXISTS LIGAND_DOCKING_TABLE;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS LIGAND_DOCKING_TABLE
# MAGIC USING DELTA
# MAGIC AS
# MAGIC   SElECT CURRENT_TIMESTAMP() AS TIMESTAMP,
# MAGIC          a.*,
# MAGIC          b.*
# MAGIC   FROM LIGAND_SAMPLES as A
# MAGIC   CROSS JOIN RECEPTOR_SAMPLES AS B
# MAGIC   WHERE B.NAME = '6COX';

# COMMAND ----------

# DBTITLE 1,Read the Ligand Docking Table
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Read the Ligand Docking Table
# MAGIC -- This contains the fields that will be passed
# MAGIC -- to Autodock Vina
# MAGIC --
# MAGIC 
# MAGIC DESCRIBE LIGAND_DOCKING_TABLE;

# COMMAND ----------

# MAGIC %md ## Distribute Vina onto Spark Workers

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Autodock Vina UDF

# COMMAND ----------

# DBTITLE 1,UDF to Run Autodock Vina
# MAGIC %python
# MAGIC 
# MAGIC 
# MAGIC import subprocess
# MAGIC from pyspark.sql.functions import udf, struct
# MAGIC from pyspark.sql.types import ArrayType, MapType, FloatType, StringType, StructType, StructField
# MAGIC from typing import List, Dict, Union
# MAGIC 
# MAGIC def run_vina(ligand_name: str, 
# MAGIC              bounding_box : Dict[str, float], 
# MAGIC              ligand_string : str,
# MAGIC              pdb_name : str,
# MAGIC              pdb_string : str) -> List[List[Union[str, Dict[str, float]]]]:
# MAGIC   """
# MAGIC   Run Autodock Vina on a ligand + pdb + bounding box combination
# MAGIC   
# MAGIC   @param ligand_name    | name of ligand <ZINC...>
# MAGIC   @param ligand_string  | PDBQT string for a ligand
# MAGIC   @param pdb_name       | Name of PDB receptor <6COX>
# MAGIC   @param pdb_string     | PDB string for a receptor
# MAGIC   """
# MAGIC   _temp_config = f"/tmp/{ligand_name}.config"
# MAGIC   _temp_pdb = f"/tmp/{pdb_name}.pdbqt"
# MAGIC   _temp_pdbqt = f"/tmp/{ligand_name}.pdbqt"
# MAGIC   _temp_output = f"/tmp/{ligand_name}-conformers.pdbqt"
# MAGIC   
# MAGIC   with open(_temp_config, "w") as _config:
# MAGIC     with open(_temp_pdbqt, "w") as _pdbqt:
# MAGIC       _pdbqt.write(ligand_string)
# MAGIC       with open(_temp_pdb, "w") as _pdb:
# MAGIC         _pdb.write(pdb_string)
# MAGIC         _config_string = f"""
# MAGIC receptor = {_temp_pdb}
# MAGIC center_x = {bounding_box['center_x']}
# MAGIC center_y = {bounding_box['center_y']}
# MAGIC center_z = {bounding_box['center_z']}
# MAGIC size_x   = {bounding_box['size_x']}
# MAGIC size_y   = {bounding_box['size_y']}
# MAGIC size_z   = {bounding_box['size_z']}
# MAGIC ligand   = {_temp_pdbqt}
# MAGIC                           """
# MAGIC         _config.write(_config_string)
# MAGIC   _result = subprocess.run(['vina',
# MAGIC                             "--config", _temp_config,
# MAGIC                             "--out", _temp_output],
# MAGIC                             stderr = subprocess.STDOUT)
# MAGIC   with open(_temp_output) as _file:
# MAGIC     _docked_pdbqt = _file.read()
# MAGIC     
# MAGIC   os.remove(_temp_output)
# MAGIC #   os.remove(_temp_config)
# MAGIC   os.remove(_temp_pdbqt)
# MAGIC   os.remove(_temp_config)
# MAGIC   
# MAGIC   _conformer_results = ["MODEL "+ a for a in _docked_pdbqt.split("MODEL") if a != ""]
# MAGIC   _metrics = [_res.split("\n")[1].split()[-3:] for _res in _conformer_results]
# MAGIC   _labeled_metrics = [{"binding_affinity" : float(a[0]), 
# MAGIC                        "distance_from_rmsd_lower_bound" : float(a[1]), 
# MAGIC                        "best_mode_rmsd_upper_bound" : float(a[2])}
# MAGIC                       for a in _metrics]
# MAGIC   
# MAGIC   _unified_results = [_res for _res in zip(_conformer_results, _labeled_metrics)]
# MAGIC   
# MAGIC   return _unified_results
# MAGIC 
# MAGIC run_vina_udf = udf(lambda ligand_name, bounding_box, ligand_string, pdb_name, pdb_string : run_vina(ligand_name = ligand_name,
# MAGIC                                                                                                      bounding_box = bounding_box,
# MAGIC                                                                                                      ligand_string = ligand_string,
# MAGIC                                                                                                      pdb_name = pdb_name,
# MAGIC                                                                                                      pdb_string = pdb_string), 
# MAGIC                    ArrayType(StructType([
# MAGIC                                StructField("docked_conformer_pdbqt", 
# MAGIC                                             StringType()),
# MAGIC                                StructField("docked_conformer_vina_results", 
# MAGIC                                             MapType(StringType(),
# MAGIC                                                     FloatType()))])))
# MAGIC 
# MAGIC spark.udf.register("run_vina", run_vina_udf)

# COMMAND ----------

# DBTITLE 1,Run Distributed Autodock Vina on Several Ligands
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Run Autodock Vina on several ligands against 6COX
# MAGIC --
# MAGIC 
# MAGIC DROP TABLE IF EXISTS VINA_RESULTS;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS VINA_RESULTS
# MAGIC USING DELTA
# MAGIC AS
# MAGIC   WITH _RESULTS AS (
# MAGIC     SELECT TIMESTAMP,
# MAGIC            SMILES,
# MAGIC            ZINC_ID,
# MAGIC            mwt,
# MAGIC            logp,
# MAGIC            reactive,
# MAGIC            purchasable,
# MAGIC            tranche_name,
# MAGIC            PDBQT,
# MAGIC            NAME,
# MAGIC            BOUNDING_BOX,
# MAGIC            RECEPTOR_PDBQT,
# MAGIC            run_vina(zinc_id, bounding_box, pdbqt, name, receptor_pdbqt) as vina_results
# MAGIC     FROM LIGAND_DOCKING_TABLE
# MAGIC   )
# MAGIC   SELECT TIMESTAMP,
# MAGIC          SMILES,
# MAGIC          ZINC_ID,
# MAGIC          mwt,
# MAGIC          logp,
# MAGIC          reactive,
# MAGIC          purchasable,
# MAGIC          tranche_name,
# MAGIC          PDBQT,
# MAGIC          NAME,
# MAGIC          BOUNDING_BOX,
# MAGIC          RECEPTOR_PDBQT,
# MAGIC          explode(vina_results) as vina_results
# MAGIC    FROM _RESULTS;

# COMMAND ----------

# DBTITLE 1,Show the results of the Docking Studies on Several Ligands
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Read the Vina docking results
# MAGIC --
# MAGIC 
# MAGIC SELECT TIMESTAMP,
# MAGIC        SMILES, 
# MAGIC        ZINC_ID,
# MAGIC        mwt,
# MAGIC        logp,
# MAGIC        reactive,
# MAGIC        purchasable,
# MAGIC        tranche_name,
# MAGIC        name as receptor,
# MAGIC        bounding_box,
# MAGIC --        vina_results.docked_conformer_pdbqt
# MAGIC        vina_results.docked_conformer_vina_results
# MAGIC FROM VINA_RESULTS;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualize Binding Affinities

# COMMAND ----------

# DBTITLE 1,Visualize Some Distributions of Binding Affinities
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Take a look at the binding affinity distribution for the vina docking results
# MAGIC --
# MAGIC 
# MAGIC SELECT zinc_id,
# MAGIC        bounding_box,
# MAGIC        vina_results.docked_conformer_vina_results.binding_affinity
# MAGIC FROM VINA_RESULTS;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualize a sample docked receptor
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/6cox_docked_3.png" width="33%" /><img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/6cox_docked_4.png" width="33%" /><img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/6cox_docked_5.png" width="33%" />

# COMMAND ----------

# MAGIC %md 
# MAGIC # Using MLFLow to develop models from docked results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Step
# MAGIC <img src="https://raw.githubusercontent.com/keiji-yoshida/databricks-demo/main/%E5%89%B5%E8%96%AC%3A%20%E5%AE%9A%E9%87%8F%E7%9A%84%E6%A7%8B%E9%80%A0%E6%B4%BB%E6%80%A7%E7%9B%B8%E9%96%A2%20(QSAR)%20%E5%B0%8F%E5%88%86%E5%AD%90/imgs/QSAR%20Architecture_mlflow.png" width="100%" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup dataset

# COMMAND ----------

# DBTITLE 1,Vina Results Dataset
# MAGIC %sql
# MAGIC 
# MAGIC SELECT SMILES, 
# MAGIC        ZINC_ID,
# MAGIC        mwt,
# MAGIC        logp,
# MAGIC        reactive,
# MAGIC        purchasable,
# MAGIC        tranche_name,
# MAGIC        name as receptor,
# MAGIC        bounding_box,
# MAGIC --        vina_results.docked_conformer_pdbqt
# MAGIC        vina_results.docked_conformer_vina_results
# MAGIC FROM VINA_RESULTS;

# COMMAND ----------

# DBTITLE 1,Featurize Dataset
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.feature import VectorAssembler, StringIndexer
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC """
# MAGIC Prepare data for decision tree training
# MAGIC """
# MAGIC 
# MAGIC # setup some variables for column filtering
# MAGIC _string_columns = ("SMILES", "ZINC_ID", "TRANCHE_NAME")
# MAGIC _filtered_columns = _string_columns + ("MWT", "LOGP", "REACTIVE")
# MAGIC _indexed_columns = ("SMILES_INDEX", "ZINC_ID_INDEX", "TRANCHE_NAME_INDEX")
# MAGIC _features = _indexed_columns + ("MWT", "LOGP", "REACTIVE")
# MAGIC 
# MAGIC # load the vina results into a dataframe
# MAGIC _df = spark.read\
# MAGIC            .format("delta")\
# MAGIC            .table("vina_results")\
# MAGIC            .withColumn("binding_affinity", col("vina_results").docked_conformer_vina_results["binding_affinity"])\
# MAGIC            .select(*_string_columns,
# MAGIC                    col("MWT").cast("float"),
# MAGIC                    col("LOGP").cast("float"),
# MAGIC                    col("REACTIVE").cast("float"),
# MAGIC                    "binding_affinity")
# MAGIC         
# MAGIC # index the strings in columns
# MAGIC _indexer = StringIndexer(inputCols = _string_columns,
# MAGIC                          outputCols = _indexed_columns)
# MAGIC _df_indexed = _indexer.fit(_df).transform(_df)
# MAGIC 
# MAGIC # generate the feature vectors for ML
# MAGIC _assembler = VectorAssembler(inputCols = _features,
# MAGIC                              outputCol = "features")
# MAGIC _dataset = _assembler.transform(_df_indexed)
# MAGIC 
# MAGIC # dataset for training
# MAGIC display(_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup the 70:30 split

# COMMAND ----------

# DBTITLE 1,Setup the Train/Test Split
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Setup the testing and training data
# MAGIC """
# MAGIC 
# MAGIC _training_df, _test_df = _dataset.randomSplit([0.7, 0.3])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Training and Testing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Training Function

# COMMAND ----------

# DBTITLE 1,Create the Training Run Function
import mlflow
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
from typing import Tuple

def training_run(p_max_depth : int,
                 training_data : DataFrame,
                 test_data : DataFrame) -> Tuple[int, float]:
  with mlflow.start_run() as run:
    # log some parameters
    mlflow.log_param("Maximum_depth", p_max_depth)
    mlflow.log_metric("Training Data Rows", training_data.count())
    mlflow.log_metric("Test Data Rows", test_data.count())
    _dtClassifier = DecisionTreeRegressor(labelCol="binding_affinity", featuresCol="features")
    _dtClassifier.setMaxDepth(p_max_depth)
    _dtClassifier.setMaxBins(20) # This is how Spark decides if a feature is categorical or continuous
    
    # Train the model
    _model = _dtClassifier.fit(training_data)
    
    # Forecast
    _df_predictions = _model.transform(test_data)
    
    # Evaluate the model
    _evaluator = RegressionEvaluator(labelCol="binding_affinity", 
                                                   predictionCol="prediction")
    _accuracy = _evaluator.evaluate(_df_predictions)
    
    # Log the accuracy
    mlflow.log_metric("Accuracy", _accuracy)
    
    # Log the feature importances
    mlflow.log_param("Feature Importances", _model.featureImportances)
    
    # Log the model
    mlflow.spark.log_model(_model, f"Decision_tree_{p_max_depth}")
    
    return (p_max_depth, _accuracy)
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hyperparameter Search

# COMMAND ----------

# DBTITLE 1,Simple Hyperparameter Search
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Tune the max depth of the Decision tree
# MAGIC """
# MAGIC 
# MAGIC _results = [training_run(i, _training_df, _test_df) for i in range(10)]
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize the Elbow Plot

# COMMAND ----------

# DBTITLE 1,Elbow Plot
# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC """
# MAGIC Visualize the effect of tuning 
# MAGIC """
# MAGIC 
# MAGIC _tuning_df = sc.parallelize(_results).toDF()\
# MAGIC                .withColumnRenamed("_1", "Max Depth")\
# MAGIC                .withColumnRenamed("_2", "Accuracy")\
# MAGIC                .withColumn("Loss", 1 / col("Accuracy"))\
# MAGIC                .select("Max Depth", "Loss")
# MAGIC display(_tuning_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualize the Decision Tree with the optimal depth performance

# COMMAND ----------

# DBTITLE 1,Load the Decision Tree
# MAGIC %python
# MAGIC 
# MAGIC import mlflow
# MAGIC 
# MAGIC """
# MAGIC Load the current optimal model from MLFlow registry
# MAGIC """
# MAGIC 
# MAGIC _model = mlflow.spark.load_model("dbfs:/databricks/mlflow-tracking/3002893299057599/a919ddb643f346ea8e9889ee7134f472/artifacts/Decision_tree_3")
# MAGIC 
# MAGIC display(_model.stages[0])