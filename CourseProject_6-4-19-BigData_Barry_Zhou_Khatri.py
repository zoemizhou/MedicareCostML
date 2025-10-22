# Databricks notebook source
# MAGIC %md ### 1. Establish Mount Point

# COMMAND ----------

print('hello')

# COMMAND ----------

sourceToBeMounted = "abfss://timeseriesdatalake@wbcstevetstadls1.dfs.core.windows.net/"

# COMMAND ----------

spark.conf.set("fs.azure.account.key.wbcstevetstadls1.dfs.core.windows.net", "O6rTsGMEwzat1TKxzOjYurx1mtKv7KqbfYCFVM+ZTtaN5raC9ev6elnEXHgN1QWSj7xK3eKREPtDKJGVghxmnw==") #dbutils.secrets.get(scope = "nyctaxi-adlsgen2-storage", key = "storage-acct-key"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://timeseriesdatalake@wbcstevetstadls1.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %md ### 2. Define credentials for mounting

# COMMAND ----------

# MAGIC %md Client ID <95e67ab5-e043-4ba8-bb07-5704d3e62fdd> and Client Credential <Mq[s:oKpB0T]0HaMvRvH*xXhZsflVw49>. Follow the instructions in Create service principal with portal.
# MAGIC Get directory ID <7ebf3a40-0f86-495e-98d7-20b5bad7d2ac>: This is also referred to as tenant ID. Follow the instructions in Get tenant ID

# COMMAND ----------

# Credentials
clientID =  "95e67ab5-e043-4ba8-bb07-5704d3e62fdd" #dbutils.secrets.get(scope = "nyctaxi-adlsgen2-storage", key = "client-id")
clientSecret = "Mq[s:oKpB0T]0HaMvRvH*xXhZsflVw49" # dbutils.secrets.get(scope = "nyctaxi-adlsgen2-storage", key = "client-secret")
tenantID = "https://login.microsoftonline.com/7ebf3a40-0f86-495e-98d7-20b5bad7d2ac/oauth2/token" #+ dbutils.secrets.get(scope = "nyctaxi-adlsgen2-storage", key = "tenant-id") 

# ADLS config for mounting
adlsConfigs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": tenantID}

# COMMAND ----------

# MAGIC %md ### 3. Mount ADLSGen2 file systems

# COMMAND ----------

# MAGIC %md #### 3.0.1. Mount a single file system

# COMMAND ----------

#One time mounting of data lake file system
#dbutils.fs.mount(
#  source = "abfss://timeseriesdatalake@wbcstevetstadls1.dfs.core.windows.net/",
#  mount_point = "/mnt/course/timeseries/",
#  extra_configs = adlsConfigs)

# COMMAND ----------

# Display contents
display(dbutils.fs.ls("/mnt/course/timeseries/"))

# COMMAND ----------

# Display contents
display(dbutils.fs.ls("/mnt/course/timeseries/landing"))

# COMMAND ----------

# Unmount in case its already
#dbutils.fs.unmount("/mnt/course/timeseries")

# COMMAND ----------

df_uszips = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/uszips.csv')
display(df_uszips)

# COMMAND ----------

from pyspark.sql.functions import substring, concat, col
df_uszips = df_uszips.withColumn('fips_lastthree', substring(df_uszips['county_fips'], -3, 3))
df_uszips = df_uszips.withColumn('state_countycode', concat(col("fips_lastthree"), col("state_name")))
df_uszips = df_uszips.select(['state_countycode', 'zip'])
display(df_uszips)

# COMMAND ----------

df_epa_hap = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/epa_hap_daily_summary.csv')
df_epa_co = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/epa_co_daily_summary.csv')

# COMMAND ----------

df_epa_hap_grp = df_epa_hap.groupby("parameter_name").count().sort("count").orderBy(["count"], ascending=[0])
df_epa_hap_grp.show(100)

# COMMAND ----------

display(df_epa_co)

# COMMAND ----------

df_epa_co_grp = df_epa_co.groupby("sample_duration").count().sort("count").orderBy(["count"], ascending=[0])
df_epa_co_grp.show(100)

# COMMAND ----------

df_epa_co.columns

# COMMAND ----------

df_epa_co = df_epa_co.withColumn('Year', df_epa_co['date_local'].substr(0,4))

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit
df_epa_co_grp = df_epa_co.groupBy('county_code', 'state_name', 'Year', 'parameter_name').agg(F.avg('arithmetic_mean')).orderBy(["county_code"], ascending=[0])

df_epa_co_grp_2012 = df_epa_co_grp.filter("Year ='2012'")
df_epa_co_grp_2012 = df_epa_co_grp_2012.withColumn('state_countycode', concat(col("county_code"), col("state_name")))

df_epa_co_grp_2013 = df_epa_co_grp.filter("Year ='2013'")
df_epa_co_grp_2013 = df_epa_co_grp_2013.withColumn('state_countycode', concat(col("county_code"), col("state_name")))

df_epa_co_grp_2014 = df_epa_co_grp.filter("Year ='2014'")
df_epa_co_grp_2014 = df_epa_co_grp_2014.withColumn('state_countycode', concat(col("county_code"), col("state_name")))

df_epa_co_grp_2015 = df_epa_co_grp.filter("Year ='2015'")
df_epa_co_grp_2015 = df_epa_co_grp_2015.withColumn('state_countycode', concat(col("county_code"), col("state_name")))

df_epa_co_grp_2016 = df_epa_co_grp.filter("Year ='2016'")
df_epa_co_grp_2016 = df_epa_co_grp_2016.withColumn('state_countycode', concat(col("county_code"), col("state_name")))


# COMMAND ----------

df_epa_co_grp_2012 = df_epa_co_grp_2012.join(df_uszips, df_epa_co_grp_2012.state_countycode == df_uszips.state_countycode)
df_epa_co_grp_2013 = df_epa_co_grp_2013.join(df_uszips, df_epa_co_grp_2013.state_countycode == df_uszips.state_countycode)
df_epa_co_grp_2014 = df_epa_co_grp_2014.join(df_uszips, df_epa_co_grp_2014.state_countycode == df_uszips.state_countycode)
df_epa_co_grp_2015 = df_epa_co_grp_2015.join(df_uszips, df_epa_co_grp_2015.state_countycode == df_uszips.state_countycode)
df_epa_co_grp_2016 = df_epa_co_grp_2016.join(df_uszips, df_epa_co_grp_2016.state_countycode == df_uszips.state_countycode)

# COMMAND ----------

df_epa_co_grp_2012 = df_epa_co_grp_2012.select(['zip', 'avg(arithmetic_mean)'])
df_epa_co_grp_2013 = df_epa_co_grp_2013.select(['zip', 'avg(arithmetic_mean)'])
df_epa_co_grp_2014 = df_epa_co_grp_2014.select(['zip', 'avg(arithmetic_mean)'])
df_epa_co_grp_2015 = df_epa_co_grp_2015.select(['zip', 'avg(arithmetic_mean)'])
df_epa_co_grp_2016 = df_epa_co_grp_2016.select(['zip', 'avg(arithmetic_mean)'])

# COMMAND ----------

df_epa_co_grp_2012.count()

# COMMAND ----------

df_physician2012 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/medicare-physician-2012.csv')
df_physician2013 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/medicare-physician-2013.csv')
df_physician2014 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/medicare-physician-2014.csv')
df_physician2015 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/medicare-physician-2015.csv')
df_physician2016 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/medicare-physician-2016.csv')

# COMMAND ----------

len(df_physician2016.columns)

# COMMAND ----------

# drop extra columns that do not exist in 2012 and 2013 data
columns_to_drop = ['Total Medicare Standardized Payment Amount', 'Total Drug Medicare Standardized Payment Amount', 'Total Medical Medicare Standardized Payment Amount']
df_physician2014 = df_physician2014.drop(*columns_to_drop)
df_physician2015 = df_physician2015.drop(*columns_to_drop)
df_physician2016 = df_physician2016.drop(*columns_to_drop)

# COMMAND ----------

# new column names for consistency across all years
newColumns = ['NPI',
'LastName',
'FirstName',
'MiddleInitial',
'Credentials',
'Gender',
'EntityCode',
'Address1',
'Address2',
'City',
'Zip',
'State',
'Country',
'ProviderType',
'MPI',
'HCPCSCount',
'ServicesCount',
'UniqueBeneficiariesCount',
'TotalSubmittedCharges',
'TotalAllowedAmount',
'TotalPaymentAmount',
'DSI',
'HCPCSDrugCount',
'DrugServicesCount',
'UniqueBeneficiariesDrugCount',
'DrugSubmittedCharges',
'DrugAllowedAmount',
'DrugPaymentAmount',
'MSI',
'HCPCSMedicalCount',
'MedicalServicesCount',
'UniqueBeneficiariesMedicalCount',
'MedicalSubmittedCharges',
'MedicalAllowedAmount',
'MedicalPaymentAmount',
'AverageBeneficiaryAge',
'BeneficiaryCountLessThan65',
'BeneficiaryCount65To74',
'BeneficiaryCount75To84',
'BeneficiaryCountMoreThan84',
'FemaleBeneficiaryCount',
'MaleBeneficiaryCount',
'NonHispanicWhiteBeneficaryCount',
'BlackOrAfricanAmericanBeneficaryCount',
'AsianPacificIslanderBeneficaryCount',
'HispanicBeneficaryCount',
'AmericanIndianAlaskaNativeBeneficaryCount',
'RaceNotClassifiedBeneficaryCount',
'MedicareOnlyBeneficaryCount',
'MedicareAndMedicaidBeneficaryCount',
'AlzheimersDementiaBeneficiaryPercent',
'AsthmaBeneficiaryPercent',
'AtrialFibrillationBeneficiaryPercent',
'CancerBeneficiaryPercent',
'KidneyBeneficiaryPercent',
'PulmonaryBeneficiaryPercent',
'DepressionBeneficiaryPercent',
'DiabetesBeneficiaryPercent',
'HeartFailureBeneficiaryPercent',
'HyperlipidemiaBeneficiaryPercent',
'HypertensionBeneficiaryPercent',
'IschemicDiseaseBeneficiaryPercent',
'OsteoporosisBeneficiaryPercent',
'ArthritisBeneficiaryPercent',
'PsychoticBeneficiaryPercent',
'StrokeBeneficiaryPercent',
'AverageHCCRiskScore']

# COMMAND ----------

# make column names the same for all years
df_physician2012 = df_physician2012.toDF(*newColumns)
df_physician2013 = df_physician2013.toDF(*newColumns)
df_physician2014 = df_physician2014.toDF(*newColumns)
df_physician2015 = df_physician2015.toDF(*newColumns)
df_physician2016 = df_physician2016.toDF(*newColumns)


# COMMAND ----------

df_irs2012 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/irs2012.csv')
df_irs2013 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/irs2013.csv')
df_irs2014 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/irs2014.csv')
df_irs2015 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/irs2015.csv')
df_irs2016 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/course/timeseries/landing/irs2016.csv')

# COMMAND ----------

display(df_irs2012)

# COMMAND ----------

df_irs2012.count() + df_irs2013.count() + df_irs2014.count() + df_irs2015.count() + df_irs2016.count()

# COMMAND ----------

# Calculate average income by zip code
df_irs2012 = df_irs2012.select(['zipcode', 'A00100']).groupby('zipcode').mean('A00100').select('zipcode', F.col("avg(A00100)").alias("AverageIncome"))
df_irs2013 = df_irs2013.select(['zipcode', 'A00100']).groupby('zipcode').mean('A00100').select('zipcode', F.col("avg(A00100)").alias("AverageIncome"))
df_irs2014 = df_irs2014.select(['zipcode', 'A00100']).groupby('zipcode').mean('A00100').select('zipcode', F.col("avg(A00100)").alias("AverageIncome"))
df_irs2015 = df_irs2015.select(['zipcode', 'A00100']).groupby('zipcode').mean('A00100').select('zipcode', F.col("avg(A00100)").alias("AverageIncome"))
df_irs2016 = df_irs2016.select(['zipcode', 'A00100']).groupby('zipcode').mean('A00100').select('zipcode', F.col("avg(A00100)").alias("AverageIncome"))

# COMMAND ----------

display(df_irs2012)

# COMMAND ----------

# Truncating Zipcode in Medicare to 5 characters
df_physician2012 = df_physician2012.withColumn('Zip', df_physician2012['Zip'].substr(0,5))
df_physician2013 = df_physician2013.withColumn('Zip', df_physician2013['Zip'].substr(0,5))
df_physician2014 = df_physician2014.withColumn('Zip', df_physician2014['Zip'].substr(0,5))
df_physician2015 = df_physician2015.withColumn('Zip', df_physician2015['Zip'].substr(0,5))
df_physician2016 = df_physician2016.withColumn('Zip', df_physician2016['Zip'].substr(0,5))

# COMMAND ----------

# Combining yearly data IRS and Medicare
df_combined2012 = df_physician2012.join(df_irs2012, df_physician2012.Zip == df_irs2012.zipcode)
df_combined2013 = df_physician2013.join(df_irs2013, df_physician2013.Zip == df_irs2013.zipcode)
df_combined2014 = df_physician2014.join(df_irs2014, df_physician2014.Zip == df_irs2014.zipcode)
df_combined2015 = df_physician2015.join(df_irs2015, df_physician2015.Zip == df_irs2015.zipcode)
df_combined2016 = df_physician2016.join(df_irs2016, df_physician2016.Zip == df_irs2016.zipcode)

# COMMAND ----------

# Combine epa co data 
df_combined2012_epa = df_combined2012.join(df_epa_co_grp_2012, df_combined2012.Zip == df_epa_co_grp_2012.zip)
df_combined2013_epa = df_combined2013.join(df_epa_co_grp_2013, df_combined2013.Zip == df_epa_co_grp_2013.zip)
df_combined2014_epa = df_combined2014.join(df_epa_co_grp_2014, df_combined2014.Zip == df_epa_co_grp_2014.zip)
df_combined2015_epa = df_combined2015.join(df_epa_co_grp_2015, df_combined2015.Zip == df_epa_co_grp_2015.zip)
df_combined2016_epa = df_combined2016.join(df_epa_co_grp_2016, df_combined2016.Zip == df_epa_co_grp_2016.zip)


# COMMAND ----------

display(df_combined2012)

# COMMAND ----------

df_combined_all = df_combined2012.unionAll(df_combined2013).unionAll(df_combined2014).unionAll(df_combined2015).unionAll(df_combined2016)
df_combined_all_epa = df_combined2012_epa.unionAll(df_combined2013_epa).unionAll(df_combined2014_epa).unionAll(df_combined2015_epa).unionAll(df_combined2016_epa)

# COMMAND ----------

# Resetting DF to use IRS and Medicare combined data
df_physicians_all = df_combined_all

# COMMAND ----------

df_physicians_all.count()

# COMMAND ----------

#Filter out some rows
df_physicians_all = df_physicians_all.filter(df_physicians_all['Country'] == 'US')
df_physicians_all = df_physicians_all.filter(df_physicians_all['MPI'] == 'Y')

# COMMAND ----------

df_physicians_all.count()

# COMMAND ----------

#df_physicians_all = df_physicians_all.withColumn('epa_co', df_physicians_all['avg(arithmetic_mean)'])

# COMMAND ----------

df_physicians_all.printSchema()

# COMMAND ----------

# Drop irrelevant columns
columns_to_drop = ['NPI', 'LastName', 'FirstName', 'MiddleInitial', 'Credentials', 'Gender', 'Address1', 'Address2', 'City', 'State', 'Country', 'MPI', 'zipcode']
df_physicians_all = df_physicians_all.drop(*columns_to_drop)

# COMMAND ----------

#HCC average score histogram
display(df_physicians_all)

# COMMAND ----------

# MAGIC %md # Unsupervised Learning

# COMMAND ----------

df_physicians_all.printSchema()

# COMMAND ----------

# now let's see how many categorical and numerical features we have:
cat_cols = [item[0] for item in df_physicians_all.dtypes if item[1].startswith('string')] 
print(str(len(cat_cols)) + '  categorical features')
num_cols = [item[0] for item in df_physicians_all.dtypes if item[1].startswith('int') | item[1].startswith('double')]
print(str(len(num_cols)) + '  numerical features')


# COMMAND ----------

df_physicians_all.count()

# COMMAND ----------

import pandas as pd
# function to find more information about the #missing values
def info_missing_table(df_pd):
    """Input pandas dataframe and Return columns with missing value and percentage"""
    mis_val = df_pd.isnull().sum() #count total of null in each columns in dataframe
    #count percentage of null in each columns
    mis_val_percent = 100 * df_pd.isnull().sum() / len(df_pd) 
    mis_val_table = pd.concat([mis_val, mis_val_percent], axis=1) 
    #join to left (as column) between mis_val and mis_val_percent
    mis_val_table_ren_columns = mis_val_table.rename(
    columns = {0 : 'Missing Values', 1 : '% of Missing Values'}) 
    #rename columns in table
    mis_val_table_ren_columns = mis_val_table_ren_columns[
    mis_val_table_ren_columns.iloc[:,1] != 0].sort_values('% of Missing Values', ascending=False).round(1) 
        
    print ("Dataframe has " + str(df_pd.shape[1]) + " columns.\n"    #.shape[1] : just view total columns in dataframe  
    "There are " + str(mis_val_table_ren_columns.shape[0]) +              
    " columns that have missing values.") #.shape[0] : just view total rows in dataframe
    
    return mis_val_table_ren_columns
    

# COMMAND ----------

df_pd = df_physicians_all.toPandas()
missings = info_missing_table_python(df_pd)
missings

# COMMAND ----------

#ML

#drop everything has more than 30% of missing values, 17 columns
columns_to_drop_ml = ['DSI','MSI','RaceNotClassifiedBeneficaryCount','AsianPacificIslanderBeneficaryCount','HispanicBeneficaryCount','AmericanIndianAlaskaNativeBeneficaryCount','BlackOrAfricanAmericanBeneficaryCount','PsychoticBeneficiaryPercent','StrokeBeneficiaryPercent','BeneficiaryCountMoreThan84','OsteoporosisBeneficiaryPercent','AsthmaBeneficiaryPercent','NonHispanicWhiteBeneficaryCount','BeneficiaryCountLessThan65','CancerBeneficiaryPercent','AlzheimersDementiaBeneficiaryPercent','AtrialFibrillationBeneficiaryPercent']

df_physicians_all= df_physicians_all.drop(*columns_to_drop_ml)

# COMMAND ----------

df_physicians_all.printSchema()

# COMMAND ----------

# now let's see how many categorical and numerical features we have:
cat_cols = [item[0] for item in df_physicians_all.dtypes if item[1].startswith('string')] 
print(str(len(cat_cols)) + '  categorical features')
num_cols = [item[0] for item in df_physicians_all.dtypes if item[1].startswith('int') | item[1].startswith('double')]
print(str(len(num_cols)) + '  numerical features')

# COMMAND ----------

# fill missing values with 0 (best assumption based on the nature of the data)
df_physicians_all = df_physicians_all.na.fill(0)

# COMMAND ----------

cat_cols

# COMMAND ----------

num_cols

# COMMAND ----------

# we use the OneHotEncoderEstimator from MLlib in spark to convert 
# each categorical feature into one-hot vectors
# next, we use VectorAssembler to combine the resulted one-hot ector 
# and the rest of numerical features into a 
# single vector column. we append every step of the process in a #stages array

from pyspark.ml.feature import VectorAssembler

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator

#convert relevant categorical into one hot encoded
indexer1 = StringIndexer(inputCol="EntityCode", outputCol="EntityCodeIdx").setHandleInvalid("skip")
indexer2 = StringIndexer(inputCol="Zip", outputCol="ZipIdx").setHandleInvalid("skip")
indexer3 = StringIndexer(inputCol="ProviderType", outputCol="ProviderTypeIdx").setHandleInvalid("skip")

#gather all indexers as inputs to the One Hot Encoder
inputs = [indexer1.getOutputCol(), indexer2.getOutputCol(), indexer3.getOutputCol()]

#create the one hot encoder
encoder_outputs = ["EntityCodeVec", "ZipVec", "ProviderTypeVec"]
encoder = OneHotEncoderEstimator(inputCols=inputs, outputCols=encoder_outputs)

#run it through a pipeline
pipeline = Pipeline(stages=[indexer1, indexer2, indexer3, encoder])
encodedData = pipeline.fit(df_physicians_all).transform(df_physicians_all)

# COMMAND ----------

encodedData.select(encoder_outputs).show(5)

# COMMAND ----------

#gather feature vector and identify features
assembler_inputs = encoder_outputs + num_cols
assembler = VectorAssembler(inputCols = assembler_inputs, outputCol = 'features')
encodedData = assembler.transform(encodedData)

# COMMAND ----------

# Try a simpler case for k-means
import numpy as np
from pyspark.ml.clustering import KMeans

k_means_columns = ['TotalSubmittedCharges','DrugSubmittedCharges','MedicalSubmittedCharges','AverageHCCRiskScore']
k_means_assembler = VectorAssembler(inputCols = k_means_columns, outputCol = 'k_means_features')
k_means_encodedData = k_means_assembler.transform(df_physicians_all.select(k_means_columns))

k_means_cost = np.zeros(20)
for k in range(2,20):
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("k_means_features")
    k_means_model = kmeans.fit(k_means_encodedData.sample(False, 0.1, seed=42))
    k_means_cost[k] = k_means_model.computeCost(k_means_encodedData)


# COMMAND ----------

display(k_means_encodedData)

# COMMAND ----------

import matplotlib.pyplot as plt
fig, ax = plt.subplots(1,1, figsize =(8,6))
ax.plot(range(2,20),k_means_cost[2:20])
ax.set_xlabel('k')
ax.set_ylabel('cost')
display(fig)

# COMMAND ----------

# Looks like k=10 works. We are now ready to train the model on the full dataset
k = 10
k_means_kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("k_means_features")
k_means_model = k_means_kmeans.fit(k_means_encodedData)
centers = k_means_model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

display(centers)

# COMMAND ----------

# Assign clusters to events
k_means_transformed = k_means_model.transform(k_means_encodedData)
k_means_transformed.show(5)

# COMMAND ----------

k_means_transformed.select('prediction').distinct().show()

# COMMAND ----------

k_means_transformed_pd = k_means_transformed.toPandas()
k_means_transformed_pd.head()

# COMMAND ----------

# This import registers the 3D projection, but is otherwise unused.
from mpl_toolkits.mplot3d import Axes3D

import matplotlib.pyplot as plt
import numpy as np

threedee = plt.figure(figsize=(12,10)).gca(projection='3d')
threedee.scatter(k_means_transformed_pd.TotalSubmittedCharges, k_means_transformed_pd.MedicalSubmittedCharges, k_means_transformed_pd.AverageHCCRiskScore, c=k_means_transformed_pd.prediction)
threedee.set_xlabel('x')
threedee.set_ylabel('y')
threedee.set_zlabel('z')
display(plt.show(threedee))

# COMMAND ----------

display(k_means_kmeans, k_means_transformed)

# COMMAND ----------

# MAGIC %md ## ML

# COMMAND ----------

#show the column contents for the AverageHCCRiskScore, i.e. count=4936683, mean = 1.54, stddev=0.75, min = 0.27, max = 13.28
#encodedData.select('AverageHCCRiskScore').describe().show()

# COMMAND ----------

# DBTITLE 1,Read Data and Rename it
data_mi=df_combined_all

# COMMAND ----------

data_mi.dropna().count()

# COMMAND ----------

data_mi.printSchema()

# COMMAND ----------

#QuantileDiscretizer takes a column with continuous features and outputs a column with binned categorical features. The number of bins is set by the numBuckets parameter. It is possible that the number of buckets used will be smaller than this value, for example, if there are too few distinct values of the input to create enough distinct quantiles.

from pyspark.ml.feature import QuantileDiscretizer

#High Medium Low
discretizer_mi = QuantileDiscretizer(numBuckets=3, inputCol="AverageHCCRiskScore", outputCol="hcc_category")
data_mi = discretizer_mi.fit(data_mi).transform(data_mi)
data_mi.show(3)

# COMMAND ----------

#According to Bharat's missing value table
#drop everything irrelevant and everything has more than 30% of missing values, 17 columns

#drop "AverageHCCRiskScore", too. It is a strong indicator for hcc_category
#drop Zip too, because it will generate too many after one-hot encoding
columns_to_drop_mi = ['NPI', 'LastName', 'FirstName', 'MiddleInitial', 'Credentials', 'Gender', 'Address1', 'Address2', 'City', 'State', 'Country', 'MPI', 'zipcode','DSI','MSI','RaceNotClassifiedBeneficaryCount','AsianPacificIslanderBeneficaryCount','HispanicBeneficaryCount','AmericanIndianAlaskaNativeBeneficaryCount','BlackOrAfricanAmericanBeneficaryCount','PsychoticBeneficiaryPercent','StrokeBeneficiaryPercent','BeneficiaryCountMoreThan84','OsteoporosisBeneficiaryPercent','AsthmaBeneficiaryPercent','NonHispanicWhiteBeneficaryCount','BeneficiaryCountLessThan65','CancerBeneficiaryPercent','AlzheimersDementiaBeneficiaryPercent','AtrialFibrillationBeneficiaryPercent','AverageHCCRiskScore',"Zip"]
data_mi=data_mi.drop(*columns_to_drop_mi)

# COMMAND ----------

# now let's see how many categorical and numerical features we have:
cat_cols_mi = [item[0] for item in data_mi.dtypes if item[1].startswith('string')] 
print(str(len(cat_cols_mi)) + '  categorical features')
num_cols_mi = [item[0] for item in data_mi.dtypes if item[1].startswith('int') | item[1].startswith('double')]
print(str(len(num_cols_mi)) + '  numerical features')

# COMMAND ----------

num_cols_mi

# COMMAND ----------

data_mi = data_mi.na.fill(0)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator

#convert relevant categorical into one hot encoded
indexer1_mi = StringIndexer(inputCol="EntityCode", outputCol="EntityCodeIdx").setHandleInvalid("skip")
indexer2_mi = StringIndexer(inputCol="ProviderType", outputCol="ProviderTypeIdx").setHandleInvalid("skip")

#gather all indexers as inputs to the One Hot Encoder
inputs_mi = [indexer1_mi.getOutputCol(), indexer2_mi.getOutputCol()]

#create the one hot encoder
encoder_outputs_mi = ["EntityCodeVec", "ProviderTypeVec"]
encoder_mi = OneHotEncoderEstimator(inputCols=inputs_mi, outputCols=encoder_outputs_mi)

#run it through a pipeline
pipeline_mi = Pipeline(stages=[indexer1_mi, indexer2_mi,  encoder_mi])
encodedData_mi = pipeline_mi.fit(data_mi).transform(data_mi)

# COMMAND ----------

encoder_outputs_mi

# COMMAND ----------

#gather feature vector and identify features
features = encoder_outputs_mi + num_cols_mi
assembler_mi = VectorAssembler(inputCols = features, outputCol = 'features')
encodedData_mi = assembler_mi.transform(encodedData_mi)

# COMMAND ----------

# DBTITLE 1,Let's take a look at encodedData_mi
display(encodedData_mi)

# COMMAND ----------

columns_to_drop_mi = ['NPI', 'LastName', 'FirstName', 'MiddleInitial', 'Credentials', 'Gender', 'Address1', 'Address2', 'City', 'State', 'Country', 'MPI', 'zipcode','DSI','MSI','RaceNotClassifiedBeneficaryCount','AsianPacificIslanderBeneficaryCount','HispanicBeneficaryCount','AmericanIndianAlaskaNativeBeneficaryCount','BlackOrAfricanAmericanBeneficaryCount','PsychoticBeneficiaryPercent','StrokeBeneficiaryPercent','BeneficiaryCountMoreThan84','OsteoporosisBeneficiaryPercent','AsthmaBeneficiaryPercent','NonHispanicWhiteBeneficaryCount','BeneficiaryCountLessThan65','CancerBeneficiaryPercent','AlzheimersDementiaBeneficiaryPercent','AtrialFibrillationBeneficiaryPercent','AverageHCCRiskScore',"Zip"]
encodedData_mi=encodedData_mi.drop(*columns_to_drop_mi)

# COMMAND ----------


#split df_physicians_all_ml into train and test
train_df_mi, test_df_mi = encodedData_mi.randomSplit([.8,.2],seed=1234)
train_df_mi.show(1)

# COMMAND ----------

# DBTITLE 1,Logistic Regression for Multi Class Classification
#Classification
#Let us try to predict the hcc_category using logistic regression
from pyspark.ml.classification import LogisticRegression
# Set parameters for Logistic Regression
lgr = LogisticRegression(maxIter=10, featuresCol = 'features', labelCol='hcc_category')
# Fit the model to the data.
lgrm = lgr.fit(train_df_mi)
# Given a dataset, predict each point's label, and show the results.
predictions = lgrm.transform(test_df_mi)

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#print evaluation metrics
evaluator_lr = MulticlassClassificationEvaluator(labelCol="hcc_category", predictionCol="prediction")

print(evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"}))
print(evaluator.evaluate(predictions, {evaluator.metricName: "f1"}))

# COMMAND ----------

print(evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"}))
print(evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"}))

# COMMAND ----------

from pyspark.mllib.evaluation import MulticlassMetrics
predictionAndLabel = predictions.select("prediction", "hcc_category").rdd
# Generate confusion matrix
metrics = MulticlassMetrics(predictionAndLabel)
print(metrics.confusionMatrix())

# COMMAND ----------

m=metrics.confusionMatrix()
rows = m.toArray().tolist()
df_m = spark.createDataFrame(rows,['Low','Medium','High'])
df_m.show()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Random Forest
#RF model depth 1 numTress1
from pyspark.ml.classification import RandomForestClassifier

# Set parameters for the Random Forest.
rfc_0 = RandomForestClassifier(maxDepth=1, numTrees=1, impurity="gini", labelCol="hcc_category", featuresCol="features")

# Fit the model to the data.
rfcm_0 = rfc_0.fit(train_df_mi)

# Given a dataset, predict each point's label, and show the results.
predictions_0 = rfcm_0.transform(test_df_mi)

# COMMAND ----------

#print evaluation metrics
print(evaluator.evaluate(predictions_0, {evaluator.metricName: "accuracy"}))
print(evaluator.evaluate(predictions_0, {evaluator.metricName: "f1"}))

# COMMAND ----------

feature_importance = ExtractFeatureImp(rfcm_0.featureImportances,encodedData_mi, "features")
feature_importance

# COMMAND ----------

rfcm_0.featureImportances

# COMMAND ----------

#RF model 4,5
from pyspark.ml.classification import RandomForestClassifier
# Set parameters for the Random Forest.
rfc_1 = RandomForestClassifier(maxDepth=4, numTrees=5, impurity="gini", labelCol="hcc_category", predictionCol="prediction")
# Fit the model to the data.
rfcm_1 = rfc_1.fit(train_df_mi)
# Given a dataset, predict each point's label, and show the results.
predictions_1 = rfcm_1.transform(test_df_mi)

# COMMAND ----------

#print evaluation metrics
print(evaluator.evaluate(predictions_1, {evaluator.metricName: "accuracy"}))
print(evaluator.evaluate(predictions_1, {evaluator.metricName: "f1"}))

# COMMAND ----------

ExtractFeatureImp(rfcm_1.featureImportances,encodedData_mi, "features")

# COMMAND ----------

matrix_rf_mi=ExtractFeatureImp(rfcm_1.featureImportances,encodedData_mi, "features")

# COMMAND ----------

display(matrix_rf_mi.head(5))

# COMMAND ----------

#RF model depth 10, numTress10
from pyspark.ml.classification import RandomForestClassifier
# Set parameters for the Random Forest.
rfc_2 = RandomForestClassifier(maxDepth=10, numTrees=10, impurity="gini", labelCol="hcc_category", predictionCol="prediction")
# Fit the model to the data.
rfcm_2 = rfc_2.fit(train_df_mi)
# Given a dataset, predict each point's label, and show the results.
predictions_2 = rfcm_2.transform(test_df_mi)

# COMMAND ----------

#print evaluation metrics
print(evaluator.evaluate(predictions_2, {evaluator.metricName: "accuracy"}))
print(evaluator.evaluate(predictions_2, {evaluator.metricName: "f1"}))

# COMMAND ----------

matrix_rf_mi_final=ExtractFeatureImp(rfcm_2.featureImportances,encodedData_mi, "features")

# COMMAND ----------

# DBTITLE 1,RF 10,10 without EPA
display(matrix_rf_mi_final.head(5))

# COMMAND ----------

display(df_combined_all_epa)

# COMMAND ----------

data_test=df_combined_all_epa

# COMMAND ----------

data_test.printSchema()

# COMMAND ----------

#QuantileDiscretizer

from pyspark.ml.feature import QuantileDiscretizer

#High Medium Low
discretizer_test = QuantileDiscretizer(numBuckets=3, inputCol="AverageHCCRiskScore", outputCol="hcc_category")
data_test = discretizer_test.fit(data_test).transform(data_test)
data_test.show(3)

# COMMAND ----------

#drop all irrelavant columns or columns have and "AverageHCCRiskScore"
columns_to_drop_test = ['NPI', 'LastName', 'FirstName', 'MiddleInitial', 'Credentials', 'Gender', 'Address1', 'Address2', 'City', 'State', 'Country', 'MPI', 'zipcode','DSI','MSI','RaceNotClassifiedBeneficaryCount','AsianPacificIslanderBeneficaryCount','HispanicBeneficaryCount','AmericanIndianAlaskaNativeBeneficaryCount','BlackOrAfricanAmericanBeneficaryCount','PsychoticBeneficiaryPercent','StrokeBeneficiaryPercent','BeneficiaryCountMoreThan84','OsteoporosisBeneficiaryPercent','AsthmaBeneficiaryPercent','NonHispanicWhiteBeneficaryCount','BeneficiaryCountLessThan65','CancerBeneficiaryPercent','AlzheimersDementiaBeneficiaryPercent','AtrialFibrillationBeneficiaryPercent','AverageHCCRiskScore',"Zip","zip"]
data_test=data_test.drop(*columns_to_drop_test)

# COMMAND ----------

data_test = data_test.na.fill(0)

# COMMAND ----------

# DBTITLE 1,One hot encoder on data_test
from pyspark.ml.feature import VectorAssembler

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator

#convert relevant categorical into one hot encoded
indexer1_test = StringIndexer(inputCol="EntityCode", outputCol="EntityCodeIdx").setHandleInvalid("skip")
indexer2_test = StringIndexer(inputCol="ProviderType", outputCol="ProviderTypeIdx").setHandleInvalid("skip")

#gather all indexers as inputs to the One Hot Encoder
inputs_test = [indexer1_test.getOutputCol(), indexer2_test.getOutputCol()]

#create the one hot encoder
encoder_outputs_test = ["EntityCodeVec", "ProviderTypeVec"]
encoder_test = OneHotEncoderEstimator(inputCols=inputs_test, outputCols=encoder_outputs_test)

#run it through a pipeline
pipeline_test = Pipeline(stages=[indexer1_test, indexer2_test,  encoder_test])
encodedData_test = pipeline_test.fit(data_test).transform(data_test)

# COMMAND ----------

# now let's see how many categorical and numerical features we have:
cat_cols_test = [item[0] for item in data_test.dtypes if item[1].startswith('string')] 
print(str(len(cat_cols_test)) + '  categorical features')
num_cols_test = [item[0] for item in data_test.dtypes if item[1].startswith('int') | item[1].startswith('double')]
print(str(len(num_cols_test)) + '  numerical features')

# COMMAND ----------

num_cols_test

# COMMAND ----------

#gather feature vector and identify features
features_test = encoder_outputs_test + num_cols_test
assembler_test = VectorAssembler(inputCols = features_test, outputCol = 'features')


# COMMAND ----------

features_test

# COMMAND ----------

encodedData_test = assembler_test.transform(encodedData_test)

# COMMAND ----------

display(encodedData_test)

# COMMAND ----------

#split df_physicians_all_ml into train and test
train_df_test, test_df_test = encodedData_test.randomSplit([.8,.2],seed=1234)
train_df_test.show(1)

# COMMAND ----------

# DBTITLE 1,RF on EPA data
#RF model 4,5
from pyspark.ml.classification import RandomForestClassifier

# Set parameters for the Random Forest.
rfc_epa = RandomForestClassifier(maxDepth=4, numTrees=5, impurity="gini", labelCol="hcc_category", predictionCol="prediction")

# Fit the model to the data.
rfcm_epa = rfc_epa.fit(train_df_test)

# Given a dataset, predict each point's label, and show the results.
predictions_epa = rfcm_epa.transform(test_df_test)

# COMMAND ----------

print(evaluator.evaluate(predictions_epa, {evaluator.metricName: "accuracy"}))
print(evaluator.evaluate(predictions_epa, {evaluator.metricName: "f1"}))

# COMMAND ----------

matrix_rf_epa=ExtractFeatureImp(rfcm_2.featureImportances,encodedData_test, "features")

# COMMAND ----------

# DBTITLE 1,RF with EPA
display(matrix_rf_epa.head(5))
