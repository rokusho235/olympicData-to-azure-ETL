# need these lib for re-casting columns

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

configs =   {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": "###",
            "fs.azure.account.oauth2.client.secret": '###',
            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/###/oauth2/token"}

# mounting the storage acc + container holding the raw-data

dbutils.fs.mount(
source = "abfss://containerName@storageAccountName.dfs.core.windows.net",
mount_point = "/mnt/olympic",
extra_configs = configs)

# testing to see if mount was successful

%fs
ls "/mnt/olympic"

# test loading one tbl from ADLS (Azure data lake storage) before doing the rest

athletes = spark.read.format("csv").option("header","true").load("/mnt/olympic/raw-data/Athletes.csv")

athletes.show()

# loading the rest of the tbls

coaches = spark.read.format("csv").option("header","true").load("/mnt/olympic/raw-data/Coaches.csv")
entriesGender = spark.read.format("csv").option("header","true").load("/mnt/olympic/raw-data/EntriesGender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympic/raw-data/Medals.csv") # inferSchema to have spark figure out the column data types
teams = spark.read.format("csv").option("header","true").load("/mnt/olympic/raw-data/Teams.csv")

# using printSchema to see what columns we need to change the data type on

entriesGender.printSchema()

# casting the correct data type for columns

entriesGender = entriesGender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
        .withColumn("Total",col("Total").cast(IntegerType()))

# medals tbl didn't need transformation cuz of the inferSchema during read

medals.printSchema()

# finding top countries in terms of # of golds

topGoldCountries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show() # select prunes out other columns

# calculating avg # of entries vs gender + disciplines
avgEntriesByGender = entriesGender.withColumn(
    'Avg_Female', entriesGender['Female'] / entriesGender['Total']
).withColumn(
    'Avg_Male', entriesGender['Male'] / entriesGender['Total']
)
avgEntriesByGender.show()

# loading data onto target location

athletes.write.mode("overwrite").option("header","true").csv("/mnt/olympic/transformed-data/athletes")   # this wrote into an athletes folder with 3 metadata files + 1 csv file.. all with crazy ID names

# repartition allows breaking up output file into smaller + distributed chunks

coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/olympic/transformed-data/coaches")
entriesGender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/olympic/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/olympic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/olympic/transformed-data/teams")