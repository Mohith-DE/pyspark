import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)
df.show()
df.write.mode("overwrite").parquet("output_data1.parquet")
parDf=spark.read.parquet("output_data1.parquet")
parDf.show()
parDf.printSchema()
parDf.createOrReplaceTempView("ParquetTable")
parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
spark.sql("CREATE TEMPORARY VIEW PERSON USING parquet OPTIONS (path \"output_data1.parquet\")")
spark.sql("SELECT * FROM PERSON").show()
#ashpscslmjhcibhvolad