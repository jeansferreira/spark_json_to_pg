from pyspark.sql import types as T
import pyspark.sql.functions as F
import json
import pandas as pd    

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType


import json  
import pandas as pd  
from pandas.io.json import json_normalize  
  
data = [
	{
		"state": "Florida",
		"shortname": "FL",
		"info": {
			"governor": "Rick Scott"
		},
		"counties": [
			{
				"name": "Dade",
				"population": 12345
			},
			{
				"name": "Broward",
				"population": 40000
			},
			{
				"name": "Palm Beach",
				"population": 60000
			}
		]
	},
	{
		"state": "Ohio",
		"shortname": "OH",
		"info": {
			"governor": "John Kasich"
		},
		"counties": [
			{
				"name": "Summit",
				"population": 1234
			},
			{
				"name": "Cuyahoga",
				"population": 1337
			}
		]
	}
]

from pandas.io.json import json_normalize
# result = json_normalize(data, 'counties', ['state', 'shortname',['info', 'governor']])
result = json_normalize(data, 'counties', ['state', 'shortname',['info', 'governor']])


print(result)

# appName = "PySpark Example - JSON file to Spark Data Frame"
# master = "local"

# # Create Spark session
# spark = SparkSession.builder \
#     .appName(appName) \
#     .master(master) \
#     .getOrCreate()

# def flatten(df):
#     complex_fields = dict([
#         (field.name, field.dataType) 
#         for field in df.schema.fields 
#         if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
#     ])
    
#     qualify = list(complex_fields.keys())[0] + "_"

#     while len(complex_fields) != 0:
#         col_name = list(complex_fields.keys())[0]
        
#         if isinstance(complex_fields[col_name], T.StructType):
#             expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
#                         for k in [ n.name for n in  complex_fields[col_name]]
#                        ]
            
#             df = df.select("*", *expanded).drop(col_name)
    
#         elif isinstance(complex_fields[col_name], T.ArrayType): 
#             df = df.withColumn(col_name, F.explode_outer(col_name))
    
      
#         complex_fields = dict([
#             (field.name, field.dataType)
#             for field in df.schema.fields
#             if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
#         ])
        
        
#     for df_col_name in df.columns:
#         df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))

#     return df

# # multiline_df = spark.read.option("multiline","true").json("example.json").flatten
# # multiline_df.show(True)  

# import requests
# from pandas.io.json import json_normalize
# url = 'https://www.energidataservice.dk/proxy/api/datastore_search?resource_id=nordpoolmarket&limit=5'

# response = requests.get(url)
# dictr = response.json()
# print(dictr)
# recs = dictr['result']['records']
# df = json_normalize(recs)
# print(df)