from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import HiveContext
import json
import pyspark

#spark = SparkSession \
#       .builder \
#       .appName("Dashapp") \
#       .getOrCreate()

#sqlContext = HiveContext(sc)
sess = SparkSession(sc)
forecastIn=("/user/hadoop/forecast_original.csv")

forecastschema = StructType ([
                                StructField("period_end_date", DateType(),True),
                                StructField("rent_space_code", StringType(),True),
                                StructField("forecast_period_end_date", DateType(),True),
                                StructField("quarter", IntegerType(),True),
                                StructField("iteration", IntegerType(),True),
                                StructField("erv", FloatType(),True),
                                StructField("net_income", FloatType(),True),
                                StructField("capital_value", FloatType(),True)
])

forecastInDF = sess.read.format("csv").option("header","true").schema(forecastschema).load(forecastIn)\
                                       .withColumn('qtr_number',F.lit(23).cast(IntegerType()))

#added cache
forecastInDF.cache()
forecastInDF.createOrReplaceTempView('forecast_df')

dfsql = "select period_end_date,rent_space_code,'PROP'||substr(rent_space_code,2,4) as property_code, iteration,quarter,forecast_period_end_date,qtr_number + quarter as cal_qtr_number,net_income,erv,capital_value,lead(net_income) over (partition by rent_space_code,iteration order by quarter) as net_income_1 from forecast_df"

df = sess.sql(dfsql).orderBy('period_end_date','rent_space_code','iteration','forecast_period_end_date')
df.createOrReplaceTempView('dash_df')

dxsql = " select period_end_date,property_code,iteration,forecast_period_end_date,cal_qtr_number,sum(net_income)   as net_income,sum(net_income_1)  as net_income1,sum(erv)   as erv,sum(capital_value)  as capital_value, (sum(net_income_1) - sum(net_income))  as net_income_diff,(sum(net_income_1) - sum(net_income))/sum(net_income)   as net_income_pq, ln(1 + ((sum(net_income_1) - sum(net_income))/sum(net_income)))    as ln_net_income_pq from dash_df  where property_code = '$modifier'  group by period_end_date,property_code,iteration,forecast_period_end_date,cal_qtr_number"

dx = sess.sql(dxsql).orderBy('period_end_date','property_code','forecast_period_end_date','iteration')

pWindow1y = W.partitionBy('period_end_date','property_code','iteration').orderBy('cal_qtr_number').rangeBetween(0,3)
pWindow3y = W.partitionBy('period_end_date','property_code','iteration').orderBy('cal_qtr_number').rangeBetween(0,11)
pWindow5y = W.partitionBy('period_end_date','property_code','iteration').orderBy('cal_qtr_number').rangeBetween(0,19)

prop = dx.withColumn('net_income_1y_pa',((F.exp(F.sum('ln_net_income_pq').over(pWindow1y)))-1)) \
         .withColumn('net_income_3y_pa',((F.exp(F.sum('ln_net_income_pq').over(pWindow3y) * 4/12))-1)) \
         .withColumn('net_income_5y_pa',((F.exp(F.sum('ln_net_income_pq').over(pWindow5y) * 4/20))-1)) \
         .orderBy('period_end_date','property_code','iteration','forecast_period_end_date')


tot = prop.groupBy('period_end_date','property_code','forecast_period_end_date') \
	.agg(F.mean('net_income_pq').alias('avg_net_income_pq'), F.stddev('net_income_pq').alias('std_net_income_pq')) \
	.orderBy('period_end_date','property_code','forecast_period_end_date')

data = tot.toPandas()

namelist=data.columns.tolist()
depths = [[] for i in range(len(data.columns.tolist()))]
depth_i=0
for col in (data.columns.tolist()):
    depths[depth_i]= data[col].tolist()
    depth_i=depth_i+1

payload={'x':depths, 'y':namelist}

print(json.dumps(payload,sort_keys=True,default=str))
