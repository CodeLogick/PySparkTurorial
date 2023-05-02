from pyspark.sql import *
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName('Hello Spark').master('local[2]').getOrCreate()

df = spark.read\
    .option('inferSchema', 'true')\
    .option('header', 'true')\
    .csv('/Users/snishad/SparkDemo/01-HelloSpark/data/sample.csv')

# df.select('Age', 'Gender', 'Country', 'state').show()

filter_df = df.where('Age > 40')\
            .select('Age', 'Gender', 'Country', 'state')\
            .groupBy('Country')
count_df = filter_df.count()
print(count_df)

