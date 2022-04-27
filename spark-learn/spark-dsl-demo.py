# import os
# os.environ['SPARK_HOME'] = "/usr/local/src/spark"
# os.environ['HADOOP_HOME'] = "/usr/local/src/hadoop"
# os.environ['PYSPARK_PYTHON'] = "/home/hadoop/anaconda3/envs/pyspark/bin/python"
# os.environ['PYSPARK_DRIVER_PYTHON'] = "/home/hadoop/anaconda3/envs/pyspark/bin/python"



from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName("AuthorsAges")
             .getOrCreate())

    data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], ["name", "age"])

    #按名字分组，聚合同名人的年龄，计算平均值
    avg_df = data_df.groupBy("name").agg(avg("age"))

    #展示执行的最终结果
    avg_df.show()

