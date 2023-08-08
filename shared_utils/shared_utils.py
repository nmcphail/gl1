print("hello")

spark.sql("use catalog development1")
spark.sql("use schema gl1_b")
spark.read.table("acdoca").show()



