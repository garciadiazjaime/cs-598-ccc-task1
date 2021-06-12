from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

path = "./aviation/airline_origin_destination/1993_Q1/Origin_and_Destination_Survey_DB1BCoupon_1993_1.csv"
# path = "./aviation/air_carrier_statistics_ALL/t-100_domestic_market/1990/349108460_T_T100D_MARKET_ALL_CARRIER_1990_All.csv"
# path = "./aviation/air_carrier_statistics_summary/RAA\ Commuter\ and\ Small\ Certificated\ Air\ Carrier\ Traffic\ and\ Capacity/252976999_T_RAA_SUMMARY.csv"
# path = "./aviation/air_carrier_statistics_summary/T1\ U.S.\ Air\ Carrier\ Traffic\ And\ Capacity/SCHEDULE_T1_1974/252976999_T_SCHEDULE_T1_1974_All.csv"
# path = "./aviation/air_carrier_statistics_US/t-100_domestic_market/1990/349108460_T_T100D_MARKET_US_CARRIER_ONLY_1990_All.csv"
# path = "./aviation/airline_ontime/1988/On_Time_On_Time_Performance_1988_1/On_Time_On_Time_Performance_1988_1.csv"

data = spark.read.csv(path, header=True)

data.createOrReplaceTempView("airlines")
spark.sql("SELECT * from airlines limit 1").show(vertical=True)

spark.sql("SELECT Origin, Dest, count(*) as count FROM airlines GROUP BY Origin, Dest ORDER BY count DESC LIMIT 10").show()


