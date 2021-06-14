# Rank the top 10 airlines by on-time arrival performance.

from pyspark.sql import SparkSession
from os import listdir
from os.path import isfile, isdir, join


def get_directories(path):
  directories = [f for f in listdir(path) if isdir(join(path, f))]
  return directories


def get_file_paths(path):
  files = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith('.csv')]
  return files[0]


def main():
  path_from = "./aviation/airline_ontime/1988"
  files = []

  directories = get_directories(path_from)
  print("directories", directories)

  for directory in directories:
    directory_path = path_from + "/" + directory
    file = get_file_paths(directory_path)
    files.append(directory_path + "/" + file)
  
  spark = SparkSession.builder.getOrCreate()
  data = spark.read.csv(files, header=True)

  data.createOrReplaceTempView("airlines")
  # spark.sql("SELECT * from airlines limit 1").show(vertical=True)
  spark.sql("SELECT AirlineID, SUM(ArrDelay) AS ontime FROM airlines GROUP BY AirlineID ORDER BY ontime LIMIT 10").show()


if __name__ == "__main__":
  main()
