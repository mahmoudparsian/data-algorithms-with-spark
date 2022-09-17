from __future__ import print_function 
import sys 
import csv
from pyspark.sql import SparkSession
import pandas as pd 

#------------------------------------
#------------------------------------
# Authors:
#   Mahmoud Parsian (https://github.com/mahmoudparsian/)
#   Krishna Sai Tejaswini Kambhampati (https://github.com/Tejaswini-popuri/)
#------------------------------------
#------------------------------------
# rec: userId,movieId,rating,timestamp
# returns a tuple(movieId, userid)
def create_pair(rec):
  tokens = rec.split(",")
  userId = tokens[0]
  movie_id = tokens[1]
  #rating = float(tokens[2])
  # timestamp = tokens[3]
  
  return (movie_id, userId)
#end-def
#------------------------------------
# create a dictionary{(movie_id, movie_name)}
def create_dict(movies):
  movies_dict = {}
  with open(movies, mode='r') as movies_file:
    reader = csv.reader(movies_file)
    movies_dict = {rows[0]:rows[1] for rows in reader}
  #
  return movies_dict
#end-def
#------------------------------------

# make sure we have 5 parameters
if len(sys.argv) != 5:  
  print("Usage: <prog> <N> <ratings> <movies> <rating_threshold>", file=sys.stderr)
  exit(-1)

# define Top-N
N = int(sys.argv[1])
print("N=", N)

# define ratings input path
# each rating record: userId,movieId,rating,timestamp
ratings = sys.argv[2]  
print("ratings=", ratings)

# define movies input path
# each movies record: movieId,title,genres
movies = sys.argv[3]  
print("movies=", movies)

# if a rating is less than rating_threshold,
# then that record will be dropped
# if you do not want to drop any records, then set rating_threshold = 0
rating_threshold = float(sys.argv[4])
print("rating_threshold=", rating_threshold)

# create an instance of SparkSession
spark = SparkSession.builder.getOrCreate()
    
# read ratings and create RDD[String]
rdd = spark.sparkContext.textFile(ratings)
print("rdd.take(5)=", rdd.take(5))

# Save the first line of the file as header
header = rdd.first()
    
# filter the header and keep the rest of the records
filtered_rdd = rdd.filter(lambda row: row != header)
       
# create RDD[(movie_id, rating)]
pairs = filtered_rdd.map(create_pair)
# Use 'take' action to see the first 5 records
print("pairs.take(5)=", pairs.take(5))

# drop undesired pairs
# if rating_threshold <= 0, then no filtering will be done
# p : (movie_id, rating)
if (rating_threshold > 0):
  filtered = pairs.filter(lambda p: p[1] > rating_threshold)
else:
  filtered = pairs
  
# Use 'take' action to see the first 5 records
print("filtered.take(5)=", filtered.take(5))
    
# Finding top 10 movies based on number of times it is seen
# Use time module to analyze performance

# Use groupByKey() to bring the data to this format 
# (movie_id, <Iterable of values>) and then count
# the number of raters per movie_id
grouped = filtered.groupByKey().mapValues(lambda x: len(x))
# Use 'take' action to see the first 5 records
print("grouped.take(5)=", grouped.take(5))
    
# Using takeOrdered will both sort the values 
# using key parameter and prints to output
# top-N (N can be 5, 10, 20, ...)
topN = grouped.takeOrdered(N, key = lambda x: -x[1])

# now, find name of movies (by using movie_id)
 
# creating a data frame
movies_df = pd.read_csv(movies)
print(movies_df.head())

for col in movies_df.columns:
  print(col)

# create a dictionary{(movie_id, movie_name)}
movies_dict = create_dict(movies)

# add a movie name to top 10 list:
topN_with_names = []
for t2 in topN:
  if (t2[0] in movies_dict):
    topN_with_names.append((t2[0], t2[1], movies_dict[t2[0]]))
  else:
    topN_with_names.append((t2[0], t2[1], None))

print("topN_with_names=", topN_with_names)
 
# done!
spark.stop()
    
