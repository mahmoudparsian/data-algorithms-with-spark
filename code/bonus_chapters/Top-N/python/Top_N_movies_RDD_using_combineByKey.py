from __future__ import print_function 
import sys 
import csv
import pandas as pd
from pyspark.sql import SparkSession 

#------------------------------------
#------------------------------------
# Authors:
#   Mahmoud Parsian (https://github.com/mahmoudparsian/)
#   Krishna Sai Tejaswini Kambhampati (https://github.com/Tejaswini-popuri/)
#------------------------------------
#------------------------------------
# rec: userId,movieId,rating,timestamp
# returns a tuple(movieId, userId)
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
rdd_2 = rdd.filter(lambda row: row != header)
    
# create RDD[(movie_id, rating)]
pairs = rdd_2.map(create_pair)
# Use 'take' action to see the first 5 records
print("pairs.take(5)=", pairs.take(5))

# drop undesired pairs
# if rating_threshold <= 0, then no filtering will be done
# p : (movie_id, rating)
if (rating_threshold > 0):
  filtered = pairs.filter(lambda p: p[1] > rating_threshold)
else:
  filtered = pairs
  
# Find the top 10 movies based on number of times it is seen
# Use time module to analyze performance     
# Use map function to extract the (Key, value) pairs
mapped = filtered.map(lambda x: (x[1],1))
# Use combineByKey() function to find the count per key
combined = mapped.combineByKey(\
  lambda v: v,\
  lambda v,x: v+x,\
  lambda x,y: x+y)
    
# Sort the values using sortBy() using values obtained from 'combined' 
sorted_rdd = combined.sortBy(lambda x: x[1],ascending=False)

# top N
topN_movies = sorted_rdd.take(N)
print("topN_movies=", topN_movies)

# creating a data frame
movies_df = pd.read_csv(movies)
print(movies_df.head())

# print movies column names
print("movies column names:")
for col in movies_df.columns:
  print(col)

# create a dictionary{(movie_id, movie_name)}
movies_dict = create_dict(movies)

# add a movie name to top 10 list:
topN_with_names = []
for t2 in topN_movies:
  if (t2[0] in movies_dict):
    topN_with_names.append((t2[0], t2[1], movies_dict[t2[0]]))
  else:
    topN_with_names.append((t2[0], t2[1], None))

print("topN_with_names=", topN_with_names)
 
# done!
spark.stop()