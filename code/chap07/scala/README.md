# Chapter 7

## Programs
The programs cover reading and writing from different sources and different file types.  
The steps to create a local setup of different sources is also added in the form of docker commands.
Run the docker commands in case you want to have a local running set-up but keep in mind that you should have a docker 
daemon running locally.

### Different Sources
For the sources, run the writer first and then reader so that it will load the data in different sources.
#### 1. JDBC
**Docker Set-up**(incase you want to have your own local instance)
```shell
# Create a mysql docker container and run it in background
docker run --name some-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:latest
# Create database metadb
docker exec some-mysql mysql -pmy-secret-pw -e "CREATE DATABASE if not exists metadb"
```
**JDBC Reader**  
`org.data.algorithms.spark.ch07.DatasourceJDBCReader` (Spark Application)  
`./run_spark_applications_scripts/datasource_jdbc_reader.sh` (Shell Script)

**JDBC Writer**  
`org.data.algorithms.spark.ch07.DatasourceJDBCWriter` (Spark Application)  
`./run_spark_applications_scripts/datasource_jdbc_writer.sh` (Shell Script)

#### 2. MONGO DB
**Docker Set-up**(incase you want to have your own local instance)
```shell
# Create a mongo docker container some-mongo and run it in background
docker run -p 27017:27017 --name some-mongo -d mongo:5.0.5
```
**MongoDB Reader**  
`org.data.algorithms.spark.ch07.DatasourceMongodbReader` (Spark Application)  
`./run_spark_applications_scripts/datasource_mongodb_reader.sh` (Shell Script)

**MongoDB Writer**  
`org.data.algorithms.spark.ch07.DatasourceMongodbWriter` (Spark Application)  
`./run_spark_applications_scripts/datasource_mongodb_writer.sh` (Shell Script)

#### 3. REDIS
**Docker Set-up**(incase you want to have your own local instance)
```shell
# Create a redis docker container some-redis and run it in background
docker run --name some-redis -p 6379:6379 -d redis redis-server --save 60 1 --loglevel warning
```
**REDIS Reader**  
`org.data.algorithms.spark.ch07.DatasourceRedisReader` (Spark Application)  
`./run_spark_applications_scripts/datasource_redis_reader.sh` (Shell Script)

**REDIS Writer**  
`org.data.algorithms.spark.ch07.DatasourceRedisWriter` (Spark Application)  
`./run_spark_applications_scripts/datasource_redis_writer.sh` (Shell Script)

#### 4. ElasticSearch
**Docker Set-up**(incase you want to have your own local instance)
```shell
# Create a Elasticsearch docker container elasticsearch and run it in background
docker network create somenetwork
docker run -d --name elasticsearch --net somenetwork -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:7.16.3
```
**ElasticSearch Reader**  
`org.data.algorithms.spark.ch07.DatasourceElasticsearchReader` (Spark Application)  
`./run_spark_applications_scripts/datasource_elasticsearch_reader.sh` (Shell Script)

**ElasticSearch Writer**  
`org.data.algorithms.spark.ch07.DatasourceElasticsearchWriter` (Spark Application)  
`./run_spark_applications_scripts/datasource_elasticsearch_writer.sh` (Shell Script)

### Different File Types
#### 1. CSV
**CSV Reader with no header**  
`org.data.algorithms.spark.ch07.DatasourceCSVReaderNoHeader` (Spark Application)  
`./run_spark_applications_scripts/datasource_csv_reader_no_header.sh` (Shell Script)

**CSV Reader with header**  
`org.data.algorithms.spark.ch07.DatasourceCSVReaderHeader` (Spark Application)  
`./run_spark_applications_scripts/datasource_csv_reader_header.sh` (Shell Script)

**CSV Writer**  
`org.data.algorithms.spark.ch07.DatasourceCSVWriter` (Spark Application)  
`./run_spark_applications_scripts/datasource_csv_writer.sh` (Shell Script)

#### 2. JSON
**JSON Reader Single Line**  
`org.data.algorithms.spark.ch07.DatasourceJSONReaderSingleLine` (Spark Application)  
`./run_spark_applications_scripts/datasource_json_reader_single_line.sh` (Shell Script)

**JSON Reader Multi Line**  
`org.data.algorithms.spark.ch07.DatasourceJSONReaderMultiLine` (Spark Application)  
`./run_spark_applications_scripts/datasource_json_reader_multi_line.sh` (Shell Script)

#### 3. GZIP
**GZIP Reader**  
`org.data.algorithms.spark.ch07.DatasourceGZIPReader` (Spark Application)  
`./run_spark_applications_scripts/datasource_gzip_reader.sh` (Shell Script)

#### 4. TEXTFILE
**TextFile Reader**  
`org.data.algorithms.spark.ch07.DatasourceTextfileReader` (Spark Application)  
`./run_spark_applications_scripts/datasource_textfile_reader.sh` (Shell Script)

**TextFile Writer**  
`org.data.algorithms.spark.ch07.DatasourceTextfileWriter` (Spark Application)  
`./run_spark_applications_scripts/datasource_textfile_writer.sh` (Shell Script)