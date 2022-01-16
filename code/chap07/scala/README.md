Scala Solutions

#### Run Redis container on Docker
```shell
docker run --name some-redis -p 6379:6379 -d redis redis-server --save 60 1 --loglevel warning
```
#### Load Data in Redis Env
```shell
docker exec some-redis redis-cli hset people:Alex city "Ames" age 50
docker exec some-redis redis-cli hset people:Gandalf city "Cupertino" age 60
docker exec some-redis redis-cli hset people:Thorin city "Sunnyvale" age 95
docker exec some-redis redis-cli hset people:Betty city "Ames" age 78
docker exec some-redis redis-cli hset people:Brian city "Stanford" age 77
```
docker network create somenetwork
docker run -d --name elasticsearch --net somenetwork -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:7.16.3
docker run -d --name kibana --net somenetwork -p 127.0.0.1:5601:5601 -e "ELASTICSEARCH_HOSTS=http://elasticsearch:9200" kibana:7.16.3
```shell
docker exec elasticsearch curl -H "Content-Type: application/json" -XPOST "http://localhost:9200/indexname/testindex/testdoc0" -d "{\"key1\": \"some_value1\", \"doc_id\": 100}"
docker exec elasticsearch curl -H "Content-Type: application/json" -XPOST "http://localhost:9200/indexname/testindex/testdoc1" -d "{\"key2\": \"some_value2\", \"doc_id\": 200}"
docker exec elasticsearch curl -H "Content-Type: application/json" -XPOST "http://localhost:9200/indexname/testindex/testdoc2" -d "{\"key3\": \"some_value3\", \"doc_id\": 300}"
docker exec elasticsearch curl -H "Content-Type: application/json" -XPOST "http://localhost:9200/indexname/testindex/testdoc3" -d "{\"key4\": \"some_value4\", \"doc_id\": 400}"
curl -H "Content-Type: application/json" -XPOST "http://localhost:9200/indexname/typename/optionalUniqueId" -d "{ \"field\" : \"value\"}"


```