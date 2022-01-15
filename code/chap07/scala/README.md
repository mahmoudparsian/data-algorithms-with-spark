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