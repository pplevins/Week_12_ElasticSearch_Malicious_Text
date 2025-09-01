@REM Setting the ElasticSearch container for local run
docker run -d --name es -p 9200:9200 -e "discovery.type=single-node" -e "xpack.security.enabled=false" -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" docker.elastic.co/elasticsearch/elasticsearch:8.15.0

@REM Setting the docker containers
cd C:\Users\share\PycharmProjects\kodkode\ElasticSearch_Malicious_Text
docker compose -f compose.yaml up -d

@REM Pushing code image to DockerHub
docker tag elasticsearch_malicious_text-server:latest pplevins/elasticsearch_malicious_text-server:v1.0
docker tag elasticsearch_malicious_text-server:latest pplevins/elasticsearch_malicious_text-server:latest
docker push pplevins/elasticsearch_malicious_text-server:v1.0
docker push pplevins/elasticsearch_malicious_text-server:latest