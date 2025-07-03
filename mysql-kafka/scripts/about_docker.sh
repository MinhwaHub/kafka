#토픽 삭제
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic mysql_data_topic
# 데이터 디렉토리 초기화
rm -rf mysql-kafka/kafka_data
# 카프카 데이터 초기화
docker-compose down -v

#컨테이너 내리기
docker-compose -f docker-compose.yml down

#컨테이너 시작하기
docker-compose -f docker-compose.yml up -d
# docker-compose up -d (same)


# 한번에
docker-compose down -v && sudo rm -rf kafka_data && docker-compose up -d