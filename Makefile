include .env

setup:
	docker-compose up -d --force-recreate --remove-orphans
	sleep 120
	docker-compose exec airflow airflow users create --username admin --password admin --role Admin --firstname july --lastname lima --email admin@email.com
	sleep 30
	docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic action-data
	sleep 60
	docker-compose exec airflow airflow dags backfill -s now load_user_action_data 

	docker-compose exec -e 'commit.interval.ms'='2000' ksqldb-cli env
	docker-compose exec -e 'cache.max.bytes.buffering'='10000000' ksqldb-cli  env
	docker-compose exec -e 'auto.offset.reset'='earliest' ksqldb-cli  env
	
down:
	docker-compose down
testing:
	docker-compose exec airflow pytest -v
stop:
	docker-compose down

