include .env

setup:
	docker-compose up -d --force-recreate --remove-orphans
	sleep 120
	docker-compose exec airflow airflow users create --username admin --password admin --role Admin --firstname july --lastname lima --email admin@email.com
	sleep 20
	docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic topic-a
	docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic topic-b
	sleep 20
	docker-compose exec airflow airflow dags backfill -s now process_replication 	
down:
	docker-compose down
testing:
	docker-compose exec airflow pytest -v
stop:
	docker-compose down
very-danger:
	docker system prune -a --volumes 

