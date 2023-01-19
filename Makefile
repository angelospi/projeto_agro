black:
	@python3 -m black .

isort:
	@isort . --tc --up

pylint:
	@pylint .

up_containers:
	sudo docker compose up

build_container_airflow:
	sudo docker build -t airflow_image .
