black:
	@python3 -m black .

isort:
	@isort . --tc --up

pylint:
	@pylint .

up_containers:
	docker compose up

build_container_airflow:
	docker build -t airflow_image .
