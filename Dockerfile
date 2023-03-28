FROM python:3.9-slim

RUN apt-get update -y && apt-get install libpq-dev -y && apt-get install gcc -y

COPY requirements.txt airflow/

RUN pip3 install -r airflow/requirements.txt

WORKDIR airflow

CMD ["airflow", "webserver", "-p", "8080"]

EXPOSE 8080
