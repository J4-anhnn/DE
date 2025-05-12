FROM python:3.9

RUN pip install dbt-core dbt-postgres

WORKDIR /dbt_project

COPY profiles.yml /root/.dbt/
COPY . /dbt_project/

EXPOSE 5432

CMD ["tail", "-f", "/dev/null"]
