FROM quay.io/astronomer/astro-runtime:12.9.0

USER root
RUN apt-get update && apt-get install -y jq
RUN pip install pandas sqlalchemy psycopg2-binary boto3
