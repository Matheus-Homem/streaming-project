FROM python:3.12-slim

WORKDIR /app

COPY requirements/ ./requirements/

RUN pip install --no-cache-dir -r requirements/ingestion.txt

COPY ingestion/ ./ingestion/
COPY shared/ ./shared/


CMD ["python", "-m", "ingestion.app", "--source", "github"]