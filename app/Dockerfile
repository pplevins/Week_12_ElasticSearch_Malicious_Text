FROM python:3.12-slim
LABEL authors="pplevins"
WORKDIR /app
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY app/loader ./loader
COPY data ./data
COPY app/api ./api
COPY app/processor ./processor
COPY app/main.py main.py
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]