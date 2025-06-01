FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/process_csv.py ./src/process_csv.py

ENTRYPOINT ["python", "src/process_csv.py"]

