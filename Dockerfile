FROM apache/spark:3.5.7

USER root
COPY requirements.txt /app/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /app
USER spark
