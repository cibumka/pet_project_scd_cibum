FROM apache/airflow:2.10.3-python3.12

# Docker CLI (client only) so tasks can `docker exec` into the spark-iceberg
# container. No daemon needed here - /var/run/docker.sock is bind-mounted in
# and the scheduler container runs as root (see docker-compose.yml).
USER root
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.3.1.tgz -o /tmp/docker.tgz \
    && tar xzf /tmp/docker.tgz -C /tmp \
    && mv /tmp/docker/docker /usr/local/bin/docker \
    && rm -rf /tmp/docker /tmp/docker.tgz

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
