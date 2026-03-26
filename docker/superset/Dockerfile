FROM apache/superset:3.1.0

USER root
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    pillow>=10.0.0

# Copy config and scripts
COPY superset_config.py /app/pythonpath/superset_config.py
COPY init_superset.sh /app/docker/init_superset.sh
COPY dashboard/ /app/docker/dashboard/

RUN chmod +x /app/docker/init_superset.sh

USER superset
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

ENTRYPOINT ["/bin/bash", "/app/docker/init_superset.sh"]
