FROM quay.io/astronomer/astro-runtime:12.7.0

# install dbt into a virtual environment
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r requirements.txt && \
    deactivate

COPY profiles.yml /home/astro/.dbt/profiles.yml