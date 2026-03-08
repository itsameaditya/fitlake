FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir \
    streamlit plotly altair duckdb pandas numpy \
    pyiceberg[s3fs,duckdb,pandas] boto3 s3fs loguru python-dotenv

COPY analytics/ ./analytics/
COPY pipeline/ ./pipeline/
COPY sql/ ./sql/
COPY data/ ./data/

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "analytics/dashboard.py", \
    "--server.port=8501", "--server.address=0.0.0.0", \
    "--server.headless=true"]
