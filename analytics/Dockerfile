FROM python:3.11-slim

WORKDIR /app

# Install from the same pinned file the hosted deploy uses, so the container
# and Streamlit Cloud cannot drift apart. The previous hardcoded list omitted
# statsmodels (needed by the OLS trendline) and click (needed by the data
# generator the dashboard calls on first boot).
COPY analytics/requirements.txt ./analytics/requirements.txt
RUN pip install --no-cache-dir -r analytics/requirements.txt

COPY analytics/ ./analytics/
COPY pipeline/ ./pipeline/
COPY sql/ ./sql/
COPY data/ ./data/

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "analytics/dashboard.py", \
    "--server.port=8501", "--server.address=0.0.0.0", \
    "--server.headless=true"]
