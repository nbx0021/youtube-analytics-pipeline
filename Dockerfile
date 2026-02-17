FROM python:3.11-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the whole project
COPY . .

# Environment variables for Python to find modules
ENV PYTHONPATH=/app/src

EXPOSE 8501

# Default command remains the Dashboard
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]