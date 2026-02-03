# 1. Start with a lightweight Linux Setup with Python 3.11 installed
FROM python:3.11-slim

# 2. Create a folder inside the container called '/app'
WORKDIR /app

# 3. Copy the "requirements.txt" file from your laptop to the container
COPY requirements.txt .

# 4. Install the libraries inside the container
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the rest of your code (etl_bigquery.py, etc.)
COPY . .

# 6. The command to run when the container starts
CMD ["python", "etl_bigquery.py"]