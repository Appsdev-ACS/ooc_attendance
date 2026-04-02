FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Expose port for Flask
EXPOSE 8080

# Run Flask app (development)
# CMD ["python", "app.py"]

# Run with Gunicorn (production)
# CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--timeout", "3000", "--graceful-timeout", "3000"]