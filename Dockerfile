FROM python:3.9-slim

WORKDIR /app

# Copy local code to the container image.
COPY . ./

# Install dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Run the script.
# Note: arguments will be passed via Cloud Run args
ENTRYPOINT ["python", "amazon_ppc_automation.py"]
