FROM python:3.11-slim

# Configure the working directory
RUN mkdir -p /app
WORKDIR /app

COPY . /app/
RUN pip install -r /app/requirements.txt

ENTRYPOINT ["python"]
