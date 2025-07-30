FROM python:3.11-slim

# Configure the working directory
RUN mkdir -p /app
WORKDIR /app
ARG GOOGLE_CLOUD_PROJECT=world-fishing-827
ENV GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT

COPY . /app/
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt
RUN pip install -e .


ENTRYPOINT ["python", "cli.py"]
