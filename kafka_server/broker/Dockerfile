FROM python:3.10-slim-buster
WORKDIR /app
ARG BROKER_PROJECT_PATH="/app/"
RUN echo "The ARG variable value is $BROKER_PROJECT_PATH"
COPY requirements.txt /app
COPY kafka_server/broker/ .
RUN pip install -r requirements.txt
EXPOSE 5003
ENV FLASK_APP=app
CMD ["python", "-u", "controller/produce.py"]