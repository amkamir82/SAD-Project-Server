FROM python:3.10-slim-buster
WORKDIR /app
ARG COORDINATOR_PROJECT_PATH="/app/"
COPY requirements.txt /app
RUN pip install -r requirements.txt
COPY . .
EXPOSE 5000
ENV FLASK_APP=app
CMD ["python","main.py"]