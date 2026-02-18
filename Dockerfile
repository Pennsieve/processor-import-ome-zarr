FROM python:3.12

WORKDIR /app

COPY processor/requirements.txt /app/processor/requirements.txt
RUN pip install -r /app/processor/requirements.txt

COPY processor/ /app/processor

ENV PYTHONPATH="/app"

CMD ["python3.12", "-m", "processor.main"]
