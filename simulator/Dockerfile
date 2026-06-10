FROM python:3.10-slim

WORKDIR /app

COPY simulator.py .

RUN pip install flask faker

EXPOSE 5000

CMD ["python", "simulator.py"]