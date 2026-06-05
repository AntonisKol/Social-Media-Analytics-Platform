# Step 1: start from an official lightweight Python base image
FROM python:3.10-slim
# Step 2: set the working directory inside the container
WORKDIR /app
# Step 3: copy the simulator script into the container
COPY simulator.py .
# Step 4: install the required Python libraries
RUN pip install flask faker
# Step 5: tell Docker which port this container is used
EXPOSE 5000
# Step 6: the command that runs when the container starts
CMD ["python", "simulator.py"]