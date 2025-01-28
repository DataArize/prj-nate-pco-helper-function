FROM python:3.11

# Set the working directory in the container
WORKDIR /app
# Copy the current directory contents into the container at /app
COPY . /app/

# Install pip dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the app
ENTRYPOINT ["python", "main.py"]
