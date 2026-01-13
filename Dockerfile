FROM python:3.11

# Create a non-root user
RUN useradd -m -u 1000 appuser

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app/

# Install pip dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Change ownership of /app to the non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Command to run the app
ENTRYPOINT ["python", "main.py"]
