# Use the official Python image from the Docker Hub
FROM python:3.12.6

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose port 8000 (or any other port your application uses)
EXPOSE 8000

# Command to run the application
CMD ["python", "app.py"]