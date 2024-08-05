# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt --verbose

# Copy the shell script into the container
COPY run_scripts.sh /app/run_scripts.sh

# Make the shell script executable
RUN chmod +x /app/run_scripts.sh

# Make port 80 available to the world outside this container
EXPOSE 80

# Run the shell script when the container launches
CMD ["/app/run_scripts.sh"]