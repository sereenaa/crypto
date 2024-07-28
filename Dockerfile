# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Install dependencies for rustup and cryo
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libssl-dev \
    pkg-config \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Add rust to the PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Verify Rust installation
RUN rustc --version

# Install necessary Python libraries
RUN pip install --no-cache-dir maturin pandas polars pyarrow python-dotenv web3 --verbose

# Clone the cryo repository
RUN git clone https://github.com/paradigmxyz/cryo

# Navigate to the Python directory and build cryo_python
WORKDIR /app/cryo/crates/python
RUN maturin build --release

# Debug: List the contents of the target directory to verify the build
RUN ls -al /app/cryo/target

# Debug: List the contents of the target/wheels directory to verify the .whl file is created
RUN ls -al /app/cryo/target/wheels

# Install the Python wrapper
RUN pip install --force-reinstall /app/cryo/target/wheels/*.whl --verbose

# Set the working directory back to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt --verbose

# Make port 80 available to the world outside this container
EXPOSE 80

# Run main.py when the container launches
CMD ["python", "main.py"]
