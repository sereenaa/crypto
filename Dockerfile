# Dockerfile
FROM node:20

# Set the working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of your application code
COPY . .

# Expose the port your app runs on (if applicable)
# EXPOSE 3000

# Define build arguments
ARG SBR_RPC
ARG DUNE_API_KEY

# Set environment variables from build arguments
ENV SBR_RPC=${SBR_RPC}
ENV DUNE_API_KEY=${DUNE_API_KEY}

# Command to run your application
CMD ["node", "get_payload.js"]