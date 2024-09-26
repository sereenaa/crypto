# Dockerfile
FROM public.ecr.aws/lambda/nodejs:18

# Copy package.json and package-lock.json
COPY package*.json ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN npm install

# Copy the rest of your application code
COPY index.js ${LAMBDA_TASK_ROOT}

# Define build arguments
ARG SBR_RPC
ARG DUNE_API_KEY

# Set environment variables from build arguments
ENV SBR_RPC=${SBR_RPC}
ENV DUNE_API_KEY=${DUNE_API_KEY}

# Command to run your application
CMD ["index.handler"]