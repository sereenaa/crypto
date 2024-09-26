# Dockerfile
FROM public.ecr.aws/lambda/nodejs:20 AS build

# Set the working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN npm ci

# Copy the rest of your application code
COPY . .

# If you have a build step, add it here
# RUN npm run build

# Use the Lambda runtime interface client
FROM public.ecr.aws/lambda/nodejs:20

# Set the working directory
WORKDIR /usr/src/app

# Copy only the necessary files from the build stage
COPY --from=build /usr/src/app ./

# Expose the port your app runs on (if applicable)
EXPOSE 3000

# Define build arguments
ARG SBR_RPC
ARG DUNE_API_KEY

# Set environment variables from build arguments
ENV SBR_RPC=${SBR_RPC}
ENV DUNE_API_KEY=${DUNE_API_KEY}

# Command to run your application
CMD ["index.handler"]