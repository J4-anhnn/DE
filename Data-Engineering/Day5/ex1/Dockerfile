# Use Node.js 18 as base image with explicit platform
FROM --platform=linux/amd64 node:18-slim

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY . .

# Expose port
EXPOSE 5000

# Command to run the application
CMD ["npm", "start"] 