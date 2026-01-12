# GenieIQ Dockerfile for Databricks Apps
FROM node:20-slim

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY frontend/package*.json ./frontend/

# Install dependencies
RUN npm install --production
RUN cd frontend && npm install

# Copy source code
COPY . .

# Build frontend
RUN cd frontend && npm run build

# Expose port
EXPOSE 8080

# Start the server
CMD ["node", "backend/server.js"]

