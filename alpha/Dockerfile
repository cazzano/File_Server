FROM ubuntu:22.04

# Prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install supervisor, Python, and pip
RUN apt update && apt upgrade -y
RUN apt install python3 python3-pip libmagic1 -y

# Create directories for frontend and backend
WORKDIR /app
RUN mkdir -p /app/file_server

# Setup frontend
#WORKDIR /app/frontend
#COPY frontend/ ./
# Uncomment these if you need to build a Node.js frontend
# RUN apt install -y nodejs npm
# COPY frontend/package*.json ./
# RUN npm install
# RUN npm run build

# Setup backend
WORKDIR /app/file_server
# Make sure requirements.txt exists in your backend directory
COPY file_server/requirements.txt ./
# Use pip3 with -r flag to specify requirements file
RUN pip3 install --no-cache-dir -r requirements.txt
COPY file_server/ ./

# Configure supervisor
# Directory should already exist with Ubuntu's supervisor package
#COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose necessary ports
EXPOSE 8000

# Start supervisor as the main process
CMD ["gunicorn", "--bind", "0.0.0.0:8000","wsgi:application"]
