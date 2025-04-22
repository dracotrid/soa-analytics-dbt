# Use an official Python runtime as a parent image
FROM python:3.11.5-slim

# Set environment varibles
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install necessary utilities
RUN apt-get update && apt-get install -y curl apt-transport-https build-essential ca-certificates gnupg

# Add the Cloud SDK distribution URI as a package source
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud Platform public key
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Update and install the Google Cloud CLI
RUN apt-get update && apt-get install -y google-cloud-cli

# Install git
RUN apt-get update && apt-get install -y git

# Copy the requirements file into the container
COPY requirements.txt .

# Upgrade PIP
RUN pip install --upgrade pip && pip install --upgrade setuptools

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
