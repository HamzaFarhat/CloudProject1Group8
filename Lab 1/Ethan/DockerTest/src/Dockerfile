# Use the OpenJDK 16
FROM openjdk:16

# Create a new app directory for my application files
RUN mkdir /app

# Copy the app files from host machine to image file system
COPY / /app

# Set the directory for executing future commands
WORKDIR /app

# Run the Main class
CMD java App