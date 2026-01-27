# Use the base image with Node.js
FROM node:22.22.0

# Copy the current directory into the Docker image
COPY . /member-api-v6

# Set working directory for future use
WORKDIR /member-api-v6

# Install the dependencies from package.json
RUN yarn

# Copy and set permissions for entrypoint script
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Use entrypoint script
ENTRYPOINT ["/entrypoint.sh"]