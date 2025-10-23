# Use the base image with Node.js
FROM node:21.6.0

# Copy the current directory into the Docker image
COPY . /member-api-v6

# Set working directory for future use
WORKDIR /member-api-v6

# Install the dependencies from package.json
RUN yarn

CMD ["sh", "-c", "npx prisma migrate deploy && node app.js"]
