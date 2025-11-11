# Use the official Node.js 18 image
FROM node:18-alpine

# Increase Node.js memory limit
ENV NODE_OPTIONS=--max_old_space_size=4096

# Set working directory
WORKDIR /app

# Install pnpm
RUN npm install -g pnpm

# Copy 'package.json' and 'pnpm-lock.yaml' from the service directory
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./

# Copy the 'contract' folder and any other dependencies
COPY apps/contract apps/contract

# Copy the 'contract' folder and any other dependencies
COPY apps/backend apps/backend

# Install dependencies using pnpm
RUN pnpm install

WORKDIR /app/apps/backend

# Build the service
RUN pnpm run build

# Set the command to run your service
CMD ["pnpm", "start"]

# Expose port 3000
EXPOSE 3000
