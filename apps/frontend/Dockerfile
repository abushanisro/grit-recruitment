# Use the official Node.js 18 image
FROM node:18-alpine

# Set working directory
WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json ./

COPY apps/contract apps/contract

COPY apps/frontend apps/frontend

# Install pnpm
RUN npm install -g pnpm

# Install dependencies using pnpm
RUN pnpm install

WORKDIR /app/apps/frontend

# Build the service
RUN pnpm run prod:build

# Set the command to run your service
CMD ["pnpm", "prod:start"]

# Expose port 3000
EXPOSE 3000