FROM node:22-slim

# Install ffmpeg, python3, and curl (needed for audio conversion and yt-dlp)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    python3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install yt-dlp
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp

WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install --omit=dev

# Copy source code
COPY . .

# Create cache and uploads dirs
RUN mkdir -p cache uploads

# Expose port (Railway sets PORT env var automatically)
EXPOSE 3000

CMD ["node", "server.js"]
