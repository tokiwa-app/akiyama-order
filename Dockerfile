FROM node:20-slim

# 🔽 ここを追加：Chromium 用のネイティブライブラリ
RUN apt-get update && apt-get install -y \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libxrender1 \
    libxshmfence1 \
    libxkbcommon0 \
    wget \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 依存関係のみ先にコピーしてインストール
COPY package*.json ./
RUN npm install --omit=dev

# アプリ本体をコピー
COPY . .

ENV NODE_ENV=production

# Cloud Run が PORT を注入します（EXPOSEは不要）
CMD ["node", "index.js"]
