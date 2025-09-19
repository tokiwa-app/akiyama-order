FROM node:20-slim

WORKDIR /app

# 依存関係のみ先にコピーしてインストール
COPY package*.json ./
RUN npm install --omit=dev

# アプリ本体をコピー
COPY . .

ENV NODE_ENV=production

# Cloud Run が PORT を注入します（EXPOSEは不要）
CMD ["node", "index.js"]
