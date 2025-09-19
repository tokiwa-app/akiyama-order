# 安定・高速起動用
FROM node:20-slim

# セキュリティ（非root）
USER node

# 作業ディレクトリ
WORKDIR /app

# 依存インストール（キャッシュ効率）
COPY --chown=node:node package*.json ./
RUN npm ci --omit=dev

# アプリ本体
COPY --chown=node:node . .

# 本番モード
ENV NODE_ENV=production

# Cloud Run は PORT を環境変数で注入する（EXPOSE不要）
CMD ["node", "index.js"]
