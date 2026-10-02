FROM node:24.9.0-bookworm-slim

WORKDIR /app

# Proto file (shared between coordinator and worker)
COPY proto/ /proto/
COPY shared/ /shared/
COPY benchmarks/artifacts/milestone2-report.json /evidence/milestone2-report.json

# Install dependencies — npm ci is reproducible (uses lockfile)
COPY coordinator/package.json coordinator/package-lock.json* ./
RUN npm ci --omit=dev

# Copy coordinator source
COPY coordinator/ .

EXPOSE 3000 50050 9464

CMD ["node", "--max-old-space-size=2048", "-r", "./tracing.js", "src/index.js"]
