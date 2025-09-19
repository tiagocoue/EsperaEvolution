FROM node:20-slim

ENV TZ=America/Sao_Paulo
WORKDIR /app

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates git \
 && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./

# 👇 instala TAMBÉM devDependencies (inclui tsx)
RUN npm ci --production=false

COPY . .

# Agora sim, depois de instalar tudo, marcamos production
ENV NODE_ENV=production

CMD ["npm", "start"]
