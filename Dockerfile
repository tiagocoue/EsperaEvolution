# --- base leve e estável ---
FROM node:20-slim

# Evita prompts e melhora logs
ENV NODE_ENV=production \
    TZ=America/Sao_Paulo

# Cria diretório
WORKDIR /app

# Instala dependências do sistema (opcional, só se precisar de git)
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
 && rm -rf /var/lib/apt/lists/*

# Copia manifestos primeiro (melhor cache)
COPY package.json package-lock.json ./

# Instala dependências (mantemos dev deps porque rodamos TS com tsx)
RUN npm ci

# Copia código
COPY . .

# Se quiser ver só os scripts disponíveis
# RUN npm run

# Início do worker
CMD ["npm", "start"]
