FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY . .
# The command is chosen per service in docker-compose.yml
CMD ["node", "sync/service-a.js"]
