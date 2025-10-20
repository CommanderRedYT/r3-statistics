FROM node:24-alpine AS base

FROM base AS dependencies

WORKDIR /app

# Install dependencies based on the preferred package manager
COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile

FROM dependencies AS builder

WORKDIR /app

COPY . .

FROM builder AS runner

WORKDIR /app

ENV NODE_ENV=production

CMD ["yarn", "start"]
