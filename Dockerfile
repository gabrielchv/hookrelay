# syntax=docker/dockerfile:1
FROM node:24-alpine AS base
RUN npm install -g pnpm@10.15.0
WORKDIR /app
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

# Full source + dev deps: used for the build and for one-shot provisioning.
FROM base AS tools
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
COPY scripts ./scripts

FROM tools AS build
RUN pnpm build && pnpm prune --prod

FROM node:24-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
USER node
EXPOSE 3000
CMD ["node", "dist/main.js", "api"]
