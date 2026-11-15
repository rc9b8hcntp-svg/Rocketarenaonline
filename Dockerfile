FROM node:20-alpine
WORKDIR /app
COPY --chown=node:node . .
ENV PORT=7860
EXPOSE 7860
USER node
CMD ["node", "server.js"]
