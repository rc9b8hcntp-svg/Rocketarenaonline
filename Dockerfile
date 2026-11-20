FROM node:20-alpine
WORKDIR /app
COPY --chown=node:node . .
ENV PORT=7860 DATA_DIR=/app/data
RUN mkdir -p /app/data && chown node:node /app /app/data
VOLUME /app/data
EXPOSE 7860
USER node
CMD ["node", "server.js"]
