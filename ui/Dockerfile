# Stage 1: build frontend ui
FROM node:16-alpine as ui-build
WORKDIR /usr/src/ui
COPY ./ui .

## Use api endpoint from same host and build production static bundle
RUN echo 'REACT_APP_API_ENDPOINT=http://127.0.0.1:18000' >> .env.production
RUN echo 'REACT_APP_ENABLE_RBAC=true' >> .env.production

RUN npm install && npm run build

# Stage 2: Setup nginx
FROM ubuntu:22.04
RUN apt-get update -y && apt-get install -y nginx && apt-get clean
RUN rm -rf /usr/share/nginx/html/*
COPY --from=ui-build /usr/src/ui/build /usr/share/nginx/html

# Note: nginx config must be mounted to the path: /etc/nginx/nginx.conf

CMD nginx -g "daemon off;"