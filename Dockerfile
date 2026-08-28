# --- stage 1: build the dashboard -------------------------------------------
FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# --- stage 2: build the Go binary (frontend embedded) ----------------------
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Use the freshly built assets rather than whatever is committed.
COPY --from=web /web/dist ./web/dist
RUN CGO_ENABLED=0 go build -trimpath -o /out/app .

# --- stage 3: minimal runtime --------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
EXPOSE 8080
ENTRYPOINT ["/app"]
CMD ["-addr", ":8080"]
