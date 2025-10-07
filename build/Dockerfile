# Build stage
FROM golang:1.24-alpine AS builder

# Install git and build dependencies
RUN apk add --no-cache git gcc musl-dev sqlite-dev

# Set CGO flags for SQLite compatibility
ENV CGO_CFLAGS="-D_LARGEFILE64_SOURCE"

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o build/poglytics-scraper ./cmd/poglytics-scraper/

# Runtime stage
FROM alpine:latest

# Install sqlite
RUN apk --no-cache add ca-certificates sqlite

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/build/poglytics-scraper .

# Create directory for database
RUN mkdir -p /root/data

# Expose port if needed (not required for this app)
# EXPOSE 8080

# Run the application
CMD ["./poglytics-scraper"]
