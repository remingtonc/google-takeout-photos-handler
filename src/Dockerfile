FROM docker.io/golang:1.24.4-bookworm
VOLUME /input
VOLUME /output
RUN apt-get update && apt-get install -y exiftool
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN go build -o /app/main .
ENTRYPOINT ["/app/main"]
CMD ["-h"]