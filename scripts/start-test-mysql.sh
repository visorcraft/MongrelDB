#!/usr/bin/env bash
set -euo pipefail

tls_dir="${1:-target/mysql-tls}"
container="${2:-mongreldb-mysql}"
port="${3:-33060}"
mkdir -p "$tls_dir"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$tls_dir/ca-key.pem" -out "$tls_dir/ca.pem" \
  -subj "/CN=MongrelDB MySQL Test CA"
openssl req -new -newkey rsa:2048 -nodes \
  -keyout "$tls_dir/server-key.pem" -out "$tls_dir/server.csr" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
openssl x509 -req -days 1 \
  -in "$tls_dir/server.csr" \
  -CA "$tls_dir/ca.pem" -CAkey "$tls_dir/ca-key.pem" \
  -CAcreateserial -copy_extensions copy \
  -out "$tls_dir/server.pem"
chmod 0644 "$tls_dir"/*
docker run --detach --name "$container" --publish "$port:3306" \
  --env MYSQL_ROOT_PASSWORD=mongreldb-test \
  --env MYSQL_ROOT_HOST=% \
  --env MYSQL_DATABASE=source \
  --volume "$(realpath "$tls_dir"):/certs:ro" \
  mysql:8.4.10 \
  --server-id=44101 \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --binlog-row-image=FULL \
  --require-secure-transport=ON \
  --ssl-ca=/certs/ca.pem \
  --ssl-cert=/certs/server.pem \
  --ssl-key=/certs/server-key.pem
for _ in {1..90}; do
  if docker exec "$container" \
    mysqladmin ping --silent -uroot -pmongreldb-test; then
    exit 0
  fi
  sleep 1
done
docker logs "$container"
exit 1
