#!/usr/bin/env bash
#
# Build + run DuckImport inside the neo4j:5 container.
# No local JDK required — the neo4j:5 image ships a full JDK 21 and all Neo4j jars.
#
# Usage:
#   ./run.sh [numPersons] [numKnows] [heap]
# Examples:
#   ./run.sh                       # 1000 / 1000, -Xmx4g   (smoke test)
#   ./run.sh 5000000 10000000      # 5M nodes / 10M rels
#   ./run.sh 50000000 100000000 8g # 50M nodes / 100M rels, -Xmx8g
#
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${NEO4J_IMAGE:-neo4j:5}"
DUCKDB_VER="${DUCKDB_VER:-1.4.4.0}"

N_PERSONS="${1:-1000}"
N_KNOWS="${2:-1000}"
HEAP="${3:-4g}"

# 1) Fetch the DuckDB JDBC driver once (bundles native libs for linux arm64/amd64).
if [ ! -f "$DIR/duckdb_jdbc.jar" ]; then
  echo ">> downloading DuckDB JDBC $DUCKDB_VER ..."
  curl -fsSL -o "$DIR/duckdb_jdbc.jar" \
    "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/${DUCKDB_VER}/duckdb_jdbc-${DUCKDB_VER}.jar"
fi

# 2) Compile + run inside the container (bind-mounts this dir at /work).
docker run --rm -v "$DIR":/work --entrypoint bash "$IMAGE" -c '
  set -e
  CP="/var/lib/neo4j/lib/*:/work/duckdb_jdbc.jar"
  mkdir -p /work/out
  echo ">> javac"
  javac -cp "$CP" -d /work/out /work/DuckImport.java
  echo ">> run '"$N_PERSONS"' / '"$N_KNOWS"' (heap -Xmx'"$HEAP"')"
  java \
    --add-opens=java.base/java.io=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    -Xmx'"$HEAP"' -cp "/work/out:$CP" DuckImport '"$N_PERSONS"' '"$N_KNOWS"'
'
