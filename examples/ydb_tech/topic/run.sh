#!/usr/bin/env bash
set -euo pipefail
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
mvn -B -f examples/ydb_tech/topic/pom.xml compile exec:java
