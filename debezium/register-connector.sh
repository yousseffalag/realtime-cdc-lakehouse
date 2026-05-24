#!/bin/bash
curl -i -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors/ \
  -d @connectors/postgres-connector.json