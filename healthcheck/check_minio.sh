#!/bin/bash
curl -sf http://localhost:9000/minio/health/live && echo "MinIO OK" || echo "MinIO FAILED"