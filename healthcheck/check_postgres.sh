#!/bin/bash
docker exec postgres pg_isready -U iceberg -d iceberg_catalog