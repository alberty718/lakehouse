#!/bin/bash
curl -sf http://localhost:8080/v1/info && echo -e "\nTrino OK" || echo "Trino FAILED"