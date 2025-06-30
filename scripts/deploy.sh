#!/bin/bash

echo "Starting production deployment with Docker Compose..."

# 스크립트 디렉토리 기준으로 프로젝트 루트 찾기
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 프로젝트 루트로 이동
cd "$PROJECT_ROOT"

# .env 파일 존재 확인
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy .env.example to .env and configure your settings."
    exit 1
fi

# Docker와 Docker Compose 설치 확인
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed!"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker Compose is not installed!"
    exit 1
fi

# Docker Compose로 빌드 및 시작
echo "Building and starting containers..."
docker-compose up --build -d

if [ $? -eq 0 ]; then
    echo ""
    echo "Deployment successful!"
    echo ""
    echo "Services are running:"
    docker-compose ps --services | while read service; do
        echo "- $service"
    done
    echo ""
    echo "To view logs: docker-compose logs -f"
    echo "To stop: docker-compose down"
else
    echo ""
    echo "Deployment failed! Check the logs above."
    exit 1
fi