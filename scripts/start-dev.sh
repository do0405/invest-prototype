#!/bin/bash

echo "Starting development environment..."

# 환경변수 설정
export BACKEND_PORT=5000
export FRONTEND_PORT=3000
export NODE_ENV=development
export FLASK_ENV=development

# 스크립트 디렉토리 기준으로 프로젝트 루트 찾기
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 백엔드 시작 (백그라운드)
echo "Starting backend server on port $BACKEND_PORT..."
cd "$PROJECT_ROOT/backend"
python api_server.py &
BACKEND_PID=$!

# 잠시 대기 (백엔드가 시작될 시간)
sleep 3

# 프론트엔드 시작 (백그라운드)
echo "Starting frontend server on port $FRONTEND_PORT..."
cd "$PROJECT_ROOT/frontend"
npm run dev &
FRONTEND_PID=$!

echo "Development servers are running:"
echo "Backend: http://localhost:$BACKEND_PORT"
echo "Frontend: http://localhost:$FRONTEND_PORT"
echo ""
echo "Press Ctrl+C to stop all servers"

# Ctrl+C 처리
trap 'echo "\nStopping servers..."; kill $BACKEND_PID $FRONTEND_PID; exit' INT

# 서버들이 실행 중인 동안 대기
wait