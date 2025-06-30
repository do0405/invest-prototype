@echo off
echo Starting development environment...

REM 환경변수 설정
set BACKEND_PORT=5000
set FRONTEND_PORT=3000
set NODE_ENV=development
set FLASK_ENV=development

REM 백엔드 시작 (새 창에서)
start "Backend Server" cmd /k "cd /d %~dp0..\backend && python api_server.py"

REM 잠시 대기 (백엔드가 시작될 시간)
timeout /t 3 /nobreak > nul

REM 프론트엔드 시작 (새 창에서)
start "Frontend Server" cmd /k "cd /d %~dp0..\frontend && npm run dev"

echo Development servers are starting...
echo Backend: http://localhost:%BACKEND_PORT%
echo Frontend: http://localhost:%FRONTEND_PORT%
pause