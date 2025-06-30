@echo off
echo Starting production deployment with Docker Compose...

REM 프로젝트 루트로 이동
cd /d %~dp0..

REM .env 파일 존재 확인
if not exist .env (
    echo Error: .env file not found!
    echo Please copy .env.example to .env and configure your settings.
    pause
    exit /b 1
)

REM Docker Compose로 빌드 및 시작
echo Building and starting containers...
docker-compose up --build -d

if %errorlevel% equ 0 (
    echo.
    echo Deployment successful!
    echo.
    echo Services are running:
    for /f "tokens=*" %%i in ('docker-compose ps --services') do (
        echo - %%i
    )
    echo.
    echo To view logs: docker-compose logs -f
    echo To stop: docker-compose down
) else (
    echo.
    echo Deployment failed! Check the logs above.
)

pause