@echo off
setlocal enabledelayedexpansion

echo ===================================================
echo    투자 전략 스크리너 및 포트폴리오 관리 시스템
echo ===================================================
echo.

:: 현재 디렉토리 확인
cd /d %~dp0
echo 현재 디렉토리: %CD%
echo.

:MENU
echo 실행할 작업을 선택하세요:
echo 1. 모든 투자 전략 실행 (전략 1 ~ 6)
echo 2. 종료
echo.

set /p choice=선택 (1-2): 

if "%choice%"=="1" goto RUN_ALL_STRATEGIES
if "%choice%"=="2" goto EXIT

echo.
echo 잘못된 선택입니다. 다시 시도하세요.
echo.
goto MENU

:RUN_ALL_STRATEGIES
echo.
echo ===================================================
echo    모든 투자 전략 실행 중...
echo ===================================================
echo.

:: 현재 디렉토리로 이동
cd /d %~dp0
echo 현재 디렉토리: %CD%
echo.

:: Python 스크립트 실행 (run_screener.py가 모든 전략을 실행)
py run_screener.py

echo.
echo 모든 투자 전략 실행이 완료되었습니다.
echo 결과 파일은 ..\results\results_ver2 폴더에서 확인할 수 있습니다.
echo.

pause
goto MENU

:EXIT
echo.
echo 프로그램을 종료합니다.
echo.
endlocal
exit /b 0