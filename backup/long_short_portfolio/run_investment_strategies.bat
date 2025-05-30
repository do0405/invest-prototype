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
echo 1. 스크리너 Ver2 실행 (전략 1, 2)
echo 2. 스크리너 Ver4 실행 (전략 4)
echo 3. 포트폴리오 관리 시스템 실행
echo 4. 종료
echo.

set /p choice=선택 (1-4): 

if "%choice%"=="1" goto RUN_SCREENER_VER2
if "%choice%"=="2" goto RUN_SCREENER_VER4
if "%choice%"=="3" goto RUN_PORTFOLIO_MANAGER
if "%choice%"=="4" goto EXIT

echo.
echo 잘못된 선택입니다. 다시 시도하세요.
echo.
goto MENU

:RUN_SCREENER_VER2
echo.
echo ===================================================
echo    스크리너 Ver2 실행 중...
echo ===================================================
echo.

:: 프로젝트 루트 디렉토리로 이동
cd ..
echo 현재 디렉토리: %CD%
echo.

:: 스크리너 실행
py long_short_portfolio\screener_ver2.py

echo.
echo 스크리너 Ver2 실행이 완료되었습니다.
echo 결과 파일은 results\results_ver2 폴더에서 확인할 수 있습니다.
echo.

pause
goto MENU

:RUN_SCREENER_VER4
echo.
echo ===================================================
echo    스크리너 Ver4 실행 중...
echo ===================================================
echo.

:: 현재 디렉토리로 이동
cd /d %~dp0
echo 현재 디렉토리: %CD%
echo.

:: 스크리너 실행
py screener_ver4.py

echo.
echo 스크리너 Ver4 실행이 완료되었습니다.
echo 결과 파일은 results\results_ver2 폴더에서 확인할 수 있습니다.
echo.

pause
goto MENU

:RUN_PORTFOLIO_MANAGER
echo.
echo ===================================================
echo    포트폴리오 관리 시스템 실행 중...
echo ===================================================
echo.

:: 현재 디렉토리로 이동
cd /d %~dp0
echo 현재 디렉토리: %CD%
echo.

:: Python 스크립트 실행
py run_screener.py

echo.
echo 포트폴리오 관리 시스템 실행이 완료되었습니다.
echo.

pause
goto MENU

:EXIT
echo.
echo 프로그램을 종료합니다.
echo.
endlocal
exit /b 0