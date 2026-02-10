@echo off
echo Creating Python virtual environment...
py -m venv venv
if %errorlevel% neq 0 (
    echo Error: 'py' command failed. Please ensure Python is installed and added to PATH.
    pause
    exit /b %errorlevel%
)

echo Activating virtual environment...
call venv\Scripts\activate

echo Installing Python dependencies...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo Error: Failed to install Python dependencies.
    pause
    exit /b %errorlevel%
)

echo Installing Node.js dependencies...
cd frontend
npm install
if %errorlevel% neq 0 (
    echo Error: Failed to install Node.js dependencies. Please ensure Node.js is installed.
    pause
    exit /b %errorlevel%
)
cd ..

echo ========================================
echo Environment setup complete!
echo To start development:
echo 1. Backend: venv\Scripts\activate
echo 2. Frontend: cd frontend ^&^& npm run dev
echo ========================================
pause
