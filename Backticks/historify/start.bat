@echo off
echo Starting Historify - Stock Historical Data Management App...

rem Set environment variables
set FLASK_APP=run.py
set FLASK_ENV=development
set FLASK_DEBUG=1

rem Check if virtual environment exists, if not create one
if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
    call venv\Scripts\activate
    echo Installing dependencies...
    pip install -r requirements.txt
) else (
    call venv\Scripts\activate
)

rem Run the Flask application
echo Starting Flask server at http://localhost:5001
python run.py

pause
