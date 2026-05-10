@echo off
echo ========================================
echo  UP Schemes Analysis Pipeline
echo ========================================
echo.

REM Try common Python paths
where py >nul 2>&1 && (
    echo Found: py launcher
    py -m pip install -r requirements.txt -q
    py pipeline.py
    goto :done
)

where python >nul 2>&1 && (
    echo Found: python
    python -m pip install -r requirements.txt -q
    python pipeline.py
    goto :done
)

REM Check common install locations
if exist "C:\Python312\python.exe" (
    echo Found: C:\Python312\python.exe
    C:\Python312\python.exe -m pip install -r requirements.txt -q
    C:\Python312\python.exe pipeline.py
    goto :done
)

if exist "C:\Python311\python.exe" (
    echo Found: C:\Python311\python.exe
    C:\Python311\python.exe -m pip install -r requirements.txt -q
    C:\Python311\python.exe pipeline.py
    goto :done
)

if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" (
    echo Found: AppData Python 3.12
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" -m pip install -r requirements.txt -q
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" pipeline.py
    goto :done
)

if exist "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" (
    echo Found: AppData Python 3.11
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" -m pip install -r requirements.txt -q
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" pipeline.py
    goto :done
)

echo.
echo ERROR: Python not found! Please install Python from python.org
echo Make sure to check "Add Python to PATH" during installation.
echo.

:done
echo.
pause
