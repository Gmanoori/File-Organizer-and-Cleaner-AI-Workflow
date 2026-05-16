@echo off
REM MongoDB Bulk Import Batch Script for Windows
REM This script imports all cleaned CSV files to MongoDB using mongoimport

echo MongoDB Bulk Import Script
echo ==========================

REM Configuration - Edit these variables as needed
set MONGODB_URI=mongodb://localhost:27017
set DATABASE=cleaned_data
set CLEANED_DIR=Cleaned\
set DROP_COLLECTIONS=false

echo Configuration:
echo   MongoDB URI: %MONGODB_URI%
echo   Database: %DATABASE%
echo   Source Directory: %CLEANED_DIR%
echo   Drop Existing Collections: %DROP_COLLECTIONS%
echo.

REM Check if mongoimport is available
mongoimport --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: mongoimport not found. Please ensure MongoDB is installed and in PATH.
    pause
    exit /b 1
)

REM Count CSV files
for /f %%c in ('dir /b "%CLEANED_DIR%*.csv" 2^>nul ^| find /c ".csv"') do set FILE_COUNT=%%c

if %FILE_COUNT% equ 0 (
    echo ERROR: No CSV files found in %CLEANED_DIR%
    pause
    exit /b 1
)

echo Found %FILE_COUNT% CSV files to import.
echo.

set SUCCESSFUL=0
set FAILED=0

REM Import each CSV file
for %%f in ("%CLEANED_DIR%*.csv") do (
    REM Get filename without extension for collection name
    set "FILENAME=%%~nf"

    REM Sanitize collection name (remove special characters, limit length)
    set "COLLECTION=!FILENAME!"
    set "COLLECTION=!COLLECTION: =_!"
    set "COLLECTION=!COLLECTION:(=_!"
    set "COLLECTION=!COLLECTION:)=_!"
    set "COLLECTION=!COLLECTION:-=_!"
    set "COLLECTION=!COLLECTION:&=_!"
    set "COLLECTION=!COLLECTION:__=_!"

    REM Remove leading/trailing underscores and convert to lowercase
    for /f "tokens=* delims=" %%i in ("!COLLECTION!") do set "COLLECTION=%%i"
    set "COLLECTION=!COLLECTION:~0,-1!" 2>nul
    for /l %%a in (1,1,100) do if "!COLLECTION:~-1!"=="_" set "COLLECTION=!COLLECTION:~0,-1!"

    REM Ensure collection name doesn't start with number
    set "FIRST_CHAR=!COLLECTION:~0,1!"
    echo !FIRST_CHAR! | findstr /r "[0-9]" >nul
    if !errorlevel! equ 0 (
        set "COLLECTION=col_!COLLECTION!"
    )

    REM Limit length
    set "COLLECTION=!COLLECTION:~0,120!"

    echo Importing %%~nxf -^> !COLLECTION!

    REM Build mongoimport command
    set "CMD=mongoimport --uri "%MONGODB_URI%" --db "%DATABASE%" --collection "!COLLECTION!" --type csv --file "%%f" --headerline"

    if "%DROP_COLLECTIONS%"=="true" (
        set "CMD=!CMD! --drop"
    )

    REM Execute command
    !CMD! >nul 2>&1
    if !errorlevel! equ 0 (
        echo   SUCCESS
        set /a SUCCESSFUL+=1
    ) else (
        echo   FAILED
        set /a FAILED+=1
    )
)

echo.
echo Import Summary:
echo   Successful: %SUCCESSFUL%
echo   Failed: %FAILED%
echo   Total: %FILE_COUNT%

if %FAILED% gtr 0 (
    echo.
    echo Some imports failed. Check MongoDB logs for details.
)

echo.
echo Press any key to continue...
pause >nul