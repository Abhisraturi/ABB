@echo off
:: === Set Python exe location ===
set PYTHON_EXE=C:\Python313\python.exe

:: === Set your program ===
set SCRIPT=D:\AI_PROGRAMS\Read_V2.py

:loop
echo [%date% %time%] Starting %SCRIPT% ...
"%PYTHON_EXE%" "%SCRIPT%"

echo [%date% %time%] Program crashed or exited with error code %errorlevel%.
goto loop
