@echo off
:: === Set Python exe location ===
set PYTHON_EXE=C:\Users\Admin\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\python.exe

:: === Set your program ===
set SCRIPT=D:\AI_PROGRAMS\Read_V3.py

:loop
echo [%date% %time%] Starting %SCRIPT% ...
"%PYTHON_EXE%" "%SCRIPT%"

echo [%date% %time%] Program crashed or exited with error code %errorlevel%.
goto loop
