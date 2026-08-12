@echo off
echo Starting VortexAI pipeline...

start "VortexAI Producer" cmd /k "python producer/hackernews_producer.py"
timeout /t 3 /nobreak >nul

start "VortexAI Consumer" cmd /k "python consumer/validated_consumer.py"
timeout /t 3 /nobreak >nul

start "VortexAI Bronze Writer" cmd /k "python storage/bronze_writer.py"

echo.
echo All three pipeline processes launched in separate windows.
echo Close this window or press any key to exit (the other windows will keep running).
pause >nul