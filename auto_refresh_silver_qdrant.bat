@echo off
echo Starting auto-refresh loop (Silver + Qdrant sync every 2 minutes)...
echo Press Ctrl+C to stop.

:loop
echo.
echo ===== Refreshing at %TIME% =====
python storage/silver_transform.py
python vector_sink/qdrant_sync.py

timeout /t 120 /nobreak
goto loop