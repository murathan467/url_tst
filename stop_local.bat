@echo off
echo Yerel Bot (main_local.py) durduruluyor...
powershell -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python' -and $_.CommandLine -match 'main_local.py' } | Invoke-CimMethod -MethodName Terminate"
echo.
echo Yeniden baslatmak icin VBS veya BAT dosyasina cift tiklayabilirsiniz.
pause
