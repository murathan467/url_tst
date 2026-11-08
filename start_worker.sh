#!/bin/bash
# Eski kilit dosyalarını temizle (Çökme sorununu engellemek için)
rm -f /tmp/.X*-lock
# Xvfb sanal ekranını başlat
export DISPLAY=:99
Xvfb :99 -screen 0 1920x1080x24 &

# VNC sunucusunu başlat
x11vnc -display :99 -nopw -listen 0.0.0.0 -xkb -ncache 10 -ncache_cr -forever &

# Hafif bir pencere yöneticisi başlat (Playwright pencereli modda çalışırken sorunu önlemek için)
fluxbox &

# Biraz bekle ve Python uygulamasını başlat
sleep 2
python main_worker.py
