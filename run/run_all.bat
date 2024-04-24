@echo off

start cmd /k call "C:\Programming\University\BDA\BDA-Assignment03\run\start_zookeeper.bat"
timeout /t 10 /nobreak >nul

start cmd /k call "C:\Programming\University\BDA\BDA-Assignment03\run\start_kafka.bat"
timeout /t 10 /nobreak >nul

start cmd /k call "C:\Programming\University\BDA\BDA-Assignment03\run\start_mongod.bat"
timeout /t 10 /nobreak >nul

start cmd /k call "C:\Programming\University\BDA\BDA-Assignment03\run\run_producer.bat"
timeout /t 5 /nobreak >nul

start cmd /k call "C:\Programming\University\BDA\BDA-Assignment03\run\run_apriori.bat"
start cmd /k call "C:\Programming\University\BDA\BDA-Assignment03\run\run_pcy.bat"
start cmd /k call "C:\Programming\University\BDA\BDA-Assignment03\run\run_custom.bat"