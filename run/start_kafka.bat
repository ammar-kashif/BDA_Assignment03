@echo off
echo Starting Kafka Server...
cd "C:\kafka"
.\bin\windows\kafka-server-start.bat .\config\server.properties
pause