@echo off
echo Starting Zookeeper...
cd "C:\kafka"
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
pause