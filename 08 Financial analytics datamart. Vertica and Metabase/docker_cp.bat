@echo off
echo Cleaning old files in container...

docker exec c0f99ea7f324 rm -rf /lessons/py
docker exec c0f99ea7f324 rm -rf /lessons/dags
docker exec c0f99ea7f324 rm -rf /lessons/sql

echo Copying files to container...

docker cp "C:\WorkSpace\Learning\Data-Engineer\Final_project\de-project-final\src\py" c0f99ea7f324:/lessons
if %errorlevel% equ 0 (echo py - OK) else (echo py - ERROR)

docker cp "C:\WorkSpace\Learning\Data-Engineer\Final_project\de-project-final\src\dags" c0f99ea7f324:/lessons
if %errorlevel% equ 0 (echo dags - OK) else (echo dags - ERROR)

docker cp "C:\WorkSpace\Learning\Data-Engineer\Final_project\de-project-final\src\sql" c0f99ea7f324:/lessons
if %errorlevel% equ 0 (echo sql - OK) else (echo sql - ERROR)

echo Done. Press any key to exit...
pause >nul