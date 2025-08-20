@echo off
D:
cd \superset
call venv\Scripts\activate.bat
set SUPERSET_CONFIG_PATH=D:\superset\superset_config.py
set FLASK_APP=superset.app:create_app()
call superset init
call superset run -h localhost -p 8088