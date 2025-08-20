@echo off
D:
cd \
cd \HGInsights\Git\hg_datapipeline\
call hg_venv\Scripts\activate.bat
call prefect server start
