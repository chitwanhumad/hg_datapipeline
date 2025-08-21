@echo off
D:
cd \
cd HGInsights\Git\hg_datapipeline\
call hg_venv\Scripts\activate
call prefect worker start --pool "chitwan-work-pool"