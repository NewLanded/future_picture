# future_picture
展示期货价格走势

pip install:
pip install fastapi[all]
pip install uvicorn[standard]   # --ignore-installed PyYAML
pip install gunicorn
pip install aiohttp
pip install aiomysql
pip install python-jose[cryptography]
pip install passlib[bcrypt]
pip install psycopg2-binary
pip install asyncpg


cd /home/stock/app/future_picture
mkdir log
gunicorn -c gunicorn_config.py main:app

/home/stock/anaconda3/envs/stock/bin/gunicorn --chdir /home/stock/app/future_picture -c /home/stock/app/future_picture/gunicorn_config.py  main:app
