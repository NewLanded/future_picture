# future_picture
展示期货价格走势

pip install:
uvicorn
gunicorn
fastapi[all]
postgres相关


nohup /home/stock/anaconda3/envs/stock/bin/python /home/stock/app/security_data_store/timed_task.py > /dev/null 2>&1 &

cd /home/stock/app/future_picture
gunicorn -c gunicorn.conf main:app
