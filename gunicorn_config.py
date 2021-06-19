bind = "0.0.0.0:6677"
workers = 1
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 3600
keepalive = 3600

backlog = 100  # 服务器中在pending状态的最大连接数，即client处于waiting的数目。超过这个数目， client连接会得到一个error
daemon = True
debug = False
proc_name = 'gunicorn_future_picture'
pidfile = './log/gunicorn_future_picture.pid'
errorlog = './log/gunicorn_future_picture_error.log'
accesslog = './log/gunicorn_future_picture_access.log'
