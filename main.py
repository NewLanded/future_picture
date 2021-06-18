import logging
import os
from logging import handlers

from fastapi import FastAPI, Request, Depends
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from source.config import LOG_LOCATION, STATIC_DIR
from source.dependencies import get_current_active_user
from source.manual_data import manual_data
from source.point import point
from source.summarize import summarize
from source.symbol import symbol
from source.users import users
from source.note import note
from source.util.util_base.db import get_multi_data, create_db_pool

# log_filename = os.path.join(LOG_LOCATION, "future_picture.log")
# logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
#                     handlers=[handlers.RotatingFileHandler(log_filename, encoding='utf-8', maxBytes=1073741824, backupCount=20)],
#                     level=logging.INFO,
#                     datefmt="%Y-%m-%d %H:%M:%S")
# base_logger = logging.getLogger()
# logger.handlers = base_logger.handlers
# logger.setLevel(base_logger.level)
# logger.propagate = False


logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    handlers=[handlers.RotatingFileHandler(os.path.join(LOG_LOCATION, "app.log"), encoding='utf-8', maxBytes=1073741824, backupCount=20)],
                    level=logging.INFO,
                    datefmt="%Y-%m-%d %H:%M:%S")

log_filename = os.path.join(LOG_LOCATION, "future_picture.log")
handler = handlers.RotatingFileHandler(log_filename, encoding='utf-8', maxBytes=1073741824, backupCount=5)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s:%(message)s", datefmt="%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)

logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/front", StaticFiles(directory=STATIC_DIR), name="front")

app.include_router(users)
# app.include_router(symbol, dependencies=[Depends(get_current_active_user)])
# app.include_router(point, dependencies=[Depends(get_current_active_user)])
# app.include_router(summarize, dependencies=[Depends(get_current_active_user)])
# app.include_router(manual_data, dependencies=[Depends(get_current_active_user)])
# app.include_router(note, dependencies=[Depends(get_current_active_user)])

app.include_router(symbol)
app.include_router(point)
app.include_router(summarize)
app.include_router(manual_data)
app.include_router(note)

db_pool = None


@app.exception_handler(ValueError)
async def http_exception_handler(request, exc):
    return JSONResponse(str(exc), status_code=500)


@app.exception_handler(TypeError)
async def http_exception_handler(request, exc):
    return JSONResponse(str(exc), status_code=500)


@app.exception_handler(IndexError)
async def http_exception_handler(request, exc):
    return JSONResponse(str(exc), status_code=500)


@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    async with db_pool.acquire() as db_conn:
        # async with db_conn.cursor() as cursor:
        request.state.db_conn = db_conn
        response = await call_next(request)

    return response


@app.on_event("startup")
async def startup():
    # TODO 没找到地方放 db_pool, 暂时使用 global
    global db_pool
    db_pool = await create_db_pool()


# TODO 貌似 shutdown 这个 方法就没起作用, 暂时不知原因
@app.on_event("shutdown")
async def shutdown():
    db_pool.close()
    await db_pool.wait_closed()


@app.get("/test")
async def read_notes(request: Request):
    db_conn = request.state.db_conn
    r = await get_multi_data(db_conn, "SELECT * from s_info where ts_code in %s;", [["603559.SH", "000001.SZ"]])

    logger.info("sssssssssssss")
    return r


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8885)
