import asyncpg

from source.config import HOST, PORT, USER, PASSWORD, DB_NAME, MINSIZE, MAXSIZE


async def create_db_pool(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB_NAME, minsize=MINSIZE, maxsize=MAXSIZE):
    pool = await asyncpg.create_pool(host=host, port=port, user=user, password=password, database=db, min_size=minsize, max_size=maxsize)
    # "postgres://stock:123@10.97.88.29:5432/stock",
    return pool


async def get_multi_data(db_conn, sql, args=None):
    try:
        if args is None:
            result = await db_conn.fetch(sql)
        else:
            result = await db_conn.fetch(sql, *args)
    except Exception as e:
        raise e
    result = [list(i) for i in result]
    return result


async def get_single_column(db_conn, sql, args=None):
    result = await get_multi_data(db_conn, sql, args)
    result = [i[0] for i in result]
    return result


async def get_single_row(db_conn, sql, args=None):
    try:
        if args is None:
            result = await db_conn.fetchrow(sql)
        else:
            result = await db_conn.fetchrow(sql, *args)
    except Exception as e:
        raise e
    result = list(result)
    return result


async def get_single_value(db_conn, sql, args=None):
    result = await get_multi_data(db_conn, sql, args)
    result = None if not result else result[0][0]
    return result


async def get_boolean_value(db_conn, sql, args=None):
    result = get_single_value(db_conn, sql, args)
    if result:
        return True
    else:
        return False


async def update_data(db_conn, sql, args=None):
    try:
        async with db_conn.transaction():
            if args is None:
                await db_conn.execute(sql)
            else:
                await db_conn.execute(sql, *args)
    except Exception as e:
        raise e
