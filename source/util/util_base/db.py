import aiomysql

from source.config import HOST, PORT, USER, PASSWORD, DB_NAME, MINSIZE, MAXSIZE


async def create_db_pool(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB_NAME, minsize=MINSIZE, maxsize=MAXSIZE):
    pool = await aiomysql.create_pool(host=host, port=port, user=user, password=password, db=db, minsize=minsize, maxsize=maxsize, charset='utf8')

    return pool


async def get_multi_data(db_conn, sql, args=None):
    # r = await get_multi_data(cursor, "SELECT * from s_info where ts_code in %s;", [["603559.SH", "000001.SZ"]])
    try:
        async with db_conn.cursor() as cursor:
            if args is None:
                await cursor.execute(sql)
                result = await cursor.fetchall()
            else:
                await cursor.execute(sql, args)
                result = await cursor.fetchall()
    except Exception as e:
        raise e

    return result


async def get_single_column(db_conn, sql, args=None):
    result = await get_multi_data(db_conn, sql, args)
    result = [i[0] for i in result]
    return result


async def get_single_row(db_conn, sql, args=None):
    try:
        async with db_conn.cursor() as cursor:
            if args is None:
                await cursor.execute(sql)
                result = await cursor.fetchone()
            else:
                await cursor.execute(sql, args)
                result = await cursor.fetchone()
    except Exception as e:
        raise e

    return result


async def get_single_value(db_conn, sql, args=None):
    result = await get_multi_data(db_conn, sql, args)
    result = None if not result else result[0][0]
    return result


async def get_boolean_value(db_conn, sql, args=None):
    result = get_multi_data(db_conn, sql, args)
    if len(result) > 0 and result[0][0] == 1:
        return True
    else:
        return False


async def update_data(db_conn, sql, args=None):
    try:
        async with db_conn.cursor() as cursor:
            if args is None:
                await cursor.execute(sql)
            else:
                await cursor.execute(sql, args)
            await db_conn.commit()
    except Exception as e:
        raise e
