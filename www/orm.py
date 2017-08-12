# -*- coding: utf-8 -*-

__author__ = 'LWhite'

import asyncio, logging

import aiomysql

def log(sql,args=()):
    logging.info('SQL:%s' % sql)

#创建连接池,每个HTTP请求都可以从连接池中直接获取数据库连接,连接池由全局变量__pool存储，缺省情况下将编码设置为utf8，自动提交事务
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

#对Select定义一个函数select()
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs =await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
            logging.info('rows returned:%s' % len(rs))
            return rs

#对Insert、Update、Delete定义一个通用的execute()函数，因为这三种SQL的执行都需要相同参数，以及返回一个证书表示影响的行数    
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount         #定义（并返回）影响的行数
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

