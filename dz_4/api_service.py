from aiohttp import web
import json
import asyncio
import aio_pika
import ctypes
import uuid
from orm import CrawlerStats, User, Token


dict_id = {}


class AuthMS:
    @classmethod
    async def make_request(cls, type, data):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        fut_id = str(uuid.uuid4())
        dict_id[fut_id] = fut

        await send(fut_id, type, data, 'auth_in')
        a = await fut
        return a


class CrawlerMS:
    @classmethod
    async def make_request(cls, type, data):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        fut_id = str(uuid.uuid4())
        dict_id[fut_id] = fut

        await send(fut_id, type, data, 'crawler_in')
        a = await fut
        return a

    @classmethod
    async def make_nowait_request(cls, type, data):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        fut_id = str(uuid.uuid4())
        dict_id[fut_id] = fut

        await send(fut_id, type, data, 'crawler_in')
        a = await fut


async def send(fut_id, type, data, name_queue):
    connection = await aio_pika.connect()
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(name_queue)

        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(data).encode(),
                             message_id=fut_id,
                             type=type),
                                routing_key=name_queue
                              )


async def response():
    connection = await aio_pika.connect()
    queue_name = "out"

    async with connection:
        # Creating channel
        channel = await connection.channel()
        # Declaring queue
        queue = await channel.declare_queue(
            queue_name
        )
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = message.body.decode()
                    dict_id[message.message_id].set_result(body)


async def start_background_tasks(app):
    app.loop.create_task(response())


async def singup(request):
    query = request.rel_url.query
    param_list = ['email', 'password', 'name']
    if all(param in query.keys() for param in param_list):
        try:
            email, password, name = str(query['email']), str(query['password']), str(query['name'])
        except:
            raise web.HTTPPreconditionFailed(text='Неправильные значения параметров')
        res = await AuthMS.make_request('singup', data={'email': email, 'password': password, 'name': name})
    else:
        raise web.HTTPPreconditionFailed(text='Неправльные параметры')
    return web.Response(body=res)


async def login(request):
    query = request.rel_url.query
    param_list = ['email', 'password']
    if all(param in query.keys() for param in param_list):
        try:
            email, password = str(query['email']), str(query['password'])
        except:
            raise web.HTTPPreconditionFailed(text='Неправильные значения параметров')
        res = await AuthMS.make_request('login', data={'email': email, 'password': password})
    else:
        raise web.HTTPPreconditionFailed(text='Неправльные параметры')
    return web.Response(text=res)


async def search(request):
    query = request.rel_url.query
    param_list = ['q', 'limit', 'offset']
    if all(param in query.keys() for param in param_list):
        try:
            q, limit, offset = str(query['q']), int(query['limit']), int(query['offset'])
            if limit > 100:
                raise web.HTTPPreconditionFailed(text='Неправильные значения параметров')
        except:
            raise web.HTTPPreconditionFailed(text='Неправильные значения параметров')
        res = await CrawlerMS.make_request('search', data={'q': q, 'limit': limit, 'offset': offset})
    else:
        raise web.HTTPPreconditionFailed(text='Неправльные параметры')
    return web.Response(text=res)


async def current(request):
    if 'X-Token' in request.headers:
        token = request.headers['X-Token']
        res = await AuthMS.make_request('current', data={'token': token})
    else:
        raise web.HTTPForbidden(text='forbidden')
    return web.Response(text=res)


async def index(request):
    query = request.rel_url.query

    if 'X-Token' in request.headers:
        token = request.headers['X-Token']
        try:
            domain = str(query['domain'])
        except:
            raise web.HTTPPreconditionFailed(text='Неправильные значения параметров')
        res = await CrawlerMS.make_request('index', data={'domain': domain, 'token': token})
        return web.Response(text=res)
    else:
        raise web.HTTPForbidden(text='forbidden')


async def stat(request):
    if 'X-Token' in request.headers:
        token = request.headers['X-Token']
        token = Token.objects.get(token=token)
        user_id = token.user_id
        stats = CrawlerStats.objects.get(author_id=user_id)
        res = {'status': 'Ok', 'data': {'domain': stats.domain, 'author_id': stats.author_id, 'time': stats.time,
                                        'pages_count': stats.pages_count}}
        res = json.dumps(res)
    else:
        raise web.HTTPForbidden(text='forbidden')
    return web.Response(text=res)


def create_app():
    app = web.Application()
    app.on_startup.append(start_background_tasks)
    app.add_routes([web.post('/singup', singup),
                    web.post('/login', login),
                    web.get('/search', search),

                    web.get('/current', current),
                    web.post('/index', index),
                    web.get('/search', stat)
                    ])

    return app


# channel = asyncio.run(start_pika())
web.run_app(create_app())








