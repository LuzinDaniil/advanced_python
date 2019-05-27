from aiohttp import web
import json
import asyncio
import aio_pika


class Singup:
    @classmethod
    async def singup_send(cls, request):
        connection = await aio_pika.connect()
        async with connection:
            channel = await connection.channel()
            await channel.declare_queue('auth_in')
            query = request.rel_url.query
            param_list = ['email', 'password', 'name']
            if any(param in query.keys() for param in param_list):
                await channel.default_exchange.publish(
                    aio_pika.Message(body='Hello World!'.encode(),
                                     message_id=query['name']),
                                        routing_key='auth_in',

                                      )
                await cls.singup_response(query['name'], message_id=12)
                response = json.dumps({'status': 'Ok',
                                       'data': {}})
            else:
                response = json.dumps({'status': 'PreconditionFailed',
                                        'data': {}})
                raise web.HTTPPreconditionFailed(text=response)
            return web.Response(text=response)

    @classmethod
    async def singup_response(cls, n, message_id):
        connection = await aio_pika.connect()
        queue_name = "auth_out"

        async with connection:
            # Creating channel
            channel = await connection.channel()

            # Declaring queue
            queue = await channel.declare_queue(
                queue_name
            )
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    print(message.message_id, n)
                    if int(message.message_id) == message_id:
                        async with message.process():
                            print(message.body)
                        break
            print('111')


async def login(request):
    pass


async def search(request):
    pass


async def create_app():
    connection = await aio_pika.connect()
    channel = await connection.channel()
    await channel.declare_queue('auth_in')
    await channel.declare_queue('auth_out')
    await channel.declare_queue('crawler_in')
    await channel.declare_queue('crawler_out')
    await channel.declare_queue('db_in')
    await channel.declare_queue('db_out')

    app = web.Application()

    app.add_routes([web.get('/singup', Singup.singup_send),
                    web.post('/login', login),
                    web.get('/search', search)
                    ])
    return app


# channel = asyncio.run(start_pika())


web.run_app(create_app())


