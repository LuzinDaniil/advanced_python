import aio_pika
import asyncio
from orm import User, Token
from datetime import datetime, timedelta
import json
import uuid
import pymysql



async def main(loop):
    connection = await aio_pika.connect()
    queue_name = "auth_in"
    queue_name_out = 'out'

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name)
        queue_out = await channel.declare_queue(queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    type = message.type
                    body = json.loads(message.body.decode())
                    if type == 'singup':
                        await singup(message.message_id, body, queue_name_out)
                    elif type =='login':
                        await login(message.message_id, body, queue_name_out)
                    elif type == 'current':
                        await validate(message.message_id, body, queue_name_out)


async def send(fut_id, data, queue_name_out, status):
    connection = await aio_pika.connect()
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(queue_name_out)
        await channel.default_exchange.publish(
            aio_pika.Message(headers={'status': status},
                             body=data.encode(),
                             message_id=fut_id),
                                routing_key=queue_name_out
                              )


async def singup(message_id, body, queue_name_out):
    try:
        date_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            User.objects.create(email=body['email'], password=body['password'], name=body['name'], created_date=date_now,
                                last_login_date=date_now)
        except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                pymysql.IntegrityError, TypeError) as err:
            return await send(message_id, json.dumps({'status': 'error', 'data': err.args[1]}), queue_name_out, 'error')
        user = User.objects.get(email=body['email'])
        token = Token.objects.create(token=str(uuid.uuid4()), user_id=user.id, expire_date=str(datetime.now()+timedelta(days=1)))
        body = {"status": 'Ok', "data": {'token': token.token, 'expire_date': token.expire_date}}
        return await send(message_id, json.dumps(body), queue_name_out, 'ok')
    except:
        user = User.objects.get(email=body['email'])
        return await send(message_id, json.dumps({}), queue_name_out, 'error')


async def login(message_id, body, queue_name_out):
    user = User.objects.get(email=body['email'], password=body['password'])
    if user is None:
        return await send(message_id, json.dumps({'status': 'error', 'data': {'message': 'password or email not correct'}}), queue_name_out, 'error')

    token = Token.objects.get(user_id=user.id)
    if token is None:
        token = Token.objects.create(token=str(uuid.uuid4()), user_id=user.id,
                                     expire_date=str(datetime.now() + timedelta(days=1)))
    else:
        token.expire_date = str(datetime.now() + timedelta(days=1))  # обновление времени истекания токена
        token.save()
    body = {"status": 'Ok', "data": {'token': token.token, 'expire_date': token.expire_date}}
    return await send(message_id, json.dumps(body), queue_name_out, 'Ok')


async def validate(message_id, body, queue_name_out):
    token_name = body['token']
    token = Token.objects.get(token=token_name)
    if token is None:
        body = {"status": 'error', "data": {'message': 'token is not valid'}}
        return await send(message_id, json.dumps(body), queue_name_out, 'error')
    ts = token.expire_date
    f = '%Y-%m-%d %H:%M:%S'
    expire_date = datetime.strptime(ts, f)
    if expire_date > datetime.now():
        user = User.objects.get(id=token.user_id)
        body = {"status": 'Ok', "data": {'id': user.id, 'email': user.email, 'name': user.name,
                                         'created_date': user.created_date, 'last_login_date': user.last_login_date}}
    else:
        body = {"status": 'error', "data": {'message': 'token is expire'}}
    return await send(message_id, json.dumps(body), queue_name_out, 'error')


loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
loop.close()
