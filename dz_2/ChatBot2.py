import random
import time
from multiprocessing import Process, Queue, Pool
from vk_api.longpoll import VkLongPoll, VkEventType
import vk_api
import requests
import pyqrcode
from io import BytesIO
import imgkit
from threading import Thread


vk_session = vk_api.VkApi(token='d6a3982c72a63260920f95a2ed14ba2c97231943b8d009fcbdc6c85519ee6551f31')
server_addr = vk_session.method('photos.getMessagesUploadServer')['upload_url']

options = {
    'format': 'png',
    'crop-h': '650',
    'crop-w': '600',
    'crop-x': '0',
    'crop-y': '0'
}

def put_queue(result):
    q.put(result)

def generate():
    data_qr = 'http://getcolor.ru/#{}'.format(str(random.randint(0, 0xFFFFF)).ljust(6, '0'))
    img = pyqrcode.create(data_qr)
    # для HTML
    image_as_str = img.png_as_base64_str(scale=10)
    html_img_cr = '<img src="data:image/png;base64,{}">'.format(image_as_str)
    html_img = html_template.replace('img">', 'img">'+html_img_cr)
    fp = BytesIO(imgkit.from_string(html_img, False, options=options))
    result = ('11637190', fp)
    return result

def listen():
    with Pool(8) as p:
        for _ in range(1000):
            p.apply_async(generate, callback = put_queue)
        p.close()
        p.join()

def send():
    while True:
        vk = vk_session.get_api()
        event, name = q.get()
        data = {'photo': ('name.png', name, 'image/png')}
        req = requests.post(server_addr, files=data).json()
        c = vk.photos.saveMessagesPhoto(photo=req['photo'], server=req['server'], hash=req['hash'])[0]
        photo_attachment = 'photo{}_{}_{}'.format(c['owner_id'], c['id'], c['access_key'])
        vk.messages.send(  # Отправляем сообщение
            random_id=random.uniform(1, 1_000_000_000),
            user_id=217884152,
            attachment=photo_attachment
        )


if __name__ == '__main__':
    longpoll = VkLongPoll(vk_session)
    vk = vk_session.get_api()
    f = open('index.html', 'r')
    html_template = f.read()
    f.close()
    q = Queue()
    process_send = Thread(target=send)
    process_send.start()
    listen()
