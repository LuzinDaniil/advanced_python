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
from jinja2 import Template
from abc import abstractmethod


vk_session = vk_api.VkApi(token='')
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
    html_img = html_template.render(name_qr=html_img_cr)
    fp = BytesIO(imgkit.from_string(html_img, False, options=options))
    result = ('11637190', fp)
    return result

def listen():
    with Pool(8) as p:
        for _ in range(1000):
            p.apply_async(generate, callback = put_queue)
        p.close()
        p.join()


class ApiCall:
    @abstractmethod
    def execute(self):
        pass


class MessageApiCall(ApiCall):
    @classmethod
    def execute(cls):
        for _ in range(1000):
            user_id = 11637190
            send_dict = {
            'random_id': random.uniform(1, 1_000_000_000),
            'user_id': user_id,
            'message': 'Для генерации, нажмите кнопку',
            'keyboard': open('keyboard.json', "r", encoding="UTF-8").read()
            }
            send(send_dict)
            time.sleep(1)


class UploadPhotoApiCall(ApiCall):
    @classmethod
    def execute(cls):
        while True:
            vk = vk_session.get_api()
            user_id, name = q.get()
            data = {'photo': ('name.png', name, 'image/png')}
            req = requests.post(server_addr, files=data).json()
            c = vk.photos.saveMessagesPhoto(photo=req['photo'], server=req['server'], hash=req['hash'])[0]
            photo_attachment = 'photo{}_{}_{}'.format(c['owner_id'], c['id'], c['access_key'])
            send_dict = {
            'random_id': random.uniform(1, 1_000_000_000),
            'user_id': user_id,
            'attachment': photo_attachment
            }
            send(send_dict)
            time.sleep(1)


def send(send_dict):
    vk.messages.send(  # Отправляем сообщение
        **send_dict
    )


if __name__ == '__main__':
    longpoll = VkLongPoll(vk_session)
    vk = vk_session.get_api()
    f = open('index.html', 'r')
    html_template = Template(f.read())
    f.close()
    q = Queue()
    q2 = Queue()
    execute1 = Thread(target=MessageApiCall.execute)
    execute2 = Thread(target=UploadPhotoApiCall.execute)
    execute1.start()
    execute2.start()
    listen()
