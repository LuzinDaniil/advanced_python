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
import os


vk_session = vk_api.VkApi(token=os.environ['TOKEN_VAR'])
server_addr = vk_session.method('photos.getMessagesUploadServer')['upload_url']
vk = vk_session.get_api()

options = {
    'format': 'png',
    'crop-h': '650',
    'crop-w': '600',
    'crop-x': '0',
    'crop-y': '0'
}

def put_queue(result):
    q.put(result)

def generate(user_id):
    data_qr = 'http://getcolor.ru/#{}'.format(str(random.randint(0, 0xFFFFF)).ljust(6, '0'))
    img = pyqrcode.create(data_qr)
    # для HTML
    image_as_str = img.png_as_base64_str(scale=10)
    html_img = html_template.render(name_qr=image_as_str)
    fp = BytesIO(imgkit.from_string(html_img, False, options=options))
    result = (user_id, fp)
    return result


def listen():
    with Pool(8) as p:
        for event in longpoll.listen():
            if event.type == VkEventType.MESSAGE_NEW and event.to_me and event.text:
                #Слушаем longpoll, если пришло сообщение то:
                if event.text == 'Сгенерировать купон': #Если написали заданную фразу
                    if event.from_user: #Если написали в ЛС
                        p.apply_async(generate, args=(event.user_id,), callback=put_queue)
                else:
                    if event.from_user:  # Если написали в ЛС
                        q.put((event.user_id, None))
        p.close()
        p.join()


class ApiCall:
    @classmethod
    def read(cls):
        while True:
            user_id, name = q.get()
            if name is None:
                MessageApiCall.execute(user_id)
            else:
                UploadPhotoApiCall.execute(user_id, name)


    @abstractmethod
    def execute(self):
        pass


class MessageApiCall(ApiCall):
    @classmethod
    def execute(cls, user_id):
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
    def execute(cls, user_id, name):
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

    execute = Thread(target=ApiCall.read)

    execute.start()
    listen()
