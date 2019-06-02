import asyncio
from bs4 import BeautifulSoup
from aioelasticsearch import Elasticsearch
import aiohttp
import urllib.parse
from lxml import html
import time
import asyncpool
import logging
import aio_pika
import json
from orm import CrawlerStats, User, Token
from datetime import datetime, timedelta

# root_url = 'https://docs.python.org'
stop_symbols = ['#', 'zip', 'epub', 'bz2', '.io', '/fr/', '/ja/', '/ko/', '/zh-cn/', 'http', ':']
stop_symbols2 = ['#', 'zip', 'epub', 'bz2', '.io', '/fr/', '/ja/', '/ko/', '/zh-cn/']
file_extensions = ['.zip', 'epub', '.bz2']


class Crawler:
    def __init__(self, max_rps):
        self.max_tasks = max_rps
        self.q_url = asyncio.Queue()
        self.q_text = asyncio.Queue()
        self.q_rps = asyncio.Queue()
        self.seen_urls = set()
        self.q_root_url = asyncio.Queue()
        self.root_url = ''
        self.i = 0
        self.user_id = 0

        self.sem = asyncio.Semaphore(max_rps)

        self.session = None
        self.es = Elasticsearch()

    async def crawl(self):
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(timeout=timeout, loop=loop)

        async with asyncpool.AsyncPool(loop, num_workers=10, name="workers", logger=logging.getLogger("Workers"),
                                       worker_co=self.worker_crawl) as pool:
            await pool.push()
            async with asyncpool.AsyncPool(loop, num_workers=10, name="workers", logger=logging.getLogger("Workers"),
                                           worker_co=self.worker_elastic) as pool2:
                await pool2.push()
                while True:
                    self.root_url, self.user_id = await self.q_root_url.get()
                    await self.q_url.put(self.root_url)
                    work = await asyncio.ensure_future(self.rps_control())
                    await self.q_rps.join()
                    await self.q_url.join()
                    await self.q_text.join()
                    work.cancel()
                    stats = CrawlerStats.objects.get(domain=self.root_url)
                    stats.time = str(datetime.now())
                    stats.save()


    async def worker_crawl(self):
        while True:
            try:
                url = await self.q_url.get()
            except asyncio.CancelledError:
                break
            await self.sem.acquire()
            async with self.sem:
                if url:
                    await self.q_rps.put(time.time())
                await self.download(url)
                self.q_url.task_done()

    async def download(self, url):
        try:
            response = await self.session.get(url)
            response_text = await response.text()
        except:
            return

        if response.status == 404:
            return

        page = BeautifulSoup(response_text, 'lxml')
        links = self.parse_link(response_text, url)
        self.q_text.put_nowait((page, url))

        for link in links.difference(self.seen_urls):
            self.q_url.put_nowait(link)
        self.seen_urls.update(links)
        response.close()

    def parse_link(self, page, url_page):
        links = set()
        parsed_body = html.fromstring(page)
        links_parsed = set(parsed_body.xpath('//a/@href'))
        for link in links_parsed:
            # откидываем сссылки на другие сайты и якори
            # преобразуем относительные в абсолютные

            if self.root_url in link and '#' not in link and any(symbol != link[-4:] for symbol in file_extensions):
                links.add(link)
            elif any(symbol in link for symbol in stop_symbols) or any(symbol in url_page for symbol in stop_symbols2):
                continue
            else:
                links.add(urllib.parse.urljoin(url_page, link))
        return links

    async def worker_elastic(self):
        while True:
            try:
                text_html, url = await self.q_text.get()
            except asyncio.CancelledError:

                break
            await self.add_to_index(text_html, url)

            self.q_text.task_done()

    async def add_to_index(self, text_html, url):
        without_tags = self.html_to_text(text_html)

        index_body = {
            'url': url,
            'content': without_tags
        }
        self.i += 1
        if self.i % 10 == 0:
            await stat(self.root_url, self.i, self.user_id)
        index = self.root_url
        for i in ['"', '*', '\\', '<', '|', '>', '/', '?', ':']:
            index = index.replace(i, '')
        await self.es.index(index=index, doc_type='html', body=index_body, id=url)

    def html_to_text(self, html):
        if html.find('head') is not None:
            html.head.decompose()
        if html.find('script') is not None:
            html.script.decompose()
        without_tags = html.get_text(" ", strip=True)
        return without_tags

    async def rps_control(self):
        while True:
            time_request = await self.q_rps.get()
            async with self.sem:
                if time.time() - time_request < 1:
                    await asyncio.sleep(1 - (time.time() - time_request))
                self.q_rps.task_done()


async def listen(crawler_obj, max_rps):
    connection = await aio_pika.connect()
    queue_name = "crawler_in"
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
                    if type == 'index':
                        token = Token.objects.get(token=body['token'])
                        user_id = token.user_id
                        try:
                            stats = CrawlerStats.objects.get(domain=body['domain'])
                            if datetime.now() - stats.time < timedelta(days=30):
                                continue
                        except:
                            pass
                        await crawler_obj.q_root_url.put((body['domain'], user_id))


async def stat(root_url, pages_count, user_id):
    CrawlerStats(domain=root_url, pages_count=pages_count, author_id=user_id)
    pass


max_rps = 10
crawler = Crawler(max_rps=max_rps)

loop = asyncio.get_event_loop()
loop.create_task(crawler.crawl())
loop.create_task(listen(crawler, max_rps))
loop.run_forever()
