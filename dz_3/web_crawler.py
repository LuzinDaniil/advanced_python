import asyncio
from bs4 import BeautifulSoup
from aioelasticsearch import Elasticsearch
import aiohttp
import urllib.parse
from lxml import html
import time

root_url = 'https://docs.python.org'
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

        self.sem = asyncio.Semaphore(max_rps)

        self.session = None
        self.es = Elasticsearch()

    async def crawl(self):
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(timeout=timeout, loop=loop)
        await self.q_url.put(root_url)
        page_first = await self.session.get(root_url)
        await self.q_text.put((BeautifulSoup(await page_first.text(), 'lxml'), root_url))
        workers = [asyncio.create_task(self.worker_crawl()) for _ in range(self.max_tasks)]
        workers += [asyncio.create_task(self.worker_elastic()) for _ in range(self.max_tasks)]
        workers += [asyncio.create_task(self.rps_control())]

        await self.q_url.join()
        await self.q_text.join()

        for w in workers:
            w.cancel()

        await self.session.close()
        await self.es.close()

    async def worker_crawl(self):
        while True:
            await self.sem.acquire()
            url = await self.q_url.get()
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

            if root_url in link and '#' not in link and any(symbol != link[-4:] for symbol in file_extensions):
                links.add(link)
            elif any(symbol in link for symbol in stop_symbols) or any(symbol in url_page for symbol in stop_symbols2):
                continue
            else:
                links.add(urllib.parse.urljoin(url_page, link))
        return links

    async def worker_elastic(self):
        while True:
            text_html, url = await self.q_text.get()
            await self.add_to_index(text_html, url)

            self.q_text.task_done()

    async def add_to_index(self, text_html, url):
        without_tags = self.html_to_text(text_html)

        index_body = {
            'url': url,
            'content': without_tags
        }
        await self.es.index(index='site.docs.python.org', doc_type='html', body=index_body, id=url)

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
            if time.time()- time_request < 1:
                await asyncio.sleep(1 - (time.time() - time_request))
                self.sem.release()
            else:
                self.sem.release()


loop = asyncio.get_event_loop()
crawler = Crawler(max_rps=10)
loop.run_until_complete(crawler.crawl())
