import asyncio
from bs4 import BeautifulSoup
from aioelasticsearch import Elasticsearch
import aiohttp
import urllib.parse
from lxml import html

root_url = 'https://docs.python.org'
stop_symbols = ['#', 'zip', 'epub', 'bz2', '.io', '/fr/', '/ja/', '/ko/', '/zh-cn/', 'http', ':']
stop_symbols2 = ['#', 'zip', 'epub', 'bz2', '.io', '/fr/', '/ja/', '/ko/', '/zh-cn/']
file_extensions = ['.zip', 'epub', '.bz2']


class Crawler:
    def __init__(self, max_rps):
        self.max_tasks = max_rps
        self.q = asyncio.Queue()
        self.q2 = asyncio.Queue()
        self.seen_urls = set()

        self.session = None
        self.es = Elasticsearch()

    async def crawl(self):
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(timeout=timeout, loop=loop)
        await self.q.put(root_url)
        page_first = await self.session.get(root_url)
        await self.q2.put((BeautifulSoup(await page_first.text(), 'lxml'), root_url))
        workers = [asyncio.create_task(self.work()) for _ in range(self.max_tasks)]
        workers2 = [asyncio.create_task(self.work2()) for _ in range(self.max_tasks)]

        await self.q.join()
        await self.q2.join()

        for w in workers:
            w.cancel()
        for w in workers2:
            w.cancel()

        await self.session.close()
        await self.es.close()

    async def work(self):
        while True:
            url = await self.q.get()
            await self.download(url)
            self.q.task_done()
            await asyncio.sleep(1)

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
        self.q2.put_nowait((page, url))

        for link in links.difference(self.seen_urls):
            self.q.put_nowait(link)
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

    async def work2(self):
        while True:
            text_html, url = await self.q2.get()
            await self.add_to_index(text_html, url)

            self.q2.task_done()

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


loop = asyncio.get_event_loop()
crawler = Crawler(max_rps=10)
loop.run_until_complete(crawler.crawl())
