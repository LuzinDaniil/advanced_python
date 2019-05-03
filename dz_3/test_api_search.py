import aiohttp
import asyncio


async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get('http://localhost:8080/api/v1/search?q="asyncio"&limit=100&offset=2') as resp:
            print(resp.status)
            print(await resp.text())

asyncio.run(main())