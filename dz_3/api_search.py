from aioelasticsearch import Elasticsearch
from aiohttp import web


async def aioes_request(request):
    es = Elasticsearch(["127.0.0.1:9200"])
    query = request.rel_url.query
    if 'q' in query:
        q = query['q']
    else:
        return []
    limit = query['limit'] if 'limit' in query else 10
    offset = query['offset'] if 'offset' in query else 0

    res = await es.search(index='site.docs.python.org',
                          body={'query':
                                    {'match':
                                         {'content': q}
                                     },
                                'size': limit,
                                'from': offset
                                }
                          )

    urls = [r['_source']['url'] for r in res['hits']['hits']]
    await es.close()
    return web.Response(text="{}".format(urls))


def create_app(loop=None):
    app = web.Application(loop=loop)
    app.add_routes([web.get('/api/v1/search', aioes_request)])
    return app


app = web.run_app(create_app())
