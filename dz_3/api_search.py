from aioelasticsearch import Elasticsearch
from aiohttp import web
import json

with open('config_api.json') as json_data_file:
    data = json.load(json_data_file)


async def aioes_request(request):
    es = Elasticsearch(data['elasticsearch']['hosts'])
    query = request.rel_url.query
    if 'q' in query:
        q = query['q']
    else:
        return []
    try:
        limit = int(query['limit']) if 'limit' in query else 10
    except ValueError:
        limit = 10
    try:
        offset = int(query['offset']) if 'offset' in query else 0
    except ValueError:
        offset = 0


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
    return web.Response(text=json.dumps({'request': q,
                                         'limit': limit,
                                         'offset': offset,
                                         'urls': urls}))


def create_app(loop=None):
    app = web.Application(loop=loop)
    app.add_routes([web.get('/api/v1/search', aioes_request)])
    return app


app = web.run_app(create_app())
