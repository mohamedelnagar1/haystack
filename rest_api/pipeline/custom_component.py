"""
Pipelines allow putting together Components to build a graph.

In addition to the standard Haystack Components, custom user-defined Components
can be used in a Pipeline YAML configuration.

The classes for the Custom Components must be defined in this file.
"""


from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from haystack.nodes.base import BaseComponent
from haystack.schema import Document
import logging
from typing import List

logger = logging.getLogger(__name__)

class WikiDataLoader(BaseComponent):
    
    outgoing_edges = 1

    def __init__(self):
        super().__init__()

        transport = AIOHTTPTransport(url="http://kbbotservice-stage.flairstech.com:3000/graphql")
        
        self.client = Client(transport=transport, fetch_schema_from_transport=True)


    def run(self, **kwargs):
        
        pages = self.__get_wiki_pages()

        documents = self.__convert_pages_to_docs(pages)
        
        logger.debug(f"Pages: {len(pages)} Contents: {len(documents)}")
        

        result = {"documents": documents}
        return result, "output_1"

    
    def __get_wiki_pages(self):
        query = gql(
                """
                    query{
                        pages{
                        listToC{
                            id,
                            title,
                            path,
                            toc                            
                        }
                        }
                    }
                """
                )

        result = self.client.execute(query)
        pages = result['pages']["listToC"]
        return pages


    def __flat_toc_content(self, toc, page, contents, parent_title):
        title = toc['title']
        title_chain =  f'{parent_title} {title}'
        contents.append({'title': title,
                    'title_combined': title_chain,
                    'content': toc['summary'],
                    'page': page['title'],
                    'path': f'{page["path"]}{toc["anchor"]}'})

        for t in toc['children']:
            self.__flat_toc_content(t, page, contents, title)
        


    def __convert_pages_to_docs(self, wikiPages) -> List[Document]:
        documents = []
        contents = []
        for page in wikiPages:
            title = page['title']
            toc = page['toc']
            
            for t in toc:
                self.__flat_toc_content(t, page, contents, title)

        
        for content in contents:
            cont = f'{content["title_combined"]} {content["content"]}'
            meta = {'page': content['page'], 'title': content['title'], 'path': content['path']}
            documents.append(Document(content=cont, meta=meta))
        
        return documents