"""
Pipelines allow putting together Components to build a graph.

In addition to the standard Haystack Components, custom user-defined Components
can be used in a Pipeline YAML configuration.

The classes for the Custom Components must be defined in this file.
"""


from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from haystack.nodes.base import BaseComponent
from haystack.nodes import QuestionGenerator
from haystack.schema import Document
from haystack.pipelines import QuestionAnswerGenerationPipeline
from haystack.document_stores import ElasticsearchDocumentStore

# reader model 
from haystack.reader.farm import FARMReader
import logging
from typing import List

logger = logging.getLogger(__name__)


class WikiDataLoader(BaseComponent):
    """
    A class used to represent an CustomComponent

    ...
    Attributes
    ----------
    
    client : Client
        client for the graphql server

    qa : QuestionAnswerGenerationPipeline
        pipeline for the question answer generation


    Methods
    -------
    run(**kwargs)
        run the component

    __get_wiki_pages()
        get the wiki pages from the graphql server

    __flat_toc_content(toc, page, contents, parent_title)
        flat the toc content

    __qa_generator(text)
        generate the questions and answers from the text

    __convert_pages_to_docs(wikiPages)
        convert the wiki pages to documents


    """

    
    outgoing_edges = 1

    def __init__(self):
        super().__init__()

        transport = AIOHTTPTransport(url="http://kbbotservice-stage.flairstech.com:3000/graphql")
        
        self.client = Client(transport=transport, fetch_schema_from_transport=True)

        self.qa = None



    def run(self, **kwargs):
        """
        A method to run the component

        Parameters
        ----------
        kwargs : dict
            dictionary of arguments

        Returns
        -------
        dict
            dictionary of arguments
            
        
        """
        
        pages = self.__get_wiki_pages()

        documents = self.__convert_pages_to_docs(pages)
        

        result = {"documents": documents}
        return result, "output_1"

    
    def __get_wiki_pages(self):
        """
        A method to get the wiki pages from the graphql server

        Parameters
        ----------
        None

        Returns
        -------
        list
            list of wiki pages
    
        
        """
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
        """
        A method to flat the toc content

        Parameters
        ----------
        toc : dict
            toc content

        page : dict
            page content

        contents : list
            list of contents

        parent_title : str
            parent title

        Returns
        -------
        None
                
        """
        title = toc['title']
        title_chain =  f'{parent_title} {title}'
        contents.append({'title': title,
                    'title_combined': title_chain,
                    'content': toc['summary'],
                    'page': page['title'],
                    'path': f'{page["path"]}{toc["anchor"]}'})

        for t in toc['children']:
            self.__flat_toc_content(t, page, contents, title)
        
    def __qa_generator(self, text):
        """
        A method to generate the questions and answers from the text

        Parameters
        ----------
        text : str
            text content

        Returns
        -------
        list
            list of questions and answers

        """        

        qa_list = []
        doc = Document(text)
        if self.qa == None:
            self.qa = QuestionAnswerGenerationPipeline(QuestionGenerator(), FARMReader(model_name_or_path="deepset/roberta-base-squad2", use_gpu=False))
        qa_result = self.qa.run(documents=[doc])


        qa_result = qa_result['results']
        for item in qa_result:
            try:
                question = item['query']
            except:
                question = None
            try:
                answer = item['answers'][0].answer
            except:
                answer = None
            qa_list.append({'question':question, 'answer':answer})
        return qa_list


    def __convert_pages_to_docs(self, wikiPages) :
        """
        A method to convert the wiki pages to documents

        Parameters
        ----------
        wikiPages : list
            list of wiki pages

        Returns
        -------
        list
            list of documents


        """
        
        documents = []
        contents = []
        for page in wikiPages:
            title = page['title']
            toc = page['toc']
            
            for t in toc:
                self.__flat_toc_content(t, page, contents, title)


        for content in contents:
            cont = content["content"]
            print(cont)
            qa_list = self.__qa_generator(cont)
            for item in qa_list:
                question = item['question']
                meta = {'page': content['page'], 'title': content['title'], 'path': content['path'], 'answer': item['answer']}
                documents.append(Document(content=question, meta=meta))
                print(documents)        
        
        return documents

class LowerText(BaseComponent):
    """
    A class used to lower the text

    ...

    Methods
    -------
    run(**kwargs)
        run the component

    """        
    outgoing_edges = 1




    def run(self, query: str):
        # Insert code here to manipulate the input and produce an output dictionary
        query = query.lower()
        output={
            "documents": query,
            "_debug": {"anything": "you want"}
        }
        return output, "output_1"

class WikiDataLoaderOld(BaseComponent):
    """
    A class used to represent an CustomComponent

    ...
    
    Parameters
    ----------
    outgoing_edges : int
        number of outgoing edges


    Methods
    -------
    run(**kwargs)
        run the component

    __get_wiki_pages()        
        get the wiki pages from the graphql server

    __flat_toc_content(toc, page, contents, parent_title)
        flat the toc content

    __convert_pages_to_docs(wikiPages)
        convert the wiki pages to documents

    """         
    
    outgoing_edges = 1

    def __init__(self):
        super().__init__()

        transport = AIOHTTPTransport(url="http://kbbotservice-stage.flairstech.com:3000/graphql")
        
        self.client = Client(transport=transport, fetch_schema_from_transport=True)


    def run(self, **kwargs):
        """
        A method to run the component

        Parameters
        ----------
        kwargs : dict
            dictionary of arguments

        Returns
        -------
        dict
            dictionary of arguments
            
        
        """
        
        pages = self.__get_wiki_pages()

        documents = self.__convert_pages_to_docs(pages)
        

        result = {"documents": documents}
        return result, "output_1"

    
    def __get_wiki_pages(self):
        """
        A method to get the wiki pages from the graphql server

        Parameters
        ----------
        None

        Returns
        -------
        list
            list of wiki pages
    
        
        """        
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
        """
        A method to flat the toc content

        Parameters
        ----------
        toc : dict
            toc content

        page : dict
            page content

        contents : list
            list of contents

        parent_title : str
            parent title

        Returns
        -------
        None
                
        """        
        title = toc['title']
        title_chain =  f'{parent_title} {title}'
        contents.append({'title': title,
                    'title_combined': title_chain,
                    'content': toc['summary'],
                    'page': page['title'],
                    'path': f'{page["path"]}{toc["anchor"]}'})

        for t in toc['children']:
            self.__flat_toc_content(t, page, contents, title)
        


    def __convert_pages_to_docs(self, wikiPages) :
        """
        A method to convert the wiki pages to documents

        Parameters
        ----------
        wikiPages : list
            list of wiki pages

        Returns
        -------
        list
            list of documents


        """
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
