# -*- coding: UTF-8 -*-
from SPARQLWrapper import SPARQLWrapper, JSON
import threading
import time

allresult = []
missqueryList = []

def sparqlCheck(query):

    endpoint_url = "https://query.wikidata.org/sparql"
    sparql = SPARQLWrapper(endpoint_url,
                           agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36')
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    allresult.append(sparql.query().convert())


class SparqlCheckThread(threading.Thread):
    def __init__(self, query, index):
        threading.Thread.__init__(self)
        self.index = index
        self.query = query

    def run(self):
        try:
            # print(self.index)
            sparqlCheck(self.query)

        except:
            try:
                # print(self.index)
                sparqlCheck(self.query)
            except:
                missqueryList.append(self.query)

def get_results(querylist = None):
    threadList = []
    allresult.clear()
    missqueryList.clear()

    for query in querylist:
        time.sleep(2)
        threadx = SparqlCheckThread(query, querylist.index(query))
        threadList.append(threadx)
        threadx.start()

    for index, threadx in enumerate(threadList):
        # print(index)
        threadx.join()

    return allresult, missqueryList
