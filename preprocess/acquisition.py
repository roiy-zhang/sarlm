# -*- coding: UTF-8 -*-
import os
import osmium
import pandas as pd
from threadsparql import get_results

# download osm：http://download.geofabrik.de/index.html

class OsmAcqusition(osmium.SimpleHandler):
    def __init__(self, cnty):
        osmium.SimpleHandler.__init__(self)
        self.num_nodes = 0
        self.num_ways = 0
        self.num_relations = 0
        self.num_name_nodes = 0
        self.num_name_ways = 0
        self.num_name_relations = 0
        self.num_wiki_nodes = 0
        self.num_wiki_ways = 0
        self.num_wiki_relations = 0
        self.cnty = cnty
        # self.overwrite = overwrite
        self.fo = open('datasource/'+self.cnty+'-osm.txt', 'w', encoding="utf-8")

    def close(self):
        self.fo.close()

    def output(self, data):
        print(data, file = self.fo)

    def node(self, n):
        self.num_nodes += 1

        if not ("name" in n.tags):
            return
        if ("wikidata" in n.tags):
            self.num_wiki_nodes += 1
        self.num_name_nodes += 1
        for k, v in n.tags:
            val = str(v)
            val = val.replace("\\", "\\\\")
            val = val.replace('"', '\\"')
            val = val.replace('\n', " ")
            k = k.replace(" ", "")
            nodes_data = str(n.id) + ',' + k + ',' + val
            self.output(nodes_data)
            # print(nodes_data)
        nodes_data = str(n.id) + ',' + 'location' + ',' + str(n.location.lat) + ',' + str(n.location.lon)
        # print(nodes_data)
        self.output(nodes_data)

class WikiIdAcqusition():
    def __init__(self, qid = None):
        self.qid = qid

    # Obtain ID data for the entire country
    def get_id(self):
        query_idlink_list = []
        query_idlink = """
        SELECT ?item
        WHERE
        {
          ?item wdt:P17 wd:%s.
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
        }
        """% self.qid
        query_idlink_list.append(query_idlink)
        results, miss = get_results(query_idlink_list)

        id_list = []
        for i in range(len(results)):
            for j in range(len(results[i]['results']['bindings'])):
                id_list.append(results[i]['results']['bindings'][j]['item']['value'])
        for i in range(len(id_list)):
            id_list[i] = id_list[i].replace('http://www.wikidata.org/entity/', '')
        newid_list = list(set(id_list))
        return newid_list

    # Obtain the ID of geographical entities for the entire country
    def get_geoid(self):
        query_geoidlink_list = []

        i = 0
        id_list = self.get_id()

        while i < len(id_list):
            new_list = id_list[i:i + 100]
            country = ''.join('wd:{0} '.format(w) for w in new_list)

            query_geoidlink = """
            SELECT ?item
            WHERE
            {
              VALUES ?item {wd:%s}
              ?item wdt:P31/wdt:P279* wd:Q27096213.
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
            }""" % country
            query_geoidlink_list.append(query_geoidlink)
            i = i + 100  # ORDER BY ?wd ?statement ?ps_


        result_list = []
        missquery_list = ['random']
        allresult_list = []


        while len(missquery_list) != 0:
            result_list, missquery_list = get_results(query_geoidlink_list)
            query_geoidlink_list = missquery_list
            allresult_list += result_list
            print('所有结果长度', len(allresult_list))

        geoid_list = []
        for i in range(len(allresult_list)):
            for j in range(len(allresult_list[i]['results']['bindings'])):
                geoid_list.append(allresult_list[i]['results']['bindings'][j]['item'][
                                          'value'])

        for i in range(len(geoid_list)):
            geoid_list[i] = geoid_list[i].replace('http://www.wikidata.org/entity/', '')

        newgeoid_list = list(set(geoid_list))
        print('geoid_list', geoid_list)

        return newgeoid_list

class WikiAcqusition():
    def __init__(self, cnty = None, qid = None, overwrite = False):
        self.cnty = cnty
        self.qid = qid
        self.overwrite = overwrite

    def load_geoid(self):

        wikiid_output = 'datasource/' + self.cnty + '-wikiid.txt'
        if not os.path.exists(wikiid_output) or os.stat(wikiid_output).st_size == 0 or self.overwrite:
            wikiid_acqusition = WikiIdAcqusition(qid=self.qid)
            wikiid_list = wikiid_acqusition.get_geoid()
            if len(wikiid_list) == 0:
                print('wikidata id acquisition failed')
                return
            else:
                file = open(wikiid_output, 'w', encoding='utf-8')
                for line in wikiid_list:
                    file.write(str(line) + '\n')
                file.close()
        else:
            wikiid_list = []
            with open(wikiid_output, 'r', encoding='utf-8') as file:
                for data in file:
                    newdata = data.strip("\n")
                    wikiid_list.append(newdata)

        return wikiid_list

    @staticmethod
    def analysis(col_name, i, col_list, result):
        # col_list = []
        if str(type(result[i]))!="<class 'dict'>":
            if str(type(result[i])) == "<class 'str'>":
                result[i] = eval(result[i])
            else:
                print(type(result[i]))
                print(result[i])
        for j in range(len(result[i]['results']['bindings'])):
            if col_name in result[i]['results']['bindings'][j]:
                value = result[i]['results']['bindings'][j][col_name]['value']
                col_list.append(value)
            else:
                col_list.append('NONE')
        return col_list

    def get_geoentity(self):
        wikiid_list = self.load_geoid()
        i = 0
        query_geoneighbor_list = []
        while i < len(wikiid_list):
            new_lst = wikiid_list[i:i + 100]
            mystring = ''.join('wd:{0} '.format(w) for w in new_lst)
            query_geoneighbor = """
            SELECT ?kgentity ?kgentityLabel ?wdLabel ?ps_Label ?ps_
            WHERE{
              VALUES ?kgentity {wd:%s}

              ?kgentity ?p ?statement.
              ?statement ?ps ?ps_.

              ?wd wikibase:claim ?p.
              ?wd wikibase:statementProperty ?ps.

              OPTIONAL {
              ?statement ?pq ?pq_.
              ?wdpq wikibase:qualifier ?pq.
              }

              SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
            }""" % mystring
            query_geoneighbor_list.append(query_geoneighbor)
            i = i + 100

        missquery_list = ['random']
        allresult_list = []
        result_list = []

        while len(missquery_list) != 0:
            result_list, missquery_list = get_results(query_geoneighbor_list)
            query_geoidlink_list = missquery_list
            allresult_list += result_list
        return allresult_list

    def extract_geoentity(self):

        wikidata_output = 'datasource/' + self.cnty + '-wikidata.txt'
        if not os.path.exists(wikidata_output) or os.stat(wikidata_output).st_size == 0 or self.overwrite:
            wikidata_list = self.get_geoentity()
            if len(wikidata_list) == 0:
                print('wikidata acquisition failed')
                return
            else:
                file = open(wikidata_output, 'w', encoding='utf-8')
                for line in wikidata_list:
                    file.write(str(line) + '\n')
                file.close()
        else:
            wikidata_list = []
            with open(wikidata_output, 'r', encoding='utf-8') as file:
                for data in file:
                    # newdata = data.strip("\n")
                    wikidata_list.append(data)
            # geoentity_count = len(wikiid_list)

        headid_list = []
        headname_list = []
        relation_list = []
        tailname_list = []
        taillink_list = []

        # col_name_list = ['kgentity', 'kgentityLabel', 'wdLabel', 'ps_Label', 'ps_']
        col_map = {'kgentity':headid_list, 'kgentityLabel':headname_list,
                   'wdLabel':relation_list, 'ps_Label':tailname_list, 'ps_':taillink_list}
        # all_list = []

        for i in range(len(wikidata_list)):
            if wikidata_list[i]:
                for col_name, col_list in col_map.items():
                    col_list = self.analysis(col_name, i, col_list, wikidata_list)
                    # all_list.append(col_list)
            else:
                pass

        for i in range(len(headid_list)):
            headid_list[i] = headid_list[i].replace('http://www.wikidata.org/entity/', '')  # 去掉前缀，取q****

        wikidata = pd.DataFrame(list(zip(headid_list, headname_list, relation_list, tailname_list, taillink_list)),
                                columns=['headid', 'headname', 'relation', 'tailname', 'taillink'])
        wikidata = wikidata.drop_duplicates()
        return wikidata





























