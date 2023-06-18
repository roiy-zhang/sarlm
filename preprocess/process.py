# -*- coding: UTF-8 -*-
import pandas as pd
import re

class OsmProcess():

    def __init__(self, input = None):
        self.input = input

    def load(self):
        osmdata = []
        with open(self.input, 'r', encoding='utf-8') as file:
            for data in file:
                newdata = data.strip("\n").split(',', maxsplit=2)
                osmdata.append(newdata)
        osm = pd.DataFrame(osmdata, columns=['id', 'key', 'value'])
        return osm

    def filter(self):

        osm = self.load()

        osm.loc[osm.key.eq('location'), 'value'] = osm.loc[osm.key.eq('location'),
                                                     'value'].transform(lambda x: 'Point(' + x + ')')
        osm.replace({"[a-zA-Z]+://[^\s]*": None, "www.[^\s]*": None},
                    regex=True, inplace=True)
        osm['key'].replace({"(.*?)wikipedia": None, "(.*?)ID": None, "(.*?)wikidata": 'wikidata', "name:(.*?)": None},
                           regex=True, inplace=True)
        osm.dropna(axis=0, how='any', inplace=True)

        new = osm.loc[osm['key'] == 'location'].groupby(osm['id']).count() <= 1
        osm_repeat = osm.loc[osm['id'].isin(new[new['id'] == False].index), :]

        loc = '((^[-+]?(?:[1-8]?\d(?:\.\d+)?|90(?:\.0+)?)),\s*([-+]?(?:180(?:\.0+)?|(?:(?:1[0-7]\d)|(?:[1-9]?\d))(?:\.\d+)?))$)'
        index_ = ''
        for index, line in osm_repeat.iterrows():
            if re.match('Point\((.*?)\)', str(line['value'])):
                origin = osm_repeat.loc[index, 'value'].replace('Point(', '').replace(')','')
                if re.match(loc, origin):
                    if index == index_:
                        osm_repeat.loc[index, 'value'] = None
                    else:
                        index_ = index
                else:
                    osm_repeat.loc[index, 'value'] = None
            else:
                pass

        osm.loc[osm['id'].isin(new[new['id'] == False].index), :] = osm_repeat
        osm.dropna(axis=0, how='any', inplace=True)

        osm_drop = osm.drop(osm.loc[osm['key'] == 'wikidata'].index, inplace=False)
        osm_count = osm_drop.groupby(osm_drop['id'], as_index=True).count() >= 4
        osm_formal = osm.loc[osm['id'].isin(osm_count[osm_count.values == True].index), :]

        return osm_formal

class WikiProcess():
    def __init__(self, input = None):
        self.input = input

    '''
        wikidata：'headid', 'headname', 'relation', 'tailname', 'taillink'
    '''
    def filter(self):
        wiki = pd.read_csv(self.input, encoding='utf-8')

        wiki['tailname'].replace({"[a-zA-Z]+://[^\s]*": None, "www.[^\s]*": None}, regex=True, inplace=True)
        wiki['relation'].replace({"(.*?)ID": None}, regex=True, inplace=True)
        wiki.loc[wiki.relation.eq('coordinate location'), 'tailname'] = wiki.loc[wiki.relation.eq('coordinate location'),
                                                           'tailname'].str.replace(' ', ',')

        wiki.loc[wiki['relation'] != 'coordinate location', 'tailname'] = wiki.loc[wiki['relation'] != 'coordinate location',
                                                                                   'tailname'].str.replace('Point(.*)', '')

        wiki.dropna(axis=0, how='any', inplace=True)

        new = wiki.loc[wiki['relation'] == 'coordinate location'].groupby(wiki['headid']).count() <= 1
        wiki_repeat = wiki.loc[wiki['headid'].isin(new[new['headid'] == False].index), :]
        loc = '((^[-+]?(?:[1-8]?\d(?:\.\d+)?|90(?:\.0+)?)),\s*([-+]?(?:180(?:\.0+)?|(?:(?:1[0-7]\d)|(?:[1-9]?\d))(?:\.\d+)?))$)'
        index_ = ''
        for index, line in wiki_repeat.iterrows():
            if re.match('Point\((.*?)\)', str(line['tailname'])):
                origin = wiki_repeat.loc[index, 'tailname'].replace('Point(', '').replace(')','')
                # swap
                temp_list = origin.split(',')
                # print(temp_list)
                temp_list[1], temp_list[0] = temp_list[0], temp_list[1]
                origin = (',').join(temp_list)
                if re.match(loc, origin):
                    if index == index_:
                        wiki_repeat.loc[index, 'tailname'] = None
                    else:
                        index_ = index
                        # wiki_repeat.loc[index, 'tailname'] = 'Point('+origin+')'
                        # wiki_repeat.loc[index, 'tailname'] = wiki_repeat.loc[index, 'tailname'].replace(' ', ',')
                else:
                    wiki_repeat.loc[index, 'tailname'] = None
            else:
                pass

        wiki.loc[wiki['headid'].isin(new[new['headid'] == False].index), :] = wiki_repeat
        wiki.dropna(axis=0, how='any', inplace=True)
        wiki.loc[wiki.relation.eq('coordinate location'), 'tailname'] = \
            wiki.loc[wiki.relation.eq('coordinate location'),'tailname'].str.replace("Point\(", '').str.replace(")",'').\
            str.split(',').transform(lambda x: 'Point(' + x[1] + ',' + x[0] + ')')

        wikiid_noname = wiki[wiki['headname'] == wiki['headid']]['headid'].unique()
        wiki['headname'].loc[wiki['headname'].isin(wikiid_noname)] = None
        wiki = wiki.reset_index(drop=True)


        compare_list = [re.match('Point(.*)', str(string)) for string in wiki['taillink']]
        wikiid_withloc = []
        for i in range(len(compare_list)):
            if compare_list[i] != None:
                # print(wiki['headid'][i])
                wikiid_withloc.append(wiki['headid'][i])
        wiki_withloc = wiki.loc[wiki['headid'].isin(wikiid_withloc)].reset_index(drop=True)

        wiki_count = wiki_withloc.groupby(wiki_withloc['headid']).count()['relation'] >= 4
        wiki_formal = wiki_withloc.loc[wiki_withloc['headid'].isin(wiki_count[wiki_count.values == True].index),
                      :].reset_index(drop=True)

        return wiki_formal





















