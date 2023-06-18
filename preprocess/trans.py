# -*- coding: UTF-8 -*-
import pandas as pd

def OsmTransform(input = None):

    osm = pd.read_csv(input, encoding='utf-8', low_memory=False, usecols=[1,2,3])

    osm_wikiid = osm[osm['key'] == 'wikidata'][['id', 'value']].reset_index(drop=True)
    osm_add = pd.merge(osm, osm_wikiid, on='id', how='left')
    osm_add.columns = ['osmid', 'key', 'value', 'wikidata']
    osm_add.drop(osm_add.loc[osm_add['key'] == 'wikidata'].index, inplace=True)
    # pd.merge(left, right, how='left')

    osm_add['concat'] = 'COL ' + osm_add['key'] + ' ' + 'VAL ' + osm_add['value']#  + 'VAL '
    osm_add.dropna(axis=0, how='any', inplace=True)

    osm_concat_wiki = osm_add.groupby(['osmid'])[['concat', 'wikidata']].transform(
        lambda x: ' '.join(x)).drop_duplicates().reset_index()
    osm_concat_wiki['wikidata'] = osm_concat_wiki['wikidata'].transform(lambda x: x.replace(x, x.split(' ')[0]))
    result = osm_concat_wiki.drop(['index'], axis=1)

    return result

def WikiTransform(input = None):
    wiki = pd.read_csv(input, encoding='utf-8', usecols=[1, 2, 3, 4])

    wiki_name = wiki.groupby(['headid'])['headname'].first().drop_duplicates().reset_index()
    wiki_name['relation'] = 'name'
    wiki_name = wiki_name.rename(columns={'headname':'tailname'})
    wiki.drop('headname', axis=1, inplace=True)
    wiki.dropna(axis=0, how='any', inplace=True)

    wiki_add = pd.concat([wiki_name,wiki], axis=0)
    wiki_add['concat'] = 'COL ' + wiki_add['relation'] + ' ' + 'VAL ' + wiki_add['tailname']#  + 'VAL '
    wiki_add.dropna(axis=0, how='any', inplace=True)
    wiki_concat = wiki_add.groupby(['headid'])['concat','headid'].transform(lambda x: ' '.join(x)).\
        drop_duplicates().reset_index()
    wiki_concat['headid'].replace({" Q[0-9]*": ""}, regex=True, inplace=True)
    result = wiki_concat

    return result