import argparse
import os
from sklearn.utils import shuffle
import re
import pandas as pd
from acquisition import OsmAcqusition, WikiAcqusition
from process import OsmProcess, WikiProcess
from trans import OsmTransform, WikiTransform

# WIKIAcqusition
def del_val(output_path, data, overwrite):
    if not os.path.exists(output_path) or os.stat(output_path).st_size == 0 or overwrite:
        with open(output_path, 'w', encoding='utf-8') as fout:
            for idx, line in data.iteritems():
                length = len(line.split('\t'))
                if length == 3:
                    x, y, label = line.split('\t')
                    x_del = x.replace('VAL ', '')
                    y_del = y.replace('VAL ', '')
                    new_line = x_del + '\t' + y_del + '\t' + label + '\n'
                    fout.write(new_line)

def filter(data=None, config=None, datatype=None):
    remain = []
    drop = []
    for idx, line in data.iteritems():
        length = len(line.split('\t'))
        if length == 3:
            x, y, label = line.split('\t')

            x_list = re.split('COL ', x)
            y_list = re.split('COL ', y)

            x_loc = y_loc = x_name = y_name = []
            for item in x_list:
                if re.findall(r"Point\((.*?)\)", item):
                    x_loc = re.findall(r"Point\((.*?)\)", item)
                if re.match('name VAL (.*)', item):
                    x_name = re.findall('name VAL (.*)', item)
            for item in y_list:
                if re.findall(r"Point\((.*?)\)", item):
                    y_loc = re.findall(r"Point\((.*?)\)", item)
                if re.match('name VAL (.*)', item):
                    y_name = re.findall('name VAL (.*)', item)
            if config.limit:
                if len(x_loc)!=0 and len(y_loc)!=0 and len(x_name)!=0 and len(y_name)!=0:
                    x_loc = x_loc[0].strip().split(',')
                    y_loc = y_loc[0].strip().split(',')

                    if len(x_loc) == 2 and len(y_loc) == 2:
                        remain.append(idx)
                    else:
                        drop.append(idx)
                else:
                    drop.append(idx)
            else:
                if len(x_loc) and len(y_loc):
                    x_loc = x_loc[0].strip().split(',')
                    y_loc = y_loc[0].strip().split(',')

                    if len(x_loc) == 2 and len(y_loc) == 2:
                        remain.append(idx)
                    else:
                        drop.append(idx)
                else:
                    drop.append(idx)
        else:
            pass
    data = data[remain]
    data = shuffle(data, random_state=10)

    if config.limit:
        output = 'datasource/' + hp.country + '-' + datatype + '-limit.csv'
        data.to_csv(output, header=False, index=False)

        out_fn = 'datasource/' + hp.country + '-' + datatype + '-limit-sarlm.csv'
        del_val(output_path=out_fn, data=data, overwrite=config.overwrite)

    else:
        out_fn = 'datasource/' + hp.country + '-' + datatype + '-sarlm.csv'
        del_val(output_path=out_fn, data=data, overwrite=config.overwrite)

if __name__=="__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--country", type=str, default="germany")
    parser.add_argument("--overwrite", type=bool, default=False)
    parser.add_argument("--statistic", type=bool, default=False)
    parser.add_argument("--qid", type=str, default="Q183")
    parser.add_argument("--alias", type=bool, default=True)
    parser.add_argument("--limit", type=bool, default=True)

    hp = parser.parse_args()

    # OSM data acquisition
    osm_input = 'datasource/' + hp.country + '-latest.osm.pbf'
    osm_output = 'datasource/' + hp.country + '-osm.txt'
    osmpre_input = osm_output
    osmpre_output = 'datasource/' + hp.country + '-osmpre.csv'
    osmtrans_input = osmpre_output
    osmtrans_output = 'datasource/' + hp.country + '-osmtrans.csv'

    if not os.path.exists(osm_output) or os.stat(osm_output).st_size == 0 or hp.overwrite:
        print('Loading %s…' %osm_input)
        osm_acqusition = OsmAcqusition(cnty=hp.country)
        osm_acqusition.apply_file(filename=osm_input, locations=True)
        print('Generated %s…' %osm_output)
    if not os.path.exists(osmpre_output) or os.stat(osmpre_output).st_size == 0 or hp.overwrite:
        print('Loading %s…' %osmpre_input)
        print('Processing %s…' % osmpre_input)
        osm_process = OsmProcess(input=osmpre_input)
        osm = osm_process.filter()
        osm.to_csv(osmpre_output, header=True)
        # print(osm)
    if not os.path.exists(osmtrans_output) or os.stat(osmtrans_output).st_size == 0 or hp.overwrite:
        print('Loading %s…' % osmtrans_input)
        print('Transforming %s…' % osmtrans_input)
        osm_trans = OsmTransform(input=osmtrans_input)
        osm_trans.to_csv(osmtrans_output, header=True)
    else:
        osm_trans = pd.read_csv(osmtrans_output, encoding='utf-8', index_col=0)

    # Wikidata data acquisition
    wiki_output = 'datasource/' + hp.country + '-wiki.csv'
    wikipre_input = wiki_output
    wikipre_output = 'datasource/' + hp.country + '-wikipre.csv'
    wikitrans_input = wikipre_output
    wikitrans_output = 'datasource/' + hp.country + '-wikitrans.csv'
    if not os.path.exists(wiki_output) or os.stat(wiki_output).st_size == 0 or hp.overwrite:
        print('Getting wikidata…')
        wiki_acqusition = WikiAcqusition(cnty=hp.country, qid=hp.qid, overwrite=hp.overwrite)
        wikidata = wiki_acqusition.extract_geoentity()
        wikidata.to_csv(wiki_output, index=False, header=True)
        print('Generated %s…' % wiki_output)
    if not os.path.exists(wikipre_output) or os.stat(wikipre_output).st_size == 0 or hp.overwrite:
        print('Processing %s…' % wikipre_input)
        # wikidata = pd.read_csv(wikipre_input, encoding='utf-8')
        wiki_process = WikiProcess(input=wikipre_input)
        wiki = wiki_process.filter()
        wiki.to_csv(wikipre_output)
        print('Generated %s…' % wikipre_output)
    if not os.path.exists(wikitrans_output) or os.stat(wikitrans_output).st_size == 0 or hp.overwrite:
        print('Processing %s…' % wikitrans_input)
        wiki_trans = WikiTransform(input=wikitrans_input)
        wiki_trans.to_csv(wikitrans_output, header=True)
    else:
        wiki_trans = pd.read_csv(wikitrans_output, encoding='utf-8', index_col=0)

    pos = pd.merge(osm_trans, wiki_trans, left_on='wikidata', right_on='headid', how='inner')
    pos_num = pos.shape[0]

    osm_link = pos[['concat_x', 'wikidata']].rename(columns={'concat_x': 'concat'})
    wiki_link = pos[['concat_y', 'headid']].rename(columns={'concat_y': 'concat'})

    wiki_remain = pd.concat([wiki_trans, wiki_link]).drop_duplicates(keep=False).rename(columns={'concat': 'concat_wiki'})
    osm_remain = pd.concat([osm_trans, osm_link]).drop_duplicates(keep=False).rename(columns={'concat': 'concat_osm'})

    # Obtain the smallest of the three values as the cardinality
    base_num = min(pos_num, osm_remain.shape[0], wiki_remain.shape[0])
    pos = pos.sample(n=base_num, random_state=111).reset_index(drop=True)
    pos_ = pos['concat_x'] + '\t' + pos['concat_y'] + '\t' + '1'

    wiki_unlink = wiki_remain.sample(n=base_num, random_state=157).reset_index(drop=True)
    neg1 = osm_link.join(wiki_unlink)
    neg1_ = neg1['concat'] + '\t' + neg1['concat_wiki'] + '\t' + '0'

    osm_unlink = osm_remain.sample(n=base_num, random_state=157).reset_index(drop=True)
    neg2 = wiki_link.join(osm_unlink)
    neg2_ = neg2['concat_osm'] + '\t' + neg2['concat'] + '\t' + '0'

    wiki_unlink2 = wiki_remain.sample(n=base_num, random_state=101).reset_index(drop=True)
    osm_unlink2 = osm_remain.sample(n=base_num, random_state=101).reset_index(drop=True)
    neg3 = osm_unlink2.join(wiki_unlink2)
    neg3_ = neg3['concat_osm'] + '\t' + neg3['concat_wiki'] + '\t' + '0'

    train_base = int(base_num*0.8)
    valid_base = int(base_num*0.1)
    test_base = base_num - train_base - valid_base
    index1 = train_base
    index2 = train_base + valid_base
    index3 = index2 + test_base

    train = pd.concat([pos_[0:index1],neg1_[0:index1], neg2_[0:index1],
                       neg3_[0:index1]]).reset_index(drop=True)
    valid = pd.concat([pos_[index1:index2],neg1_[index1:index2],
                       neg2_[index1:index2], neg3_[index1:index2]]).reset_index(drop=True)
    test = pd.concat([pos_[index2:index3],neg1_[index2:index3],
                      neg2_[index2:index3], neg3_[index2:index3]]).reset_index(drop=True)

    filter(train, config=hp, datatype='train')
    filter(valid, config=hp, datatype='valid')
    filter(test, config=hp, datatype='test')
