#!/usr/bin/env python

# -*- coding: utf-8 -*-

####
# scripts to index a completely downloaded day
###

__author__ = 'Martin Andreas Woerz'
__email__ = 'm.woerz@ieservices.de'
__copyright__ = "Copyright 2018, Martin Woerz"
__version__ = "0.0.7"

import glob
import os
from time import time
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# define the initial values
target_url = "http://archive.luftdaten.info/"
data_directory = 'data/luftdaten/'

# establishes the connection to the Elastic Search server
ELASTICSEARCH_HOST = os.environ.get("ELASTICSEARCH_HOST") if 'ELASTICSEARCH_HOST' in os.environ else "localhost"
ELASTICSEARCH_PORT = os.environ.get("ELASTICSEARCH_PORT") if 'ELASTICSEARCH_PORT' in os.environ else "9200"
ELASTICSEARCH_USERNAME = os.environ.get("ELASTICSEARCH_USERNAME") if 'ELASTICSEARCH_USERNAME' in os.environ else ""
ELASTICSEARCH_PASSWORD = os.environ.get("ELASTICSEARCH_PASSWORD") if 'ELASTICSEARCH_PASSWORD' in os.environ else ""

# use single host mode => creates
# set env: ELASTICSEARCH_SINGLE_HOST=0 to disable the single host mode
ELASTICSEARCH_SINGLE_HOST = not os.environ.get("ELASTICSEARCH_SINGLE_HOST") == "0" if 'ELASTICSEARCH_SINGLE_HOST' in os.environ else True

# init ElasticSearch
http_auth = ()

if ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD:
    http_auth = (ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)

es = Elasticsearch('http://%s:%s/' % (ELASTICSEARCH_HOST, ELASTICSEARCH_PORT), http_auth=http_auth)
es_doc_type = "sensor_data"


def prepare_index(index_name, truncate=False):
    indices_exists = es.indices.exists(index_name)
    
    if indices_exists and truncate:
        es.indices.delete(index_name)
        indices_exists = False
    
    if not indices_exists:
        message = "Index '{}' + mapping will be created".format(index_name)
        print("    " + message)
        
        mapping = {
            "mappings": {}
        }
        
        # added the mapping
        mapping["mappings"][es_doc_type] = {
            "properties": {
                "geo_location": {
                    "type": "geo_point",
                }
            }
        }
        
        if ELASTICSEARCH_SINGLE_HOST:
            # if this is a once node cluster only create 1 shard and no replicas
            mapping["settings"] = {"number_of_replicas": 0}
        
        es.indices.create(index_name, body=mapping)


def index_csv_data(index_name, records):
    """
        indexes a given csv file into ElasticSearch
    :param index_name: str the index name
    :param records: list the data to index
    """
    start_time = time()
    
    items_count = len(records)
    
    # index the records
    try:
        bulk(es, records)
        duration = time() - start_time
        speed = items_count / duration
        message = "Indexing of bucket done. Wrote %s items into %s in %.3fs. Speed (%s items/s)." % (items_count, index_name, duration, round(speed, 2))
        print("    " + message)
    except Exception as e:
        import_message = "Error in indexing. Used [index:'{}'] [doc_type:{}]. Details:\n  {}".format(index_name, es_doc_type, e)
        print("  " + import_message)
    
    return items_count


def collect_csv_data(index_name, csv_file, current_id, chunk_size=8 * 1024):
    # open csv file
    fp = open(csv_file)  # read csv
    
    # parse csv with pandas # todo add: parse_dates=True, index_col='DateTime',
    csv_data = pd.read_csv(fp, iterator=True, chunksize=chunk_size, parse_dates=True)
    
    # start indexing
    message = "Collecting csv data for bucket list. Reading file '{}'".format(csv_file)
    print("      " + message)
    
    file_date = os.path.split(csv_file)[0].split(os.path.sep)[-1]
    
    list_records = []
    for i, df in enumerate(csv_data):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.drop('Unnamed: 0', axis=1, inplace=True)
        
        # fetch the data frame records
        records = df.where(pd.notnull(df), None).T.to_dict()
        
        # enrich index entry with meta data
        for df_index in records:
            record = records[df_index]
            
            # related import directory (date)
            record['file_date'] = file_date
            
            # related import file
            record['file_id'] = current_id
            
            # prepare the geo data (array representation with [lon,lat])
            # see @url https://www.elastic.co/guide/en/elasticsearch/guide/current/lat-lon-formats.html
            record['geo_location'] = [record['lon'], record['lat']]
            
            del record['lat']
            del record['lon']
            
            record.update({
                "_index": index_name,
                "_type": es_doc_type,
            })
            
            list_records.append(record)
    
    return list_records


def main():
    csv_files = glob.glob('data/luftdaten_full/2018-05-07/*.csv')
    
    index_data_name = "luftdate_full_2018-05-07"
    
    prepare_index(index_data_name, truncate=True)
    
    start_time = time()
    
    bucket = []
    items_count = 0
    for csv_file in csv_files:
        file_id = int(csv_file.split('.')[-2].split('_')[-1])
        bucket.extend(collect_csv_data(index_data_name, csv_file, file_id))
        
        if len(bucket) > 2000:
            items_count += index_csv_data(index_data_name, bucket)
            bucket = []
        
        duration = time() - start_time
        speed = items_count / duration
        message = "Overall speed: Wrote %s items into %s in %.3fs. Speed (%s items/s)." % (items_count, index_data_name, duration, round(speed, 2))
        print("  " + message)
    
    index_csv_data(index_data_name, bucket)


if __name__ == "__main__":
    main()
