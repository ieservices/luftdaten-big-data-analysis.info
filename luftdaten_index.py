#!/usr/bin/env python

# -*- coding: utf-8 -*-

####
# the script downloads and indexes the csv data of the sensor data of the fine particulates provided by the project luftdaten.info
#
# download process:
# 1. it will iterate through the provided index and download by a descending date order the csv files
# 2. it will save the csv files in the sub directory data/luftdaten/YYYY-MM-DD/
#
# the index process:
# 1. it will index all downloaded csv files into Elastic Search
# 2. it will keep track of the most recent indexed file and continue on that progress
#
###

__author__ = 'Martin Andreas Woerz'
__email__ = 'm.woerz@ieservices.de'
__copyright__ = "Copyright 2018, Martin Woerz"
__version__ = "0.0.7"

import glob
import os
from datetime import datetime
from time import time
import urllib.request
import pandas as pd
from bs4 import BeautifulSoup
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


def prepare_data_directory():
    """
    create the target directory
    """
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)


def fetch_links(resource_url, only_directories=False):
    """
        fetches all links on an HTML document
    :param resource_url: str url of the HTML document
    :return: list of urls
    """
    urls = []
    
    try:
        resp = urllib.request.urlopen(resource_url)
        
        soup = BeautifulSoup(resp, "html5lib", from_encoding=resp.info().get_param('charset'))
        
        for link in soup.find_all('a', href=True):
            if not only_directories or link['href'][-1] == '/':
                urls.append(link['href'])
    
    except Exception as e:
        print("Error occurred in fetching the data from: {}. Details:\n  {}".format(resource_url, e))
    
    return urls


def download_resources(resource_url, sub_directory, last_days=0, max_files_per_day=0, file_filters=None, sensor_ids_filter=None):
    """
        downloads all csv files
    :param resource_url: string
    :param sub_directory: string the target directory, where the csv files are stored
    :param last_days: int the amount of days back the files should be fetched
    :param max_files_per_day: int the amount of files which are fetched for each day
    :param file_filters: list the file containing the list values are accepted
    :param sensor_ids_filter: list the file containing the list of sensor ids
    """
    if file_filters is None:
        file_filters = []
    
    if sensor_ids_filter is None:
        sensor_ids_filter = []
    
    # create the data directories
    prepare_data_directory()
    
    # get all directories where are the .csv files stored (the directories are in the format: YYYY-MM-DD)
    date_directory_urls = fetch_links(target_url, True)
    
    # order by the newest to get the newest items first
    date_directory_urls.reverse()
    
    message = '{} directories of tracked days found'.format(len(date_directory_urls))
    print(message)
    
    if 0 < last_days < len(date_directory_urls):
        message = 'Only download files from the last {} days.'.format(last_days)
        print('  ' + message)
        date_directory_urls = date_directory_urls[:last_days]
    
    for date_directory_url in date_directory_urls:
        target_directory = os.path.join(sub_directory, date_directory_url)
        
        # create the target directory if not existing
        if not os.path.exists(target_directory):
            os.mkdir(target_directory)
        
        date_url_absolute = resource_url + date_directory_url
        
        url_df_path = data_directory + os.path.sep + date_directory_url + 'urls.pickle'
        
        message = 'Fetch the file list of day {}...'.format(date_directory_url.rstrip('/'))
        print('  ' + message)
        
        # cache the response of the directory content (fetch_links could take a while)
        if not os.path.exists(url_df_path):
            
            # fetch the sub directory of the date 2018-05-09
            message = "Fetch the directory structure of the day {}".format(date_directory_url[:-1])
            print('  ' + message)
            file_urls = fetch_links(date_url_absolute)
            
            df_file_urls = pd.DataFrame({'url': file_urls})
            
            # extract the date, the sensor type, file type and location id
            df_file_urls = df_file_urls['url'].str.extract('(?P<url>(?P<date>[^_]+)_(?P<sensor>[^_]+)_(?P<type>[^_]+)_(?P<id>[^_.]+).*)', expand=True)
            
            # parse the date
            df_file_urls['date'] = pd.to_datetime(df_file_urls['date'], format="%Y-%m-%d", errors="ignore")
            
            # drop the items which are not matching the regex pattern above
            df_file_urls = df_file_urls.dropna().reset_index().drop('index', axis=1)
            
            df_file_urls.to_pickle(url_df_path)
            
            message = "{} unique measurements found for the day".format(len(df_file_urls))
            print('  ' + message)
        
        # read the directory content if it was cached before
        else:
            df_file_urls = pd.read_pickle(url_df_path)
            file_urls = list(df_file_urls['url'])
        
        file_index = 1
        
        # get only the links which habe the .csv extension
        csv_urls = []
        for file_url in file_urls:
            extension = os.path.splitext(file_url)[1].lower()
            if extension == '.csv':
                csv_urls.append(file_url)
        
        message = 'For date {} tracking {} files have found'.format(date_directory_url.rstrip('/'), len(csv_urls))
        print('  ' + message)
        
        if file_filters and len(file_filters) > 0:
            
            message = 'File filter for downloads has been set. ' \
                      'Only accepting files containing: {}'.format(", ".join(file_filters))
            print('  ' + message)
            
            file_limit_reached = False
            csv_urls_filtered = []
            
            # first iterate over the filter
            for file_filter in file_filters:
                
                # then iterate over the csv files to get all files matching the filter
                for csv_url in csv_urls:
                    if csv_url.find(file_filter) > -1:
                        csv_urls_filtered.append(csv_url)
                    
                    if 0 < max_files_per_day < len(csv_urls_filtered) + 1:
                        message = 'More files found then accepted. Limiting the files to be downloaded to: {}'.format(max_files_per_day)
                        print('  ' + message)
                        file_limit_reached = True
                        break
                
                if file_limit_reached:
                    break
            
            csv_urls = csv_urls_filtered
        
        if sensor_ids_filter and len(sensor_ids_filter) > 0:
            
            message = 'File filter for downloads has been set. ' \
                      'Only accepting files containing: {}'.format(", ".join([str(id) for id in sensor_ids_filter]))
            print('  ' + message)
            
            file_limit_reached = False
            csv_urls_filtered = []
            
            # iterate over the files
            for csv_url in csv_urls:
                file_id = int(csv_url.split('.')[-2].split('_')[-1])
                if file_id in sensor_ids_filter:
                    csv_urls_filtered.append(csv_url)
                
                if 0 < max_files_per_day < len(csv_urls_filtered) + 1:
                    message = 'More files found then accepted. Limiting the files to be downloaded to: {}'.format(max_files_per_day)
                    print('  ' + message)
                    break
                
                if file_limit_reached:
                    break
            
            csv_urls = csv_urls_filtered
        
        elif 0 < max_files_per_day < len(csv_urls):
            message = 'More files found then accepted. Limiting the files to be downloaded to: {}'.format(max_files_per_day)
            print('  ' + message)
            csv_urls = csv_urls[:max_files_per_day]
        
        for file_url in csv_urls:
            uri = date_url_absolute + file_url
            
            local_filename = uri.split('://')[1].replace('/', '_')
            
            target_filename = os.path.join(target_directory, local_filename)
            
            if not os.path.exists(target_filename):
                progress = round((file_index / len(csv_urls) * 100), 2)
                message = 'Progress: {}% | Download csv file: {} | File {}/{}'.format(progress, uri, file_index, len(csv_urls))
                print('    ' + message)
                df = pd.read_csv(uri, sep=';')
                df.to_csv(target_filename)
            file_index += 1
        
        print("")


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


def prepare_and_cleanup_index(index_name, file_id, file_date):
    # delete all partially indexed entries (when the import process has been aborted)
    
    try:
        if es.count(index_name, doc_type=es_doc_type).get('count') > 0:
            
            match_query = {"bool": {"must":
                [
                    {"match": {"file_date": file_date}},
                    {"match": {"file_id": file_id}},
                ]
            }}
            
            # fetches all index items which are matching the same file date and id
            result = es.delete_by_query(index_name, doc_type=es_doc_type, body={"query": match_query})
            
            if result.get('deleted') > 0:
                import_message = "Deleted {} partially imported indexes.".format(result.get('deleted'))
                print("      " + import_message)
    except Exception as e:
        message = "Error in performing elasticsearch action. Details\n  '{}'".format(e)
        print(message)


def index_csv_data(index_name, index_files, records, collection_data):
    """
        indexes a given csv file into ElasticSearch
    :param index_name: str the index name
    :param index_files: str the index files
    :param records: list the data to index
    :param collection_data: list the related meta information about the records
    """
    start_time = time()
    
    # index the records
    try:
        bulk(es, records)
    except Exception as e:
        import_message = "Error in indexing. Used [index:'{}'] [doc_type:{}]. Details:\n  {}".format(index_name, es_doc_type, e)
        print("  " + import_message)
    
    # once all items of a file have been indexed, save the import status to the file index
    for bucket_collection_item in collection_data:
        file_id = bucket_collection_item.get('file_id')
        file_date = bucket_collection_item.get('file_date')
        file_index_data = {"file_id": file_id, "file_date": file_date, 'timestamp': datetime.now()}
        es.index(index_files, doc_type="indexed", body=file_index_data)
    
    duration = time() - start_time
    items = len(records)
    speed = items / duration
    message = "Indexing of bucket done. Wrote %s items into %s in %.3fs. Speed (%s items/s)." % (items, index_name, duration, round(speed, 2))
    print("    " + message)


def prepare_file_index(index_files_name, truncate_index=False):
    empty_index = False
    
    # check the status of the indices
    index_file_exists = es.indices.exists(index_files_name)
    
    # does the file index exist
    if index_file_exists and es.count(index=index_files_name, doc_type="indexed").get('count') == 0:
        empty_index = True
    
    # if this is the first import or the import should be restarted (WARNING: will delete all indices data!!)
    if truncate_index or empty_index:
        
        if index_file_exists:
            
            if truncate_index:
                message = "Index '{}' will be forcefully deleted (truncate_index=True)".format(index_files_name)
            else:
                message = "Index '{}' is empty and will be recreated".format(index_files_name)
            print("    " + message)
            
            es.indices.delete(index_files_name)
            index_files_name = False
    
    if not index_file_exists:
        message = "Index '{}' + mapping will be created".format(index_files_name)
        print("    " + message)
        
        mapping = {
            "mappings": {}
        }
        
        if ELASTICSEARCH_SINGLE_HOST:
            # if this is a once node cluster only create 1 shard and no replicas
            mapping["settings"] = {"number_of_replicas": 0}
        
        es.indices.create(index_files_name, body=mapping)


def prepare_data_index(index_name, truncate_index=False):
    empty_index = False
    
    # check the status of the indices
    index_name_exists = es.indices.exists(index_name)
    
    # if this is the first import or the import should be restarted (WARNING: will delete all indices data!!)
    if truncate_index or empty_index:
        
        if index_name_exists:
            if truncate_index:
                message = "Index '{}' will be forcefully deleted (truncate_index=True)".format(index_name)
            else:
                message = "Index '{}' is empty and will be recreated".format(index_name)
            print("    " + message)
            
            es.indices.delete(index_name)
            index_name_exists = False
    
    if not index_name_exists:
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


def index_csv_files(index_name, directory, truncate_index=False, max_csv_file_index_per_day=0, file_filters=None, sensor_ids_filter=None, max_bucket_size=100):
    """
    Indexes all csv files to the ELASTICSEARCH server.
    Also it will keep track of the most recent indexed file and continue on that progress.
    :param directory: str the directories where the csv files are stored
    :param truncate_index: boolean WARNING: if set to True it will delete all indices data!
    :param max_csv_file_index_per_day: int the amount of files which are indexed for each day (0=no limit)
    :param file_filters: list only index files with the matching string pattern
    :param sensor_ids_filter: list the file containing the list of sensor ids
    :param max_bucket_size: int the amount of files which are collected to be indexed (before they are actually being bulk indexed)
    """
    
    if file_filters is None:
        file_filters = []
    
    if sensor_ids_filter is None:
        sensor_ids_filter = []
        
        ###
    # check if any data has been previously imported
    ##
    message = "Continuing the indexing process"
    print(message)
    
    index_files_name = "{}_file_index".format(index_name)
    prepare_file_index(index_files_name, truncate_index)
    
    date_directories = glob.glob('%s/**' % directory)
    
    # order the date directories by the most recent first
    date_directories = sorted(date_directories, reverse=True)
    
    indexes_truncated = []
    
    for date_directory in date_directories:
        
        file_date = date_directory.split('/')[-1]
        
        # ignore files and directories not complying to the date structure
        if os.path.isfile(date_directory) or len(file_date.split('-')) != 3:
            continue
        
        last_imported_file_id = None
        
        # create a unique index for each month in the format YYYY-MM (2018-01)
        date_year_month = "-".join(file_date.split('-')[:2])
        index_data_name = "{}_{}".format(index_name, date_year_month)
        
        # truncate the indexes only once per run
        if index_data_name not in indexes_truncated:
            prepare_data_index(index_data_name, truncate_index)
            indexes_truncated.append(index_data_name)
        
        ##
        # get the id last imported file id of the imported directory
        # (to be able to return on the import where it was last)
        ##
        files_indexed_day_count = 0
        
        if es.indices.exists(index_files_name) and es.count(index_files_name).get('count') > 0:
            
            # get one result with the newest timestamp (descendant ordering)
            doc = {
                'query': dict(match=dict(file_date=file_date)),
                'sort': dict(timestamp=dict(order="desc")),
                'size': 1
            }
            
            items = es.search(index=index_files_name, doc_type="indexed", body=doc)
            
            hits = items.get('hits')
            
            imported_files = hits.get('total')
            
            if len(hits) > 0:
                records = hits.get('hits')
                
                # get the id of the last imported file
                if len(records) > 0:
                    message = "{} csv files for the date {} have already been imported".format(imported_files, file_date)
                    files_indexed_day_count = imported_files
                    print(message)
                    
                    last_imported_file_id = records[-1].get('_source').get('file_id')
                    message = "The last imported id for the date {} was {}".format(file_date, last_imported_file_id)
                    print(message)
        
        last_imported_id_found = False
        csv_files = glob.glob('%s%s/*.csv' % (directory, file_date))
        
        # order the files by the filename index
        csv_files = sorted(csv_files, key=lambda name: int(name.split('.')[-2].split('_')[-1]))
        
        bucket_records = []
        bucket_size = 0
        bucket_collection_data = []
        
        # iterate over each filter
        if file_filters == [] or len(file_filters) > 0:
            
            # iterate over all csv files and check
            for csv_file in csv_files:
                
                accept_file = True
                if file_filters:
                    accept_file = False
                    for file_filter in file_filters:
                        if csv_file.find(file_filter) > -1:
                            accept_file = True
                
                if sensor_ids_filter:
                    accept_file = False
                    
                    file_id = int(csv_file.split('.')[-2].split('_')[-1])
                    if file_id in sensor_ids_filter:
                        accept_file = True
                
                if accept_file:
                    
                    if max_csv_file_index_per_day == 0 or files_indexed_day_count < max_csv_file_index_per_day:
                        file_id = int(csv_file.split('.')[-2].split('_')[-1])
                        file_date = csv_file.split('.')[-2].split('_')[1]
                        
                        # create a unique index for each month in the format YYYY-MM (2018-01)
                        date_year_month = "-".join(file_date.split('-')[:2])
                        index_data_name = "{}_{}".format(index_name, date_year_month)
                        
                        if file_id == last_imported_file_id:
                            last_imported_id_found = True
                        elif last_imported_file_id is None or last_imported_id_found:
                            
                            if files_indexed_day_count > 0:
                                if max_csv_file_index_per_day > 0:
                                    message = '{}/{} (limited) files have been queued for indexing'.format(files_indexed_day_count, max_csv_file_index_per_day)
                                else:
                                    message = '{}/{} files have been queued for indexing'.format(files_indexed_day_count, len(csv_files))
                                print("    " + message)
                            
                            # cleanup the index:
                            # delete items related items towards the file_id and the file_date, if the previous indexing process was aborted
                            prepare_and_cleanup_index(index_data_name, file_id, file_date)
                            
                            # read multiple the csv files into a record list and then only index it
                            # when the bucket
                            bucket_records.extend(collect_csv_data(index_data_name, csv_file, file_id))
                            
                            bucket_collection_data.append({'file_id': file_id, 'file_date': file_date})
                            bucket_size += 1
                            files_indexed_day_count += 1
                
                # when the bucket is full, index
                if bucket_size > max_bucket_size:
                    message = "Indexing data of bucket list into index: {}".format(index_data_name)
                    print(" " + message)
                    index_csv_data(index_data_name, index_files_name, bucket_records, bucket_collection_data)
                    bucket_size = 0
                    print("")
        
        # when the bucket was filled, all files of the day considered
        if len(bucket_records) > 0:
            message = "Indexing data of bucket list into index: {}".format(index_data_name)
            print(" " + message)
            index_csv_data(index_data_name, index_files_name, bucket_records, bucket_collection_data)
            print("")
        
        message = "Files for day: {} have been indexed".format(file_date)
        print("    " + message)
        print("")


def download_and_index(index_name, max_csv_file_index_per_day, last_days, file_filters=None, sensor_ids_filter=None, truncate_index=False, download=True, index=True):
    # step 1. download the csv files for the sensors with the type containing the dust values
    if download:
        download_resources(target_url, data_directory, last_days=last_days, max_files_per_day=max_csv_file_index_per_day,
                           file_filters=file_filters, sensor_ids_filter=sensor_ids_filter)
    
    # step 3. index the csv files into elastic search
    if index:
        index_csv_files(index_name, data_directory, truncate_index=truncate_index, max_csv_file_index_per_day=max_csv_file_index_per_day, file_filters=file_filters, sensor_ids_filter=sensor_ids_filter)


def main():
    
    sensor_types = {
        'weather_conditions': [
            'dht22',  # values: temperature, humidity
            'bme280',  # values: temperature, pressure
            'bmp180',  # values:  temperature, pressure
            'bmp280'  # values:temperature, pressure
        ],
        'fine_dust_conditions': [
            'sds011',  # values: P1, P2
            'pms3003',  # values: P1, P2
            'hpm',  # values: P1, P2
            'ppd42ns',  # values: P1, P2, durP1, ratioP1, durP2, ratioP2
            'pms7003',  # values: P1, P2
            'pms5003'  # values: P1, P2
        ],
    }
    
    last_days = int(365.25 * 4)

    truncate_index = True
    max_index_count_per_day = 0
    
    # Sensor ids for the area of stuttgart south for the sensors with fine dust values:
    sensor_ids_filter = [219, 430, 549, 671, 673, 723, 751, 757, 1364, 2199, 2820, 8289]
    
    download_and_index("luftdaten_stuttgart_weather", max_index_count_per_day, last_days,
                       sensor_ids_filter=sensor_ids_filter,
                       truncate_index=truncate_index)
    # Sensor ids for the area of stuttgart south for the sensors with weather values:
    sensor_ids_filter = [431, 550, 672, 674, 724, 752, 758, 1365, 2200, 2821, 8290, 11462, 12323]
    
    download_and_index("luftdaten_stuttgart__fine_dust", max_index_count_per_day, last_days,
                       sensor_ids_filter=sensor_ids_filter,
                       truncate_index=truncate_index
                       )

    truncate_index = False
    # get the sensor data of a certain sensor type (of weather conditions) over the defined last days
    max_index_count_per_day = 100
    download_and_index("luftdaten_weather", max_index_count_per_day, last_days,
                       file_filters=[sensor_types.get('weather_conditions')[0]],
                       truncate_index=truncate_index)

    # get the sensor data of a certain sensor type (of fine dust conditions) over the defined last days
    download_and_index("luftdaten_fine_dust", max_index_count_per_day, last_days,
                       file_filters=[sensor_types.get('fine_dust_conditions')[0]],
                       truncate_index=truncate_index
                       )


if __name__ == "__main__":
    main()
