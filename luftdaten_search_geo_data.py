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

import os
from datetime import datetime

from elasticsearch import Elasticsearch

target_url = "http://archive.luftdaten.info/"
data_directory = 'data/luftdaten'

# establishes the connection to the Elastic Search server
ELASTICSEARCH_HOST = os.environ.get("ELASTICSEARCH_HOST") if 'ELASTICSEARCH_HOST' in os.environ else "localhost"
ELASTICSEARCH_PORT = os.environ.get("ELASTICSEARCH_PORT") if 'ELASTICSEARCH_PORT' in os.environ else "9200"
ELASTICSEARCH_USERNAME = os.environ.get("ELASTICSEARCH_USERNAME") if 'ELASTICSEARCH_USERNAME' in os.environ else ""
ELASTICSEARCH_PASSWORD = os.environ.get("ELASTICSEARCH_PASSWORD") if 'ELASTICSEARCH_PASSWORD' in os.environ else ""

# init ElasticSearch
http_auth = ()

if ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD:
    http_auth = (ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)

es = Elasticsearch('http://%s:%s/' % (ELASTICSEARCH_HOST, ELASTICSEARCH_PORT), http_auth=http_auth)
es_doc_type = "sensor_data"

index_name = "luftdaten"


def get_geo_data(latitude, longitude, distance_in_km, limit=100, page=0):
    distance = "{}km".format(float(distance_in_km))
    
    search_params = {
        "bool": {
            "must": {"match_all": {}},
            "filter": {
                "geo_distance": {
                    "distance": distance,
                    "location": {"lat": latitude, "lon": longitude}
                }
            }
        }
    }
    
    search_query = {
        "query": search_params,
        "size": limit,
        'sort': {'timestamp': {'order': "desc"}},
        "from": page * limit,
    }
    
    response = es.search(index=index_name, doc_type=es_doc_type, body=search_query)
    total_results = response.get('hits').get('total')
    pages = int(total_results / limit)
    message = "{} results ({} pages) have been found".format(total_results, pages)
    
    print(message)
    
    return response.get('hits').get('hits')


def get_locations():
    search_query = {
        "aggs": {
            "geo_locations": {
                "terms": {"field": "location"}
            }
        }
    }
    
    response = es.search(index=index_name, doc_type=es_doc_type, body=search_query)
    
    locations = response.get('aggregations').get('geo_locations').get('buckets')
    
    message = "{} locations with sensor data found".format(len(locations))
    print(message)
    
    return locations


def get_locations_nearby(latitude, longitude, distance_in_km, limit=100, page=0):
    """
    
    :param latitude:
    :param longitude:
    :param distance_in_km:
    :param limit:
    :param page:
    :return:
    """
    
    distance = "{}km".format(float(distance_in_km))
    
    search_params = {
        "bool": {
            "must": {"match_all": {}},
            "filter": {
                "geo_distance": {
                    "distance": distance,
                    "geo_location": {"lat": latitude, "lon": longitude}
                }
            }
        }
    }
    
    search_query = {
        "query": search_params,
        "size": limit,
        'sort': {'timestamp': {'order': "desc"}},
        "from": page * limit,
        "aggs": {
            "locations": {
                "terms": {"field": "location"}
            }
        }
    }
    
    response = es.search(index=index_name, doc_type=es_doc_type, body=search_query)
    
    locations = response.get('aggregations').get('locations').get('buckets')
    
    message = "{} locations with sensor data found {} near ({}, {}) ".format(len(locations), distance_in_km, latitude, longitude)
    print(message)
    
    print([location.get('key') for location in locations])
    
    return locations


def get_sensor_data(location, limit=1000, page=0):
    search_query = {
        "query": {"match": {"location": location}},
        "size": limit,
        'sort': {'timestamp': {'order': "desc"}},
        "from": page * limit,
        "aggs": {
            "days": {
                "date_histogram": {
                    "field": "timestamp",
                    "interval": "1d"
                }
            }
        }
    }
    
    response = es.search(index=index_name, doc_type=es_doc_type, body=search_query)
    
    results = response.get('hits').get('hits')
    
    message = "{} sensor items found for location: {}".format(len(results), response.get('hits').get('total'))
    print(message)
    
    sensor_data_dates = response.get('aggregations').get('days').get('buckets')
    
    dates = [{'date': datetime.fromtimestamp(sensor_data_date.get('key')/1000),
              'doc_count': sensor_data_date.get('doc_count')} for sensor_data_date in sensor_data_dates]
    
    dates_list = "\n".join(["{}\t{} items".format(date_item.get('date').strftime('%Y-%m-%d'), date_item.get('doc_count')) for date_item in dates])
    message = "Also sensor data for the days found:\n{}".format(dates_list)
    print(message)
    
    return [result.get('_source') for result in results]


def main():
    # get the geo data around a certain point (here Stuttgart)
    latitude = 48.76490
    longitude = 9.168818
    distance_in_km = 1
    
    # get all locations
    get_locations()
    
    results = get_locations_nearby(latitude=latitude, longitude=longitude, distance_in_km=distance_in_km)
    
    if len(results) > 0:
        location = results[0]
        get_sensor_data(location.get('key'))
    
    # results = get_geo_data(latitude=latitude, longitude=longitude, distance_in_km=distance_in_km)


if __name__ == "__main__":
    main()
