#!/usr/bin/env python

# -*- coding: utf-8 -*-

####
# fetch sensor_ids of specific geo locations and sensor types
###

__author__ = 'Martin Andreas Woerz'
__email__ = 'm.woerz@ieservices.de'
__copyright__ = "Copyright 2018, Martin Woerz"
__version__ = "0.0.7"

import os

import pandas as pd
from elasticsearch import Elasticsearch

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

es_index_name = 'luftdate_full_2018-05-07'
es_doc_type = "sensor_data"


def get_unique_sensor_ids_around_geo_location(geo_shape, filter_by_sensor_types=None):
    if filter_by_sensor_types is None:
        filter_by_sensor_types = []
    
    size = 1000
    search_query = {
        "size": size,
        "query": {"bool": {}},
        "aggs": {
            "unique_sensor_ids": {
                "terms": {
                    "field": "sensor_id"
                }
            }
        }
    }
    
    geo_data = {
        "geo_polygon": {
            "ignore_unmapped": True,
            "geo_location": {
                "points": geo_shape
            }
        }
    }
    
    search_query["query"]["bool"]["filter"] = geo_data
    
    if filter_by_sensor_types:
        search_query["query"]["bool"]["must"] = {
            "terms": {
                "sensor_type": filter_by_sensor_types
            }
        }
    
    # query the results and pass a param: scroll=1m
    response = es.search(index=es_index_name, doc_type=es_doc_type, body=search_query, params={'scroll': '1m'})
    
    # get the scroll id
    scroll_id = response.get('_scroll_id')
    total_results = response['hits']['total']
    
    scroll_size = total_results
    
    from pandasticsearch import Select
    df = Select.from_dict(response).to_pandas()
    
    results_fetched = size
    
    while scroll_size > 0:
        page = es.scroll(scroll_id=scroll_id, scroll='2m')
        
        # Update the scroll ID
        scroll_id = page.get('_scroll_id')
        
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        
        message = "Fetching {}/{} results ({}%)".format(results_fetched, total_results,
                                                        round((results_fetched / total_results) * 100, 2))
        print(message)
        
        # Do something with the obtained page
        df_page_next = Select.from_dict(page).to_pandas()
        
        df = pd.concat([df, df_page_next], ignore_index=True)
        
        results_fetched += scroll_size
        
        print("")
    
    # get the unique sensor_id
    df_sensor_ids = df['sensor_id'].unique()
    
    # sort the ids
    df_sensor_ids.sort()
    
    unique_sensor_ids = list(df_sensor_ids)
    
    return unique_sensor_ids


def main():
    geo_shapes = {
        "Stuttgart": {
            "south": [
                {"lat": 48.7645060889677, "lon": 9.160966873168947},
                {"lat": 48.769258228422665, "lon": 9.174184799194338},
                {"lat": 48.76546786777273, "lon": 9.180965423583986},
                {"lat": 48.75749020404446, "lon": 9.168004989624025}
            ],
            "west": [
                {"lat": 48.77106844897414, "lon": 9.151782989501955},
                {"lat": 48.77265233842162, "lon": 9.15538787841797},
                {"lat": 48.78068415138507, "lon": 9.164314270019533},
                {"lat": 48.783568502957735, "lon": 9.155559539794924},
                {"lat": 48.77615934438715, "lon": 9.148778915405275}
            ],
            "east": [
                {"lat": 48.785774071728454, "lon": 9.187574386596681},
                {"lat": 48.78990218352415, "lon": 9.194955825805666},
                {"lat": 48.7899587306429, "lon": 9.201908111572267},
                {"lat": 48.78718784688117, "lon": 9.201478958129885},
                {"lat": 48.782776736678855, "lon": 9.19735908508301},
                {"lat": 48.78192840180478, "lon": 9.190578460693361}
            ],
        }
    }
    
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
    
    def get_sensor_ids_from_area(city, region, sensor_types, sensor_type_str):
        geo_shape = geo_shapes.get(city).get(region)
        
        sensor_ids = get_unique_sensor_ids_around_geo_location(geo_shape, filter_by_sensor_types=sensor_types)
        
        message = 'Sensor ids for the area of {} {} for the sensors with {} values: {} ({} locations) '.format(city.casefold(), region, sensor_type_str, sensor_ids, len(sensor_ids))
        return sensor_ids, message
    
    messages = []
    sensor_ids, message = get_sensor_ids_from_area('stuttgart', 'south', sensor_types.get('fine_dust_conditions'), 'fine dust')
    messages.append(message)
    
    sensor_ids, message = get_sensor_ids_from_area('Stuttgart', 'south', sensor_types.get('weather_conditions'), 'weather')
    messages.append(message)
    
    sensor_ids, message = get_sensor_ids_from_area('Stuttgart', 'west', sensor_types.get('fine_dust_conditions'), 'fine dust')
    messages.append(message)
    
    sensor_ids, message = get_sensor_ids_from_area('Stuttgart', 'west', sensor_types.get('weather_conditions'), 'weather')
    messages.append(message)
    
    sensor_ids, message = get_sensor_ids_from_area('Stuttgart', 'east', sensor_types.get('fine_dust_conditions'), 'fine dust')
    messages.append(message)
    
    sensor_ids, message = get_sensor_ids_from_area('Stuttgart', 'east', sensor_types.get('weather_conditions'), 'weather')
    
    messages.append(message)
    
    for message in messages:
        print(message)


if __name__ == "__main__":
    main()
