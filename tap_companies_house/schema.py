import os
import json
from singer import metadata
from tap_companies_house.streams import STREAMS
import requests
import singer
import time

LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas(config):
    schemas = {}
    field_metadata = {}
    selected_all = config.get('selected_all', False)
    selected_streams = config.get('selected_streams', [])

    LOGGER.info('Building catalog')
    for stream_name, stream_metadata in STREAMS.items():
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)
        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.get('key_properties', None),
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )
        if selected_all:
            for m in mdata:
                m['metadata']['selected'] = True
        elif selected_streams:
            if stream_name in selected_streams:
                for m in mdata:
                    m['metadata']['selected'] = True
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
