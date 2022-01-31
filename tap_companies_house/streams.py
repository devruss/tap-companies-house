STREAMS = {
    'basic_company_data': {
        'key_properties': [' CompanyNumber'],
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['basic_company_data_date'],
        'data_from': 'bulk',
        'url': 'http://download.companieshouse.gov.uk/en_output.html'
    },
    "company_officers": {
        "key_properties": ["company_number", "name"],
        "replication_method": "FULL_TABLE",
        "replication_keys": ["Company_officers_date"],
        'data_from': 'api'
    },
    "people_with_significant_control": {
        "key_properties": ["etag"],
        "replication_method": "FULL_TABLE",
        "replication_keys": ["people_with_significant_control_date"],
        'data_from': 'bulk',
        'url': 'http://download.companieshouse.gov.uk/en_pscdata.html'
    }
}