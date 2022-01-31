import os
import time
import requests
import singer
from singer import utils
import tqdm

LOGGER = singer.get_logger()

class CompaniesHouseClient:
    def __init__(self, config):
        self.baseUrl = "https://api.companieshouse.gov.uk/"
        self.apiKey = config['api_key']
        self.company_information = ['registered_office_address', 'officers', 'filing-history', 'insolvency',
                               'charges', 'uk-establishments', 'persons-with-significant-control',
                               'persons-with-significant-control-statements', 'registers', 'exemptions']
        self.session = requests.Session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    @utils.ratelimit(600, 300)
    def request(self, **kwargs):

        retries = 1
        success = False
        while not success:
            try:
                if kwargs['api'] == 'search_companies':
                    url = '{}{}'.format(self.baseUrl, 'search/companies')
                    q = kwargs['q']
                    response = self.session.get(url,
                                       headers={"content-type": "application/json", "Authorization": self.apiKey},
                                       params={'q': q})
                elif kwargs['api'] == 'profile':
                    company_number = kwargs['company_number']
                    url = '{}{}/{}'.format(self.baseUrl, 'company', company_number)
                    response = self.session.get(url,
                                        headers={"content-type": "application/json", "Authorization": self.apiKey})

                elif kwargs['api'] in self.company_information:
                    company_number = kwargs['company_number']
                    url = '{}{}/{}/{}'.format(self.baseUrl, 'company', company_number, kwargs['api'])
                    response = self.session.get(url,
                                        headers={"content-type": "application/json", "Authorization": self.apiKey})

                success = True
            except:
                LOGGER.info("ConnectionError")
                time.sleep(retries * 30)
                retries += 1

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            response = None
            return response

    def get_abs_path(self, path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def download_file(self, url, filename, verbose=False, file_size=None):
        """ Download file with progressbar """
        local_filename = self.get_abs_path(filename)
        success = False
        while not success:
            try:
                r = self.session.get(url, stream=True)
                if r.status_code != 200:
                    success = False
                    LOGGER.info("Response status code is not 200 while downloading the file, trying again")

                elif r.status_code == 200:
                    if not file_size:
                        file_size = int(r.headers['Content-Length'])
                    chunk = 1
                    chunk_size = 1024
                    num_bars = int(file_size / chunk_size)
                    if verbose:
                        LOGGER.info(dict(file_size=file_size))
                        LOGGER.info(dict(num_bars=num_bars))

                    with open(local_filename, 'wb') as fp:
                        for chunk in tqdm.tqdm(
                                r.iter_content(chunk_size=chunk_size)
                                , total=num_bars
                                , unit='KB'
                                , desc=local_filename
                                , leave=True  # progressbar stays
                        ):
                            fp.write(chunk)
                    success = True
                    return True
            except Exception as e:
                print(e)
                success = True
                LOGGER.info("Exception has occured while downloading the file, trying again")
        return False