import json
import re
from zipfile import ZipFile
import tqdm
import os.path
import pandas as pd
import singer
from singer import write_state
import requests
from datetime import datetime, timedelta, date
from tap_companies_house.streams import STREAMS
from tap_companies_house.schema import get_abs_path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.utils import ChromeType

LOGGER = singer.get_logger()

def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err

def download_file(url, filename=False, verbose=False):
    """ Download file with progressbar """
    local_filename = get_abs_path(filename)
    success = False
    while not success:
        try:
            r = requests.get(url, stream=True)
            if r.status_code != 200:
                success = False
                LOGGER.info("Response status code is not 200 while downloading the file, trying again")

            elif r.status_code == 200:
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
                            , leave=False  # progressbar stays
                    ):
                        fp.write(chunk)
                success = True
                return True
        except:
            success = False
            LOGGER.info("Exception has occured while downloading the file, trying again")
    return False

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def fix_date(json_data):
    date_list = ["DissolutionDate", "IncorporationDate", "Accounts.NextDueDate", "Accounts.LastMadeUpDate",
                 "Returns.NextDueDate", "Returns.LastMadeUpDate", "PreviousName_1.CONDATE", " PreviousName_2.CONDATE",
                 "PreviousName_3.CONDATE", "PreviousName_4.CONDATE", "PreviousName_5.CONDATE", "PreviousName_6.CONDATE",
                 "PreviousName_7.CONDATE", "PreviousName_8.CONDATE", "PreviousName_9.CONDATE",
                 "PreviousName_10.CONDATE", "ConfStmtNextDueDate", " ConfStmtLastMadeUpDate"]

    for comp in json_data:
        for prop in comp:
            if prop in date_list:
                if comp[prop] is not None:
                    datetime_object = datetime.strptime(comp[prop], '%d/%m/%Y')
                    comp[prop] = str(datetime_object.date())
    return json_data

# def getCompanyNumber(json_data, sicCodes):
#     companyNumber = []
#     for company in json_data:
#         if check_splcharacter(company["SICCode.SicText_1"], sicCodes):
#             companyNumber.append(company[" CompanyNumber"])
#     return companyNumber

def get_company_number(json_data, sic_codes, reg_date):
    companyNumber = []
    for company in json_data:
        if company["SICCode.SicText_1"] in sic_codes and (datetime.strptime(company["IncorporationDate"], '%Y-%m-%d').date())> reg_date:
            companyNumber.append(company[" CompanyNumber"])
    return companyNumber

def check_splcharacter(string, sicCodes):
    for sicCode in sicCodes:
        string_check = re.compile(sicCode)
        if (string_check.search(string) == None):
            return False
        else:
            return True

def get_download_urls(stream_name, endpoint_config):
    service = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(executable_path=service.path, chrome_options=chrome_options)

    url = endpoint_config['url']
    LOGGER.info("Processing site: {}".format(url))
    driver.get(url)
    download_urls = []
    if stream_name == 'basic_company_data':
        elements_filenames = driver.find_elements(By.XPATH, '/html/body/div/div[2]/div[2]/ul[2]')
        for elements_filename in elements_filenames:
            download_filenames = (elements_filename.text).split("\n")
            for download_filename in download_filenames:
                elements_download_filenames = driver.find_elements(By.PARTIAL_LINK_TEXT, download_filename)
                for el in elements_download_filenames:
                    link = el.get_attribute("href")
                    download_urls.append(link)
    elif stream_name == 'people_with_significant_control':
        elements_filenames = driver.find_elements(By.XPATH, '//*[@id="mainContent"]/div[2]/ul[2]')
        for elements_filename in elements_filenames:
            download_filenames = (elements_filename.text).split("\n")
            for download_filename in download_filenames:
                elements_download_filenames = driver.find_elements(By.PARTIAL_LINK_TEXT, download_filename)
                for el in elements_download_filenames:
                    link = el.get_attribute("href")
                    download_urls.append(link)
    driver.quit()
    return download_urls

def get_basic_company_data(download_url):
    json_data = []
    folder = get_abs_path("files/")
    zip_filename = os.path.basename(download_url)
    zip_file_path = get_abs_path(folder + zip_filename)
    download_file(download_url, filename=zip_file_path)
    with ZipFile(zip_file_path, 'r') as zipObj:
        file_names = zipObj.namelist()
        for file_name in file_names:
            if file_name.endswith('.csv'):
                zipObj.extractall(folder)
    os.remove(zip_file_path)
    file_path = get_abs_path(folder + file_name)
    df = pd.read_csv(file_path, sep=",", header=0, index_col=False, error_bad_lines=False, dtype='unicode')
    os.remove(file_path)
    json_str = df.to_json(orient="records")
    del df
    json_data = json.loads(json_str)
    return json_data

def get_people_with_significant_control_data(download_url):
    folder = get_abs_path("files/")
    zip_filename = os.path.basename(download_url)
    zip_file_path = get_abs_path(folder + zip_filename)
    download_file(download_url, filename=zip_file_path)
    with ZipFile(zip_file_path, 'r') as zipObj:
        file_names = zipObj.namelist()
        for file_name in file_names:
            if file_name.endswith('.txt'):
                zipObj.extractall(folder)
    os.remove(zip_file_path)
    file_path = get_abs_path(folder + file_name)
    file = open(file_path, "r", encoding="utf8")
    pwsc_data = file.readlines()
    del file
    companies_pwsc_data = []
    for data in pwsc_data:
        company_data = json.loads(data)
        company_data.update(company_data["data"])
        company_data.pop("data")
        try:
            if company_data["etag"]:
                companies_pwsc_data.append(company_data)
        except KeyError:
            LOGGER.error('KeyError')
    del pwsc_data
    return companies_pwsc_data

def get_collected_company_numbers(stream_name, endpoint_config, sic_codes, reg_date):
    collected_company_numbers = []
    download_urls = get_download_urls(stream_name, endpoint_config)
    for download_url in download_urls:
        records = get_basic_company_data(download_url)
        if records:
            ## Collect company numbers to retrieve company officer data
            records = fix_date(records)
            if sic_codes:
                LOGGER.info('Collecting company numbers for api request')
                company_numbers = get_company_number(records, sic_codes, reg_date)
                collected_company_numbers = collected_company_numbers + company_numbers
            del records
    return collected_company_numbers

def sync(client, config, catalog, state):
    current_date = str(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"))
    value = {"last_updated": current_date}
    collected_company_numbers = []
    sic_codes = config.get('sic_codes', [])
    reg_date = datetime.strptime(config["start_date"], "%Y-%m-%d").date()

    # TODO: control RAM usage
    # import psutil
    # ram_usage_percent = psutil.virtual_memory().percent

    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    for stream_name, endpoint_config in STREAMS.items():
        if stream_name in selected_streams:
            if endpoint_config['data_from'] == 'bulk':
                LOGGER.info('Extracting: {}'.format(stream_name))
                if stream_name == 'basic_company_data':
                    write_schema(catalog, stream_name)
                    download_urls = get_download_urls(stream_name, endpoint_config)
                    for download_url in download_urls:
                        records = get_basic_company_data(download_url)
                        if records:
                            singer.write_records(stream_name, records)
                            ## Collect company numbers to retrieve company officer data
                            records = fix_date(records)
                            if sic_codes:
                                LOGGER.info('Collecting company numbers for api request')
                                company_numbers = get_company_number(records, sic_codes, reg_date)
                                collected_company_numbers = collected_company_numbers + company_numbers
                            del records
                elif stream_name == 'people_with_significant_control':
                    write_schema(catalog, stream_name)
                    download_urls = get_download_urls(stream_name, endpoint_config)
                    for download_url in download_urls:
                        records = get_people_with_significant_control_data(download_url)
                        if records:
                            singer.write_records(stream_name, records)
            elif endpoint_config['data_from'] == 'api':
                LOGGER.info('Extracting: {}'.format(stream_name))
                if len(collected_company_numbers) == 0:
                    collected_company_numbers = get_collected_company_numbers('basic_company_data',
                                                                              STREAMS['basic_company_data'],
                                                                              sic_codes, reg_date)
                LOGGER.info('Number of companies: {}'.format(len(collected_company_numbers)))
                # companyNumbers = ['FC029478','12408591']
                if stream_name == 'company_officers':
                    write_schema(catalog, stream_name)
                    for company_number in collected_company_numbers:
                        company_officers_records = []
                        LOGGER.info("Extracting data for company: '{}'".format(str(company_number)))
                        datas = client.request(api='officers', company_number=company_number)
                        if datas:
                            for data in datas["items"]:
                                data.update({"company_number": str(company_number)})
                                company_officers_records.append(data)
                        if company_officers_records:
                            singer.write_records(stream_name, company_officers_records)
    write_state(value)
    LOGGER.info('Finished extracting data')


    # for stream_name, endpoint_config in STREAMS.items():
    #     LOGGER.info(stream_name + ' - extracting')
    #
    #     # # DOWNLOAD CSV DATA (Company basic information and People with significant control)
    #     url = 'http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-'
    #
    #     dataFolder = 'data/'
    #     start_date = date(2020, 4, 29)
    #     end_date = date.today() + timedelta(days=1)
    #     for single_date in daterange(start_date, end_date):
    #         urlName = url + single_date.strftime("%Y-%m-%d") + '.zip'
    #         filePath = dataFolder + os.path.basename(urlName)
    #         if download_file(urlName, filename=filePath):
    #             break
    #
    #     #zip_path = 'data/BasicCompanyDataAsOneFile-2020-04-01.zip'
    #     with ZipFile(filePath, 'r') as zipObj:
    #         listOfFileNames = zipObj.namelist()
    #         for fileName in listOfFileNames:
    #             if fileName.endswith('.csv'):
    #                 zipObj.extractall(dataFolder)
    #
    #     df = pd.read_csv(dataFolder + fileName, sep=",", header=0, index_col=False)
    #     jsonFileName = 'Company_basic_information.json'
    #     ##jsonFileName = 'BasicCompanyData-2020-04-01-part1_6.json'
    #     #jsonFileName = 'CSV_bulk_data.json'
    #     df.to_json(dataFolder + jsonFileName, orient="records", date_format="epoch", double_precision=10, force_ascii=True,
    #                            date_unit="ms", default_handler=None)
    #     del df
    #     with open(dataFolder + jsonFileName) as json_file:
    #         json_data = json.load(json_file)
    #     LOGGER.info(' [ Fixing date ... ] ')
    #     json_data = fixDate(json_data)
    #     LOGGER.info(' [ Writing schema and records ... ] ')
    #     write_schema(catalog, stream_name)
    #     singer.write_records(stream_name, json_data)


        # with open(dataFolder + jsonFileName) as json_file:
        #     json_data = json.load(json_file)
        # LOGGER.info(' [ Fixing date ... ] ')
        # json_data = fixDate(json_data)
        # sicCode = ['64209 - Activities of other holding companies n.e.c.',
        #            '64303 - Activities of venture and development capital companies',
        #            '64304 - Activities of open-ended investment companies',
        #            '64999 - Financial intermediation not elsewhere classified',
        #            '66190 - Activities auxiliary to financial intermediation n.e.c.',
        #            '66300 - Fund management activities',
        #            '70221 - Financial management',
        #            '82990 - Other business support service activities n.e.c.']
        #
        # regDate = datetime(2020, 1, 1).date()
        # LOGGER.info(' [ Getting company numbers ... ] ')
        # companyNumbers = getCompanyNumber(json_data, sicCode, regDate)
        # del json_data
        # LOGGER.info('Number of companies: {}'.format(len(companyNumbers)))
        # LOGGER.info(' [ Extracting company officiers data ... ] ')
        # companyOfficersData = []
        # #companyNumbers = ['FC029478','12408591']
        # for companyNumber in companyNumbers:
        #     LOGGER.info(' {} - extracting '.format(str(companyNumber)))
        #     datas = client.request(api='officers', company_number=companyNumber)
        #     if datas:
        #         for data in datas["items"]:
        #             data.update({"company_number": str(companyNumber)})
        #             companyOfficersData.append(data)
        #
        # LOGGER.info(' [ Writing schema and records ... ] ')
        # write_schema(catalog, stream_name)
        # singer.write_records(stream_name, companyOfficersData)
        # #del json_data




        # # PWSC txt data
        #         # url = 'http://download.companieshouse.gov.uk/persons-with-significant-control-snapshot-'
        #         # start_date = date(2020, 3, 25)
        #         # end_date = date.today() + timedelta(days=1)
        #         # for single_date in daterange(start_date, end_date):
        #         #     urlName = url + single_date.strftime("%Y-%m-%d") + '.zip'
        #         #     filePath = dataFolder + os.path.basename(urlName)
        #         #     if download_file(urlName, filename=filePath):
        #         #         break
        #         #
        #         # #zip_path = 'data/persons-with-significant-control-snapshot-2020-04-28.zip'
        #         # with ZipFile(filePath, 'r') as zipObj:
        #         #     listOfFileNames = zipObj.namelist()
        #         #     for fileName in listOfFileNames:
        #         #         if fileName.endswith('.txt'):
        #         #             zipObj.extractall(dataFolder)

        # fileName = 'persons-with-significant-control-snapshot-2020-04-28.txt'
        # data_folder = 'data/'
        # pwsc_data_path = data_folder + fileName
        # file = open(pwsc_data_path, "r", encoding="utf8")
        # pwsc_data = file.readlines()
        # del file
        # companies_data = []
        # for data in pwsc_data:
        #     company_data = json.loads(data)
        #     company_data.update(company_data["data"])
        #     company_data.pop("data")
        #     try:
        #         if company_data["etag"]:
        #             companies_data.append(company_data)
        #     except KeyError:
        #         LOGGER.info('KeyError')
        # del pwsc_data
        #
        # LOGGER.info('Writing schema and records')
        # write_schema(catalog, stream_name)
        # singer.write_records(stream_name, companies_data)
        # del companies_data

        # q = 'CARNIVAL PLC'
        # companiesData = client.request(api='search_companies', q=q)
        # #data11 = data1["items"]
        #
        # companyData = []
        # for item in companiesData["items"]:
        #     if item["title"] == q:
        #         #companyData.append(item)
        #         companyData = item
        #         break


        # LOGGER.info(stream_name + ' - extracting')
        # #company_number = companyData["company_number"]
        # company_number = '00617987'
        # companyProfileData = client.request(api='profile', company_number=company_number)
        # write_schema(catalog, stream_name)
        # singer.write_records(stream_name, [companyProfileData])

    # companyOfficersData = client.request(api='officers', company_number=company_number)
    # print()

