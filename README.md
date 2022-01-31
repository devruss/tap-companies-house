# tap-companies-house

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singerspec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Companies House API](https://developer-specs.company-information.service.gov.uk/guides/index) or [Companies House Bulk data](http://download.companieshouse.gov.uk/en_output.html)
- Supports the following modes:
    - discover
    - catalog
- The code was tested with Python `3.7.7`
- Extracts the following data:
    - BULK [basic_company_data](http://download.companieshouse.gov.uk/en_output.html) (archived csv format data)
    - API [company_officers](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/officers/list) (filtering by company sic code and registration date)
    - BULK [people_with_significant_control](http://download.companieshouse.gov.uk/en_output.html) (archived json lines in txt format)

## Quick Start

1. Install
    
    Create new virtual environment and activate it:
    ```
    > python3 -m venv ~/.virtualenvs/tap-companies-house
    > source ~/.virtualenvs/tap-companies-house/bin/activate
    ```

    Clone this repository, and then install it.
    ```
   > git clone https://github.com/RFAInc/tap-companies-house.git
   > cd tap-companies-house
   > python install setup.py
   > deactivate
    ```
    
2. Create `config.json` file for tap and put the following.
    ```
    {
      "api_key": "************************************",
      "sic_codes": ["64209 - Activities of other holding companies n.e.c.",
        "64303 - Activities of venture and development capital companies",
        "64304 - Activities of open-ended investment companies",
        "64999 - Financial intermediation not elsewhere classified",
        "66190 - Activities auxiliary to financial intermediation n.e.c.",
        "66300 - Fund management activities",
        "70221 - Financial management",
        "82990 - Other business support service activities n.e.c."],
      "selected_all": true,
      "selected_streams": ["basic_company_data", "company_officers", "people_with_significant_control"],
      "start_date": "2021-01-01"
    }
    ```
    Full list of options in `config.json`:
    
    | Property                            | Type    | Required?  | Description                                                   |
    |-------------------------------------|---------|------------|---------------------------------------------------------------|
    | api_key                             | String  | Yes        | Companies House API key. The Companies House API uses HTTP basic access authentication to send an API key.      |
    | sic_codes                           | Array(String)| Yes   | Provide sic codes to filter companies. The company officers data will be extracted only for those filtered companies.       |
    | selected_all                        | Boolean | No         | By default False. This property allows to select all tables with all fields when a catalog is generated in a discover mode.       |
    | selected_streams                    | Array(String)| Yes   | If you want to select only specific streams, do "selected_all": False and put stream names in array that you want to extract in selected_streams.       |
    | start_date                          | String  | Yes        | Extracts only companies that were registered after this date for company_officers stream. Must be in format 'YYYY-mm-dd'.       |

3. Run the tap in discovery mode to generate a `catalog.json`:
	```
    ~/.virtualenvs/tap-tap-companies-house/bin/tap-companies-house --config config.json --discover > catalog.json
    ```

4. Run the tap with catalog file, extract data from Companies House API or Bulk data and load it with target:
	```
    ~/.virtualenvs/tap-companies-house/bin/tap-companies-house --config config.json --catalog catalog.json | singer-tap --config target_config.json > state.json.tmp
    ```

