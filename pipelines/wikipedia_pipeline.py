import json
from bs4 import BeautifulSoup
import requests
import pandas as pd
import geocoder
NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url):
    print("Getting Wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Check if the request is successful
        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")

def get_wikipedia_data(html):
    soup = BeautifulSoup(html, features='html.parser')
    tables = soup.find_all("table", {"class": "sortable wikitable"})
    all_rows = []

    for table in tables:
        table_rows = table.find_all('tr')
        all_rows.extend(table_rows)

    return all_rows

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if ' *' in text:
        text = text.split(' *')[0]
    if '[' in text:
        text = text.split('[')[0]
    return text.replace('\n', '').replace('*', '')

def extract_wikipedia_data(**kwargs):
    url = kwargs.get('url')
    html = get_wikipedia_page(url)
    if not html:
        return "Failed to retrieve page."

    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table', {'class': 'wikitable'})

    data = []
    overall_rank = 1  # Initialize the global rank counter

    for table in tables:
        rows = table.find_all('tr')
        for i in range(1, len(rows)):  # Start from 1 to skip header
            tds = rows[i].find_all('td')
            values = {
                'rank': overall_rank,
                'stadium': clean_text(tds[0].text) if len(tds) > 0 else 'N/A',
                'capacity': clean_text(tds[1].text).replace(',', '').replace('.', '') if len(tds) > 1 else '0',
                'city': clean_text(tds[2].text) if len(tds) > 2 else 'N/A',
                'country': clean_text(tds[3].text) if len(tds) > 3 else 'N/A',
                'region': clean_text(tds[4].text) if len(tds) > 4 else 'N/A',
                'tenants': clean_text(tds[5].text) if len(tds) > 5 else 'N/A',
                'sports': clean_text(tds[6].text) if len(tds) > 6 else 'N/A',
                'image': 'https://' + tds[7].find('img').get('src').split("//")[1] if len(tds) > 7 and tds[7].find(
                    'img') else 'NO_IMAGE'
            }

            # Append to data list
            data.append(values)
            overall_rank += 1  # Increment the rank counter

    # Convert data list to JSON and push to XCom
    json_rows = json.dumps(data, indent=2)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "OK"

def get_lat_long(country, city):
    location = geocoder.arcgis(f'{city}, {country}')

    if location and location.latlng:
        return location.latlng[0], location.latlng[1]

    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['image'] = stadiums_df['image'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"

def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    # data.to_csv('data/' + file_name, index=False)
    data.to_csv('abfs://stadiumsdataeng@stadiumsdataengproject.dfs.core.windows.net/data/' + file_name,
              storage_options={
                  'account_key': 'AZURE_STORAGE_KEY'
              }, index=False)
