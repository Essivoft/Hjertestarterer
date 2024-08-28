import os 
import time
import logging
import json
import csv
import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from ckanapi import RemoteCKAN
import creds

os.makedirs("113HertestartereStavangerData", exist_ok=True ) # Lagre data
os.makedirs("113HjertestartereStavangerError", exist_ok=True) # Lage errors
os.makedirs("113HertestartereStavangerData\\fileDicts", exist_ok=True) #Lagring for fileDicts senere når ressurser lages i CKAN

# Variables
timeString2= time.strftime('%Y-%m-%d')
timeString3 = time.strftime("%d-%B-%Y")
urlGetHjertestartereStavanger5KRadius = f"https://hjertestarterregister.113.no/ords/api/v1/assets/search/"
ckan_api_key = creds.ckan_api_key

#Oauth lib
client_id = creds.client_id
client_secret = creds.client_secret
token_url = "https://hjertestarterregister.113.no/ords/api/oauth/token"

# Get token for later queries
client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)
token = oauth.fetch_token(token_url=token_url, client_id=client_id, client_secret=client_secret)

#Set up logging
log_file = os.path.join('113HjertestartereStavangerError', f'113HjertestartereStavangerError.log')
logging.basicConfig(filename=log_file, level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

# Request headers for retrieving data from API
headers = {
    #"accept-ranges": "bytes",
    #"access-control-allow-headers": "Origin",
    #"access-control-allow-methods": "GET",
    #"access-control-allow-origin": "*",
    #"Content-Type": "application/json",
    #'User-Agent': 'Stavanger-kommune/Opencom.no',  # Use a unique identifier for your app here
    'Authorization': 'Bearer ' + token['access_token'],
    }

params = {
    "latitude" : "58.969759103857314",
    "longitude" : "5.731975285041405",
    "distance" : "70000",   # Radius from the given latitude and longitude in meters. 
    "date" : timeString3    # Date for the query in the format day(01)-Month(Jan)-Year(20XX)
}

try:
    sporring = requests.get(url = urlGetHjertestartereStavanger5KRadius, params=params, headers=headers)
    print (sporring)
    sporring.raise_for_status()
    data = sporring.json()
    with open(f"113HertestartereStavangerData\\113-Hjertestartere-Stavanger.json", "w", encoding='utf8') as outfile:
        json.dump(data, outfile, sort_keys=False, ensure_ascii=False, indent=4)
except Exception as e:
    logging.error(f"{timeString2}Error during request: {str(e)}")
    raise
finally:
    sporring.close()

# Clean up JSON file
with open("113HertestartereStavangerData\\113-Hjertestartere-Stavanger.json", 'r+', encoding="utf-8") as json_replace:
    data =json_replace.read()
    data = data.replace('"API_MESSAGE": "Request successful.",', '')
    data = data.replace('"API_CURRENT_USER_ID": 48952,', '')
    #data = data.replace("]", "")
    json_replace.seek(0)
    json_replace.write(data)
    json_replace.truncate()
    json_replace.close()

# Convert JSON to CSV
with open("113HertestartereStavangerData\\113-Hjertestartere-Stavanger.json", "r", encoding="utf-8") as json_to_CSV:
    data = json.load(json_to_CSV)
    
# Define columns
fieldnames = [
        'ASSET_ID',
        'SITE_LATITUDE',
        'SITE_LONGITUDE',
        'SITE_NAME',
        'SITE_ADDRESS',
        'SITE_FLOOR_NUMBER',
        'SITE_POST_CODE',
        'SITE_POST_AREA',
        'SITE_DISTANCE',
        'CREATED_DATE',
        'MODIFIED_DATE',
        'IS_OPEN',
        'IS_OPEN_DATE',
        'OPENING_HOURS_TEXT',
        'OPENING_HOURS_LIMITED',
        'OPENING_HOURS_MON_FROM',
        'OPENING_HOURS_MON_TO',
        'OPENING_HOURS_TUE_FROM',
        'OPENING_HOURS_TUE_TO',
        'OPENING_HOURS_WED_FROM',
        'OPENING_HOURS_WED_TO',
        'OPENING_HOURS_THU_FROM',
        'OPENING_HOURS_THU_TO',
        'OPENING_HOURS_FRI_FROM',
        'OPENING_HOURS_FRI_TO',
        'OPENING_HOURS_SAT_FROM',
        'OPENING_HOURS_SAT_TO',
        'OPENING_HOURS_SUN_FROM',
        'OPENING_HOURS_SUN_TO',
        'OPENING_HOURS_CLOSED_HOLIDAYS',
        'ACTIVE_DATE_LIMITED'
        ]

with open("113HertestartereStavangerData\\113-Hjertestartere-Stavanger.csv", "w", newline='', encoding="utf-8-sig") as csv_file:
    try:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write column headers to the CSV file
        writer.writeheader()
        # Loop through each ASSET and write data to the CSV file
        for asset in data['ASSETS']:
            row = {}
            for fieldname in fieldnames:
                # Check if the field exists in the ASSET object
                if fieldname in asset:
                    row[fieldname] = asset[fieldname]
                else:
                    row[fieldname] = ''
            writer.writerow(row)
    except Exception as e:
        logging.error(f"{timeString2}-Error JSON to CSV: {str(e)}")
        raise

# Convert CSV to GeoJSON
df = pd.read_csv("113HertestartereStavangerData\\113-Hjertestartere-Stavanger.csv")
geometry = [Point(xy) for xy in zip(df.SITE_LONGITUDE, df.SITE_LATITUDE)] # Make geometry from lat and long
gdf = gpd.GeoDataFrame(df, geometry=geometry)
gdf = gdf.set_crs('epsg:4326') # Set crs to WGS84

gdf.to_file("113HertestartereStavangerData\\113-Hjertestartere-Stavanger.geojson", driver="GeoJSON")

# Convert CSV to XLSX
with open("113HertestartereStavangerData\\113-Hjertestartere-Stavanger.csv", "r", encoding='utf-8') as csv_to_xlsx:
    try:
        df = pd.read_csv(csv_to_xlsx, encoding="utf-8-sig")
        df.to_excel("113HertestartereStavangerData\\113-Hjertestartere-Stavanger.xlsx", sheet_name=f'113-Hjertestartere-Stavanger', engine='xlsxwriter')
    except Exception as e:
        logging.error(f"{timeString2}-Error CSV to XLSX: {str(e)}")
        raise
    finally:
        csv_to_xlsx.close()

ua = 'ckanapiexample/1.0 (+https://opencom.no)'
mypackage_id = 'e54fb04a-af8c-4ef2-bc0b-8af01e3099af'
mysite = RemoteCKAN('https://opencom.no', apikey=ckan_api_key, user_agent=ua)

geojson_path = "113HertestartereStavangerData\\113HertestartereStavangerDataGEOJSON"
json_path = "113HertestartereStavangerData"
csv_path = "113HertestartereStavangerData"
xlsx_path = "113HertestartereStavangerData\\113HertestartereStavangerXLSX"

# Make empty dictionaries to store file names, file types and ids for each file
json_file_dict = {}
csv_file_dict = {}
xlsx_file_dict = {}
geojson_file_dict = {}

### BLOCK OF CODE FOR CREATING NEW DATASETS UNDER mypackage_id. COMMENT OUT UNLESS NEEDED TO CREATE NEW DATASETS. DOES NOT UPDATE ALREADY EXISTING DATASETS
# # Loop gjennom alle JSON-filer i mappen
# for file in os.listdir(json_path):
#     if file.endswith('.json'):
#         file_name, file_ext = os.path.splitext(file)
#         file_path = os.path.join(json_path, file)

#         # Last opp filen til CKAN
#         try:
#             result = mysite.action.resource_create(
#                 package_id=mypackage_id,
#                 name=file_name,
#                 format=file_ext[1:],
#                 upload=open(file_path, 'rb')
#             )
#             print(f'Successfully uploaded {file_name}{file_ext} with id: {result["id"]}')
#             json_file_dict[file_name+file_ext] = result["id"]
#         except Exception as e:
#             print(f'Error uploading {file_name}{file_ext}: {e}')
#             logging.error(f'{timeString2}-Error uploading JSON file to opencom.no: {str(e)}')
# # Skriv ordboken til filen "json_resource_dict.json"
# with open("113HertestartereStavangerData\\fileDicts\\json_resource_dict.json", 'w') as f:
#     json.dump(json_file_dict, f)

# # Loop gjennom alle CSV-filer i mappen
# for file in os.listdir(csv_path):
#     if file.endswith('.csv'):
#         file_name, file_ext = os.path.splitext(file)
#         file_path = os.path.join(csv_path, file)

#         # Last opp filen til CKAN
#         try:
#             result = mysite.action.resource_create(
#                 package_id=mypackage_id,
#                 name=f"Hjertestartere {file_name} Hjertestarterregisteret til 113.csv",
#                 format='csv',
#                 upload=open(file_path, 'rb')
#             )
#             print(f'Successfully uploaded {file_name} with id: {result["id"]}')
#             csv_file_dict[file_name+file_ext] = result["id"]
#         except Exception as e:
#             print(f'Error uploading {file_name}: {e}')
#             logging.error(f'{timeString2}-Error uploading CSV file to opencom.no: {str(e)}')
# # Skriv ordboken til filen "csv_resource_dict.json"
#     with open("113HertestartereStavangerData\\fileDicts\\csv_resource_dict.json", 'w') as f:
#      json.dump(csv_file_dict, f)

# # Loop gjennom alle XLSX-filer i mappen
# for file in os.listdir(xlsx_path):
#     if file.endswith('.xlsx'):
#         file_name, file_ext = os.path.splitext(file)
#         file_path = os.path.join(xlsx_path, file)

#         # Last opp filen til CKAN
#         try:
#             result = mysite.action.resource_create(
#                 package_id=mypackage_id,
#                 name=f"Hjertestartere {file_name} Hjertestarterregisteret til 113.xlsx",
#                 format='xlsx',
#                 upload=open(file_path, 'rb')
#             )
#             print(f'Successfully uploaded {file_name} with id: {result["id"]}')
#             xlsx_file_dict[file_name+file_ext] = result["id"]
#         except Exception as e:
#             print(f'Error uploading {file_name}: {e}')
#             logging.error(f'{timeString2}-Error uploading XLSX file to opencom.no: {str(e)}')
# # Skriv ordboken til filen "xlsx_resource_dict.json"
#     with open("113HertestartereStavangerData\\fileDicts\\xlsx_resource_dict.json", 'w') as f:
#      json.dump(xlsx_file_dict, f)

# # Loop gjennom alle GEOJSON-filer i mappen
# for file in os.listdir(geojson_path):
#     if file.endswith('.geojson'):
#         file_name, file_ext = os.path.splitext(file)
#         file_path = os.path.join(geojson_path, file)

#         # Last opp filen til CKAN
#         try:
#             result = mysite.action.resource_create(
#                 package_id=mypackage_id,
#                 name=f"Hjertestartere {file_name} Hjertestarterregisteret til 113.geojson",
#                 format='geojson',
#                 upload=open(file_path, 'rb')
#             )
#             print(f'Successfully uploaded {file_name} with id: {result["id"]}')
#             geojson_file_dict[file_name+file_ext] = result["id"]
#         except Exception as e:
#             print(f'Error uploading {file_name}: {e}')
#             logging.error(f'{timeString2}-Error uploading XLSX file to opencom.no: {str(e)}')
# # Skriv ordboken til filen "geojson_resource_dict.json"
#     with open("113HertestartereStavangerData\\fileDicts\\geojson_resource_dict.json", 'w') as f:
#      json.dump(geojson_file_dict, f)

# BLOCK OF CODE FOR UPDATING DATASETS IN CKAN. COMMENT OUT UNLESS NEEDED TO UPDATE DATASETS
# Reads from FILTYPE_file_dict, which is created when new resources are created in the code above, and updates the ids that are found there.
with open("113HertestartereStavangerData\\fileDicts\\json_resource_dict.json", 'r') as f:
    json_file_dict = json.load(f)

for file_name, resource_id in json_file_dict.items():
    try:
        file_path = os.path.join(json_path, file_name)
        result = mysite.action.resource_update(
            id=resource_id,
            format='json',
            name=f"Hjertestartere {file_name} Hjertestarterregisteret til 113",
            upload=open(file_path, 'rb')
        )
        print(f'Successfully updated resource with id: {result["id"]}')
    except Exception as e:
        print(f'Error updating resource with id {resource_id}: {e}')
        logging.error(f'{timeString2}-Error updating resource with id {resource_id}: {str(e)}')

#CSV files
with open("113HertestartereStavangerData\\fileDicts\\csv_resource_dict.json", 'r') as f:
    json_file_dict = json.load(f)

for file_name, resource_id in json_file_dict.items():
    try:
        file_path = os.path.join(json_path, file_name)
        result = mysite.action.resource_update(
            id=resource_id,
            format='csv',
            name=f"Hjertestartere {file_name} Hjertestarterregisteret til 113",
            upload=open(file_path, 'rb')
        )
        print(f'Successfully updated resource with id: {result["id"]}')
    except Exception as e:
        print(f'Error updating resource with id {resource_id}: {e}')
        logging.error(f'{timeString2}-Error updating resource with id {resource_id}: {str(e)}')

#XLSX files
with open("113HertestartereStavangerData\\fileDicts\\xlsx_resource_dict.json", 'r') as f:
    json_file_dict = json.load(f)

for file_name, resource_id in json_file_dict.items():
    try:
        file_path = os.path.join(json_path, file_name)
        result = mysite.action.resource_update(
            id=resource_id,
            format='xlsx',
            name=f"Hjertestartere {file_name} Hjertestarterregisteret til 113",
            upload=open(file_path, 'rb')
        )
        print(f'Successfully updated resource with id: {result["id"]}')
    except Exception as e:
        print(f'Error updating resource with id {resource_id}: {e}')
        logging.error(f'{timeString2}-Error updating resource with id {resource_id}: {str(e)}')

#GEOJSON files
with open("113HertestartereStavangerData\\fileDicts\\geojson_resource_dict.json", 'r') as f:
    json_file_dict = json.load(f)

for file_name, resource_id in json_file_dict.items():
    try:
        file_path = os.path.join(json_path, file_name)
        result = mysite.action.resource_update(
            id=resource_id,
            format='geojson',
            name=f"Hjertestartere {file_name} Hjertestarterregisteret til 113",
            upload=open(file_path, 'rb')
        )
        print(f'Successfully updated resource with id: {result["id"]}')
    except Exception as e:
        print(f'Error updating resource with id {resource_id}: {e}')
        logging.error(f'{timeString2}-Error updating resource with id {resource_id}: {str(e)}')