from PIL import Image, TiffImagePlugin
from PIL.ExifTags import TAGS, GPSTAGS
import os
import pandas as pd
from datetime import datetime
from meteostat import Point, Daily
from datetime import datetime
import matplotlib.pyplot as plt
import csv
from geopy.geocoders import Nominatim
import random

import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error


#extract all exif data
def exif_data_extract(filename):
    phoneImage = Image.open(filename)
    exif = {}
    if phoneImage._getexif() != None:
        for key,value in phoneImage._getexif().items():
            index = TAGS.get(key)

            if index == "GPSInfo":
                exif[index] = gpsDictGPSInformation(value)
            else:
                exif[index] = value
        return exif
    return None

#specifically extract Geolocation Data
def gpsDictGPSInformation(value):
    gpsDict = {}
    for key, value in value.items():
        index = GPSTAGS.get(key)
        gpsDict[index] = value
    return gpsDict

def getDecimalCoordinates(exifTable):
    if 'GPSInfo' in exifTable and exifTable['GPSInfo'] != None:
        #latitude extract
        lat = exifTable['GPSInfo']['GPSLatitude']
        lat_ref = exifTable['GPSInfo']['GPSLatitudeRef']
        latitude = degree_to_decimal(lat[0], lat[1], lat[2], lat_ref)

        #longitude extract
        lon = exifTable['GPSInfo']['GPSLongitude']
        lon_ref = exifTable['GPSInfo']['GPSLongitudeRef']
        longitude = degree_to_decimal(lon[0], lon[1], lon[2], lon_ref)

        return (latitude, longitude)
    return None

#converts the latitude and longitude from degree, minute, second, 
def degree_to_decimal(degrees, minutes, seconds, direction):
    decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
    if direction == 'S' or direction == 'W':
        decimal *= -1
    return decimal



def weather_by_month_2023(exif_table):

    if "latitude_decimal" in exif_table and "latitude_decimal" in exif_table:
        start = datetime(2023, 1, 1)
        end = datetime(2023, 12, 31)

        latLon = Point(exif_table["latitude_decimal"], exif_table["longitude_decimal"])

        #find nearest weather station to coordinates and 2023 weather data to a DataFrame
        data = Daily(latLon, start, end)
        data = data.normalize().aggregate(freq='1D').fetch()
        cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://flood-api.open-meteo.com/v1/flood"
        params = {
            "latitude": exif_table["latitude_decimal"],
            "longitude": exif_table["longitude_decimal"],
            "daily": "river_discharge",
        }
        responses = openmeteo.weather_api(url, params=params)
        

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]

        # Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()
        daily_river_discharge = daily.Variables(0).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}
        daily_data["river_discharge"] = daily_river_discharge

        return (data['tavg'].mean(), data['tmin'].mean(),  data['tmax'].mean(), data['prcp'].mean(), daily_data["river_discharge"])

#this iterates through all the files in the images directory and gets the latitude and longitude of the files
def image_metadata_to_json(images_folder):
    keys = []
    exif_table = {}
    for filename in os.listdir(images_folder):
        file_table = {}
        file_table['file'] = filename
        filename = "images/" + filename
        filepath = os.path.join(os.path.dirname(images_folder), filename)
        if filename.endswith('.jpg') or filename.endswith('.jpeg') or filename.endswith('.png'):
                file_table = exif_data_extract(filepath)
                if file_table != None:
                    print(filename)
                    file_table['Food Combination'] = random.randint(1, 1000)
                    if getDecimalCoordinates(file_table) != None:
                        file_table['latitude_decimal'] = getDecimalCoordinates(file_table)[0]
                        file_table['longitude_decimal'] = getDecimalCoordinates(file_table)[1]
                    if 'GPSInfo' in file_table:
                        del file_table["GPSInfo"]

                    #deleting makernote key in table due to clean up data
                    if 'MakerNote' in file_table:
                        del file_table["MakerNote"]

                    weather = weather_by_month_2023(file_table)
                    if weather != None:
                        file_table['Average 2023 Daily Temperature (Celsius)'] = weather[0]
                        file_table['Average 2023 Daily Min Temperature (Celsius)'] = weather[1]
                        file_table['Average 2023 Daily Max Temperature (Celsius)'] = weather[2]
                        file_table['Average 2023 Daily Rainfall (mm) '] = weather[3]
                        file_table['Daily River Discharge (Flooding Risk)'] = weather[4]

                        geolocator = Nominatim(user_agent ="EpiNu")
                        latitude = str(file_table['latitude_decimal'])
                        longitude = str(file_table['longitude_decimal'])

                        location = geolocator.reverse(latitude+","+longitude)
                        address = location.raw['address']

                        city = address.get('city', '')
                        state = address.get('state', '')
                        country = address.get('country', '')

                        file_table["nearby_city"] = city
                        file_table["state"] = state
                        file_table["country"] = country
                        
                    exif_table[filename] = file_table
    
    df = pd.DataFrame.from_dict(exif_table, orient='index')
    df.to_csv(os.path.dirname(__file__) + '/images.csv', sep='\t')

#concatenates the filepath of images with the absolute directory of the exif_test file
images_folder = os.path.join(os.path.dirname(__file__), "images")
image_metadata_to_json(images_folder)





