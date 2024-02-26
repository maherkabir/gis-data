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
        return (data['tavg'].mean(), data['tmin'].mean(),  data['tmax'].mean())

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
                    if getDecimalCoordinates(file_table) != None:
                        file_table['latitude_decimal'] = getDecimalCoordinates(file_table)[0]
                        file_table['longitude_decimal'] = getDecimalCoordinates(file_table)[1]
                    del file_table["GPSInfo"]

                    #deleting makernote key in table due to clean up data
                    if 'MakerNote' in file_table:
                        del file_table["MakerNote"]

                    weather = weather_by_month_2023(file_table)
                    file_table['Average 2023 Daily Temperature'] = weather[0]
                    file_table['Average 2023 Daily Min Temperature'] = weather[1]
                    file_table['Average 2023 Daily Max Temperature'] = weather[2]

                    geolocator = Nominatim(user_agent ="EpiNu")
                    latitude = str(file_table['latitude_decimal'])
                    longitude = str(file_table['longitude_decimal'])

                    location = geolocator.reverse(latitude+","+longitude)
                    address = location.raw['address']

                    city = address.get('city', '')
                    country = address.get('country', '')
                    state = address.get('state', '')

                    file_table["nearby_city"] = city
                    file_table["country"] = country
                    for key in file_table.keys():
                        if type(file_table[key]) in [TiffImagePlugin.IFDRational]:
                            file_table[key] = float(file_table[key]._numerator / file_table[key]._denominator)
                        if type(file_table[key]) in [bytes]:
                            file_table[key] = "N/A"
                        if type(file_table[key]) in [tuple]:
                            for i in file_table[key]:
                                if type(i) in [TiffImagePlugin.IFDRational]:
                                    i = float(i._numerator / i._denominator)
                        
                    exif_table[filename] = file_table
    
    df = pd.DataFrame.from_dict(exif_table, orient='index')
    df.to_csv(os.path.dirname(__file__) + '/images.csv', sep='\t')

#concatenates the filepath of images with the absolute directory of the exif_test file
images_folder = os.path.join(os.path.dirname(__file__), "images")
image_metadata_to_json(images_folder)



