import pandas as pd
import json

"""
----- Docs String -------

1.Get csv file data from a location, this location could be your S3 location.
2.For now i have taken my local desktop location
3.If  i will take s3 location then for testing prospective end user need to have s3 credentials 
and redshift and s3 set in place

"""

# Store file path in variable
file_path = 'C:/Users/Govind/Desktop/Assignment/data.csv'

# Read file from the path using read_csv() method
read_file = pd.read_csv(file_path, sep=',')

""" 
Converting csv data into dataframe
Transforming data from dataframe to json is easy that's why converted to csv data into Dataframe 
"""
access_event_df = pd.DataFrame(read_file)

"""
Transforming  dataframe event data into json by using to_json() method .
Attribute orient='records' means it will print the json data in records manners
"""
json_event_data = access_event_df.to_json(orient='records')

""" 
First of all, we are using json.loads() to create the json object from the json_event_data.
The json.dumps() method takes the json object and returns a JSON formatted string. 
The indent parameter is used to define the indent level for the formatted string. 
"""
json_object = json.loads(json_event_data)
json_formatted_data = json.dumps(json_object, indent=2)
print(json_formatted_data)

"""
If you want to export JSON Data corresponding to flat CSV file
you can put the outfile path to export the data on the particular location
Instead of 'json_data' put the complete path
"""
with open("json_data", "w") as outfile:
    json.dump(json_object, outfile, indent=2)
print('JSON data exported')
