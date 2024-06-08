import googlemaps
from datetime import datetime
import pandas as pd
import sys
import json
import time
import numpy as np
from joblib import Parallel, delayed
import os

SECRET_KEY = "xxx"

gmaps_client = googlemaps.Client( key = SECRET_KEY)
now = datetime.now()


sheet_name = 'Final'  
data = pd.read_excel\
    ("/Users/praveenchandardevarajan/Downloads/India_all_markets.xlsx", \
     sheet_name = sheet_name)



def split_lat_long(lat_long_str):
    try:
        lat, long = map(float, lat_long_str.split(','))
        return lat, long
    except ValueError as e:
        print(f"Error converting {lat_long_str}: {e}")
        return None, None

# Apply the function to the DataFrame
data['latitude'], data['longitude'] = \
    zip(*data['Latitude-Longitude'].map(split_lat_long))





def distance_matrix(gmaps_client, sources, destinations):
    try:
        result = gmaps_client.distance_matrix\
            (origins=sources, destinations=destinations, \
             mode="driving", avoid="ferries")
        return result
    except Exception as e:
        print(f"Error: {e}")
        return None

def extract_distances_durations(response, source_names, destination_names):
    distances_durations = []
    if response and 'rows' in response:
        for i, row in enumerate(response['rows']):
            elements = row['elements']
            for j, element in enumerate(elements):
                if element['status'] == 'OK':
                    distance = element['distance']['value']
                    duration = element['duration']['value']
                else:
                    distance = None
                    duration = None
                distances_durations.append({
                    'Source': source_names[i],
                    'Destination': destination_names[j],
                    'Distance (meters)': distance,
                    'Duration (seconds)': duration
                })
    return distances_durations




results = []

# Starting row index
start_index = 0  

# Batch size
batch_size = 10
loop_count = 0
# Number of rows in the DataFrame
num_rows = data.shape[0]

# Initialize lists to store the current batch of API calls
sources_batch = []
source_names_batch = []
destinations_batch = []
destination_names_batch = []

for i in range(start_index, min(start_index + batch_size, num_rows)):
    row_i = data.iloc[i]
    source = f"{row_i['latitude']},{row_i['longitude']}"
    source_name = row_i['AMC Name']
    for j in range(num_rows):
        if i != j:  # To avoid calculating distance/time from a place to itself
            row_j = data.iloc[j]
            destination = f"{row_j['latitude']},{row_j['longitude']}"
            destination_name = row_j['AMC Name']
            sources_batch.append(source)
            source_names_batch.append(source_name)
            destinations_batch.append(destination)
            destination_names_batch.append(destination_name)

            # When the batch is full, process it
            if len(sources_batch) == batch_size:
                response = distance_matrix\
                    (gmaps_client, sources_batch, destinations_batch)
                batch_results = extract_distances_durations\
                    (response, source_names_batch, destination_names_batch)
                results.extend(batch_results)
                sources_batch = []  
                source_names_batch = []  
                destinations_batch = []  
                destination_names_batch = [] 
                # Increment loop count
                loop_count += 1
                if loop_count %100 == 0:
                    print(loop_count)
                # Save results to CSV after every 1000 loops
                if loop_count % 1000 == 0:
                    results_df = pd.DataFrame(results)
                    if os.path.exists('results.csv'):
                        results_df.to_csv('results.csv', \
                                          mode='a', header=False, index=False)
                        print('saved')
                    else:
                        results_df.to_csv('results.csv', index=False)
                        print('saved')
                    results = [] 


# Process any remaining API calls in the last batch (if not processed already)
if sources_batch and destinations_batch:
    response = distance_matrix\
        (gmaps_client, sources_batch, destinations_batch)
    batch_results = extract_distances_durations\
        (response, source_names_batch, destination_names_batch)
    results.extend(batch_results)

# Save remaining results to CSV
if results:
    results_df = pd.DataFrame(results)
    if os.path.exists('results.csv'):
        results_df.to_csv('results.csv', mode='a', header=False, index=False)
    else:
        results_df.to_csv('results.csv', index=False)






