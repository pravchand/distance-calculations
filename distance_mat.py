import googlemaps
from datetime import datetime
import pandas as pd

import numpy as np
from joblib import Parallel
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




sources_batch = []
source_names_batch = []
destinations_batch = []
destination_names_batch = []
results = []
loop_count = 0
save_after_batches = 1000
origins_batch_size = 10  
destinations_batch_size = 10  
num_rows = data.shape[0]
start_index = 619 

for i in range(start_index, start_index + 10):
    row_i = data.iloc[i]
    source = f"{row_i['latitude']},{row_i['longitude']}"
    source_name = row_i['AMC Name']
    sources_batch.append(source)
    source_names_batch.append(source_name)
    print("done")
count = 0
for j in range(num_rows):
    if j!=start_index: 
        count += 1
        row_j = data.iloc[j]
        destination = f"{row_j['latitude']},{row_j['longitude']}"
        destination_name = row_j['AMC Name']
        destinations_batch.append(destination)
        destination_names_batch.append(destination_name)
        print(count,len(destinations_batch))
        print(len(sources_batch))
        if len(sources_batch) == origins_batch_size and len(destinations_batch) == destinations_batch_size:
            response = distance_matrix(gmaps_client, sources_batch, destinations_batch)
            batch_results = extract_distances_durations(response, source_names_batch, destination_names_batch)
            results.extend(batch_results)
            destinations_batch = []
            destination_names_batch = []
            #Increment loop count
            loop_count += 1
            if loop_count in [5, 25, 50, 150]:
                print(batch_results)
            # Save results to CSV after every 1000 batches
            if loop_count % save_after_batches == 0:
                results_df = pd.DataFrame(results)
                if os.path.exists('results_working.csv'):
                    results_df.to_csv('results_working.csv', mode='a', header=False, index=False)
                    print("SAVED")
                else:
                    results_df.to_csv('results_working.csv', index=False)
                results = []  # Clear results after saving to CSV

if sources_batch and destinations_batch:
    response = distance_matrix(gmaps_client, sources_batch, destinations_batch)
    batch_results = extract_distances_durations(response, source_names_batch, destination_names_batch)
    results.extend(batch_results)
    results_df = pd.DataFrame(results)
    if os.path.exists('results_working.csv'):
        results_df.to_csv('results_working.csv', mode='a', header=False, index=False)
    else:
        results_df.to_csv('results_working.csv', index=False)






