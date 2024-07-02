#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import osrm
import time
import numpy as np



client = osrm.Client(host='http://router.project-osrm.org')

def split_lat_long(lat_long_str):
    try:
        lat, long = map(float, lat_long_str.split(','))
        return lat, long
    except ValueError as e:
        print(f"Error converting {lat_long_str}: {e}")
        return None, None

# Load data from Excel
sheet_name = 'Final'  
data = pd.read_excel("/Users/praveenchandardevarajan/Downloads/India_all_markets.xlsx", sheet_name=sheet_name)

# Apply the function to the DataFrame
data['latitude'], data['longitude'] = zip(*data['Latitude-Longitude'].map(split_lat_long))

# Function to calculate distance and duration using OSRM
def calculate_distance_duration_osrm(sources, destinations):
    distances_durations = []
    for source in sources:
        for destination in destinations:
            try:
                route = client.route(
                    coordinates=[source, destination],
                    overview='false'
                )
                distance = route['routes'][0]['distance']
                duration = route['routes'][0]['duration']
                distances_durations.append((distance, duration))
            except Exception as e:
                print(f"Error: {e}")
                distances_durations.append((None, None))
    return distances_durations



start_time = time.time()
num_locations = data.shape[0]

distance_matrix = np.full((num_locations, num_locations), np.nan)
travel_time_matrix = np.full((num_locations, num_locations), np.nan)

# Set save frequency (number of iterations after which to save the matrices)
save_frequency = 100
counter = 0

start_row = 629

def calculate_distance_time(client, sources, destinations):
    coordinates = [[src[1], src[0]] for src in sources] + [[dest[1], dest[0]] for dest in destinations]
    responses = []
    for i, src in enumerate(sources):
        for j, dest in enumerate(destinations):
            if src != dest:
                coords = [coordinates[i], coordinates[j + len(sources)]]
                response = client.route(coordinates=coords, overview=osrm.overview.full)
                distance = response['routes'][0]['distance']  # distance in meters
                duration = response['routes'][0]['duration']  # duration in seconds
                responses.append((distance, duration))
    return responses

batch_size = 10 
num_batches = (num_locations - start_row) // batch_size + 1
count = 0
for batch in range(num_batches):
    print(batch)
    batch_start = start_row + batch * batch_size
    print(batch_start)
    batch_end = min(start_row + (batch + 1) * batch_size, num_locations)
    print(batch_end)
    sources = [(data.iloc[i]['latitude'], data.iloc[i]['longitude']) for i in range(batch_start, batch_end)]
    source_names =[data.iloc[i]['AMC Name'] for i in range(batch_start, batch_end)]
    print(source_names)

    if count == 105:
        break
    for j, dest_row in data.iterrows():
        destinations = [(dest_row['latitude'], dest_row['longitude'])]
        print(dest_row['AMC Name'])
        count += 5
        results = calculate_distance_time(client, sources, destinations)
        if count == 105:
            break
        for k, (i, src) in enumerate(zip(range(batch_start, batch_end), sources)):
            distance, travel_time = results[k]
            print(f"Distance from {src} to {destinations[0]}: {distance} meters, {travel_time} seconds")
            distance_matrix[i, j] = distance
            travel_time_matrix[i, j] = travel_time

            if count == 100:
                end_time = time.time()
                print(f"Time taken for 100 pairs: {end_time - start_time} seconds")
                # Extrapolate to a million pairs
                estimated_time_for_million = (end_time - start_time) * (1000000 / 100)
                print(f"Estimated time for 1 million pairs: {estimated_time_for_million / 3600} hours")
                break
            if count % save_frequency == 0:
                # Convert matrices to DataFrames
                distance_df = pd.DataFrame(distance_matrix, index=data['AMC Name'], columns=data['AMC Name'])
                travel_time_df = pd.DataFrame(travel_time_matrix, index=data['AMC Name'], columns=data['AMC Name'])
                break
                # Save intermediate results
                distance_df.to_csv(f'distance_matrix_{counter // save_frequency}.csv')
                travel_time_df.to_csv(f'travel_time_matrix_{counter // save_frequency}.csv')
                print(f"Saved interim results at iteration {counter}")
                
# Final save after completing all calculations
# distance_df = pd.DataFrame(distance_matrix, index=data['AMC Name'], columns=data['AMC Name'])
# travel_time_df = pd.DataFrame(travel_time_matrix, index=data['AMC Name'], columns=data['AMC Name'])

# distance_df.to_csv('final_distance_matrix.csv')
# travel_time_df.to_csv('final_travel_time_matrix.csv')

# print("Final Distance Matrix (meters):")
# print(distance_df)
# print("\nFinal Travel Time Matrix (seconds):")
# print(travel_time_df)



