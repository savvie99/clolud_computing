
import re


# mapper function
def mapperfunction(map_in):

    # apply the map for each row
    return list(map(maprow, map_in)) 


# map flights to passengers and validate the input values
def maprow(row):
    
    # keys are passenger and flight ids
    cols = row.split(',')
    passenger_id = cols[0]
    
    if re.match(r"^[A-Z]{3}\d{4}[A-Z]{2}\d$", passenger_id):
        
        # put 1 to count the flights
        return (passenger_id, 1)
    





















