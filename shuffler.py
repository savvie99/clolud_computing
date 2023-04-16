
# Sorts and shuffle the data based on the key
def shuffle(mapper_out):
    
    # Dictionary to group the flights per passenger
    data = {}
    
    # Iterate each entry of the mapper_out
    for key, value in mapper_out:
        if key not in data:
            # If the key is not in dictionary
            # We need to create a list with the value as single item
            # And put key-value pair to the dictionary
            data[key] = [value]
        else:
            # If the key is already there
            # Append the value to values list
            data[key].append(value)
         
    # Return the data for reduce stage
    return data

