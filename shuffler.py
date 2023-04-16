
# sort and shuffle data 
def shuffle(mapper_out):
    
    # dict to group flights
    data = {}
    
    # iterate through mapper output
    for key, value in mapper_out:
        if key not in data:
            data[key] = [value]
        else:
            data[key].append(value)
         
    # return data to be sued by reducer
    return data

