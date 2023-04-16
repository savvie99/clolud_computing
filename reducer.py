
# reducer function
def reducerfunction(reduce_in, reduce_out):
    
    # iterate through values and keys
    for key, values in reduce_in.items():
        # calculate sum off values for each key
        num_flights = reduceitem(values)    
        # save reducer result
        if key not in reduce_out:
            # if the passenger is not in the dictionary the number of flights is set as the value
            reduce_out[key] = num_flights
        else:
            # else add number of flights to value
            reduce_out[key] += num_flights

# calculate sum of values to count the flights
def reduceitem(values):
    return sum(values)


        
        
