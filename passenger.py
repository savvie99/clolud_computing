import pandas as pd


def mapper(passengers_list, flights_list):

        mapper_out=[]

        for passenger, flight in zip(passengers_list, flights_list):

            mapper_out.append((passenger,1))

        # print(mapper_out)

        return mapper_out
def shuffle(mapper_out):

        reduce_in={}

        for passenger, flight in mapper_out:
 
            if passenger not in reduce_in:
                reduce_in[passenger]=[flight]

            else:
                reduce_in[passenger].append(flight)
        # print(reduce_in)

        return reduce_in
    
def reducer(reduce_in):

        reduce_out={}

        for passenger, flight_list in reduce_in.items():

            reduce_out[passenger]=sum(flight_list)

        df=pd.DataFrame.from_dict(reduce_out, orient="index")
        df.to_csv("reduce_output.csv")

        # print(reduce_out)

        return reduce_out

def max_passenger(reduce_out):
        max_passenger=max(reduce_out, key=reduce_out.get)

        max_flight = reduce_out[max_passenger]

        print("The passenger with ID: "+max_passenger+" had the highest number of flights: "+str(max_flight))

        return max_passenger
        
        
