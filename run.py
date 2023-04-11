from passenger import mapper, shuffle, reducer, max_passenger
import multiprocessing as mp
import pandas as pd
colnames= ['passenger_id', "flight_id","departure_code","arrive_code","departure_time", "flight_time"]
df = pd.read_csv("AComp_Passenger_data_no_error.csv",names=colnames, header=None)


if __name__=='__main__':
    with mp.Pool(processes=mp.cpu_count()) as pool:



        mapper_out=mapper(df["passenger_id"], df["flight_id"])

        reduce_in=shuffle(mapper_out)

        reduce_out=reducer(reduce_in)

        max_passenger(reduce_out)



