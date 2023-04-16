import sys
import mapper
import shuffler
import reducer
from threadschedule import threadschedule

def main(argv):
    # csv file with flight data
    data_path = "AComp_Passenger_data_no_error.csv"
        
    # read the file
    rows = set() # set to remove duplicate rows
    with open(data_path, 'r') as file:
        row = file.readline()
        while row:
            rows.add(row.strip())
            row = file.readline()
        
    # convert set of rows to list
    data_rows = list(rows)
    
    # create and start map reduce
    mapreducetask = threadschedule(mapper.mapperfunction, shuffler.shuffle, reducer.reducerfunction, data_rows)
    mapreducetask.start()
    mapreducetask.waituntilcomplete()

    # sort in descending order
    sortedflights = sorted(mapreducetask.reduce_out.items(), key=lambda item: item[1], reverse=True)
    # print the results
    print("\nThe passenger(s) with the most flights are:\n")
    for i in range(10):
        key, value = sortedflights[i]
        print(f"{key}\t\t{value}")
    
    # save output to file
    with open('output.txt', 'w', encoding='utf-8') as my_file:
        for item in sortedflights:
            my_file.write(f'{item}\n')

if __name__ == '__main__':
    main(sys.argv)
    
    
