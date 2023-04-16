
import multiprocessing as mp
import threading

# class to shcedule the threads of the map reduce task
class threadschedule:
    
    # class constructor
    def __init__(self, mapper_func, shuffler_func, reducer_func, data_rows):
        # mapper
        self.map_func     = mapper_func
        # shuffler
        self.shuffle_func = shuffler_func
        # reducer
        self.reduce_func  = reducer_func
        # data
        self.data_records = data_rows
        # dictionary to store reducer results
        self.reduce_out = {}
        # for multithreading
        self.threads = []
        
        
    
    def map_reduce(self, map_in):

        # map passengers with flights
        map_out   = self.map_func(map_in)

        # group flights for each passenger
        reduce_in = self.shuffle_func(map_out)

        # count number of flights for each passenger
        self.reduce_func(reduce_in, self.reduce_out)
         
        
    def start(self):
    
        # calculate chunk size
        recordtotal = len(self.data_records)
        threadnumber   = mp.cpu_count()
        chunksize = recordtotal // threadnumber
        while chunksize * threadnumber < recordtotal:
            chunksize += 1  
            
        # chuncks input for the mapper
        mapchuncks = [self.data_records[i:i+chunksize] for i in range(0, recordtotal, chunksize)]

        # create and start thread for each chunck 
        for mapinput in mapchuncks:
            t = threading.Thread(target=self.map_reduce, args=(mapinput,))
            t.start()
            self.threads.append(t)
    
           
    # function to wait until all threads are completed
    def waituntilcomplete(self):
        for t in self.threads:
            t.join()
        
       
            
    
