#d. What is the average trip time for different pickup locations?
from mrjob.job import MRJob
from mrjob.step import MRStep

#Import datetime for handling timestamp data
from datetime import datetime
from datetime import timedelta

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRAvgTripTime(MRJob):

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        #To Skip Header Line
                puloc = row[7]              #Pick Up Location ID
                putime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")  #Pick Up Time Stamp to Time Object
                dotime = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")  #Drop Of Time Stamp to Time Object
                
                #Difference between Drop Off Time and Pick Up time as Trip Duration in seconds
                duration =(dotime - putime).total_seconds()      
                #Returns Pick Up Location ID and trip duration
                yield puloc,duration      

    #Reducer to calculate the count and total revenue of each vendor                
    def reducer(self, puloc, duration):       
        time = 0
        count = 0
        total = 0
        avg_duration = 0
        result = ''

        #Loop through the duration list to calculate total duration and total count
        for time in duration:
            count = count + 1
            total = total + time

        #Calculate Average Duration for each Pick Up Location      
        avg_duration = int(total/count)
        
        #Converting average duration into Hours, Minutes and Seconds
        hours = avg_duration // 3600
        minutes = (avg_duration % 3600) // 60
        seconds = avg_duration % 60

        result = str(hours) + 'hours ' + str(minutes) + 'minutes ' + str(seconds) + 'seconds'
        
        #Returns Average Duration per Pick Up Location
        yield puloc,result      

if __name__ == '__main__':
    MRAvgTripTime.run()
