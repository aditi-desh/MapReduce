#f.How does revenue vary over time? Calculate the average trip revenue - analysing it by hour of the day (day vs night)

from mrjob.job import MRJob
from mrjob.step import MRStep

#importing datetime library to handle timestamp data
from datetime import datetime
# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRAvgRevenueByHour(MRJob):
    
    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID': #To Skip Header Line
                
                putime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")  #Pick Up Time Stamp to Time Object                
                hour = putime.hour              #Get the Hour from pickup datetime
                hour_day = 'Hour ' + str(hour)  #Concatenate the word 'Hour' and Hour of the day for output
                amount = float(row[16])         #Get Total Amount 

                #Returns Hour of the day and Total Amount
                yield hour_day, amount         
                
    #Reducer to calculate average revenue per hour of the day             
    def reducer(self, hour_day, amount):       
        
        amt = 0
        count = 0
        total = 0
        avg_revenue = 0

        for amt in amount:
            count = count + 1
            total = total + amt

        #Calculate Average Revenue     
        avg_revenue = total/count
        
        #Returns Average Revenue
        yield hour_day, avg_revenue      

if __name__ == '__main__':
    MRAvgRevenueByHour.run()                
                
                