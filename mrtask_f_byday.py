#f.How does revenue vary over time? Calculate the average trip revenue - analysing it by the day of the week .

from mrjob.job import MRJob
from mrjob.step import MRStep

#importing datetime library to handle timestamp data
from datetime import datetime
# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRAvgRevenueByDay(MRJob):

    def mapper(self, _, line):
        
        #Dictionary for Week Days
        days = {1:'Monday',2:'Tuesday',3:'Wednesday',4:'Thursday',5:'Friday',6:'Saturday',7:'Sunday'}
        
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID': #To Skip Header Line
                
                putime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")  #Pick Up Time Stamp to Time Object
                              
                weekday = putime.isoweekday()   #Returns an integer between 1-7 for days Monday to Sunday
                
                dayofweek = days.get(weekday)   #Get the week day name corresponding to the week day number

                amount = float(row[16])         #Get Total Amount 

                #Returns Day of week and Total Amount
                yield dayofweek, amount

    #Reducer to calculate average revenue per day of week             
    def reducer(self, day, amount):       
        
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
        yield day, avg_revenue      

if __name__ == '__main__':
    MRAvgRevenueByDay.run()