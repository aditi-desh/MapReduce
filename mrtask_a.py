#a. Which vendors have the most trips, and what is the total revenue generated by that vendor?

from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRVendorMaxTrips(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
            MRStep(reducer=self.reducer2) ]    

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        #To Skip Header Line
                vendor = row[0]             #Get Vendor ID
                amount = float(row[16])     #Get Total Amount

                yield vendor, amount        #Return Vendor ID and Total Amount
                
    #Reducer to calculate the count and total revenue of each vendor                
    def reducer1(self, vendor, amount):     
        amt = 0
        count = 0
        total = 0
        
        for amt in amount:
            count = count + 1
            total = total + amt
        
        #Returns Count as key and Vendor ID and Total Revenue
        yield None,(count,(vendor,total))     
    
    #Reducer2 displays the Revenue and Vendor ID of those with most trips (highest count)
    def reducer2(self, _, result):             
        #Since Count is the key, this gives the result of Vendor with highest Count 
        output = max(result)
        #Displays only the highest Vendor and Total Amount
        yield output[1]                          
        
if __name__ == '__main__':
    MRVendorMaxTrips.run()        
            
           