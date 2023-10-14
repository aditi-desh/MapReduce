#b. Which pickup location generates the most revenue? 

from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRMaxRevenueLoc(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
            MRStep(reducer=self.reducer2) ]

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        #To Skip Header Line
                puloc  = row[7]             #Pick Up Location ID
                amount = float(row[16])     #Total Amount
                #Return Pick Up Location ID and Total Amount    
                yield puloc, amount         

    #Reducer1 to calculate the total revenue            
    def reducer1(self, puloc, amount):     
        #Calculate sum of Total Amount for each Pick Up Location
        yield None, (sum(amount),puloc)     

    #Reducer2 to find the Pick Up Location with highest revenue    
    def reducer2(self, _, amnt_puloc_pair):         
        result = max(amnt_puloc_pair)
        #Displays Pick Up Location and Total Revenue
        yield result[1], result[0]              


if __name__ == '__main__':
    MRMaxRevenueLoc.run()
        
        