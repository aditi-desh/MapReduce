#c.What are the different payment types used by customers and their count? The final results should be in a sorted format.
from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRCountOfPaymentTypes(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,                   
                  reducer=self.reducer1),
            MRStep(reducer=self.reducer2) ]
           
    def mapper(self, _, line):                      
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        #To Skip Header Line
                ptype  = row[9]             #Payment Type
                count  = 1                  #Assign Count=1 for each trip 
                #Return Payment Type and Count
                yield ptype,count                           
        
    #Reducer to calculate the total count for each Payment Type            
    def reducer1(self, ptype, count):       
        #Return Payment Type and the total trip count for each Payment Type
         yield None, (sum(count),ptype)
            
    #Reducer to display the sorted result    
    def reducer2(self, _, result_pair):  
        #Sort the result by Total Trip Count
        sorted_result = sorted(result_pair, reverse = True)
        for pair in sorted_result:
            yield pair[1], pair[0]            
                                   
            
if __name__ == '__main__':
    MRCountOfPaymentTypes.run()                
