#e.Calculate the average tips to revenue ratio of the drivers for different locations in sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRTotalAmtTipRatio(MRJob):

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
                amount = float(row[16])     #Total_amount
                tip    = float(row[13])     #Tip Amount
                
                #Returns Pick Up Location, Amount and Tip Amount
                yield puloc, (amount,tip)

    #Reducer to calculate Tip to Revenue Ratio for each Pick Up Location                
    def reducer1(self, puloc, amnt_pair):
        amt = 0
        total_amnt = 0
        total_tip = 0

        #Loop through the tip-total amount pair from Mapper to calculate the average ration
        for amt in amnt_pair:
            total_amnt = total_amnt + amt[0]
            total_tip  = total_tip + amt[1]
        
        #Calculate Average Ratio 
        if total_amnt == 0:
            avg_ratio = 0
        else:
            avg_ratio = total_tip/total_amnt
        
        #Returns Pick Up Location and Average Tip to Revenue Ratio         
        yield None, (avg_ratio,puloc)

    #Reducer to display the sorted result    
    def reducer2(self, _, result_pair):  
        #Sort the result by Average Ratio
        sorted_result = sorted(result_pair, reverse = True)
        for pair in sorted_result:
            yield pair[1], pair[0]

if __name__ == '__main__':
    MRTotalAmtTipRatio.run()            