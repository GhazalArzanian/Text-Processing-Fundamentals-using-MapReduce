from mrjob.job import MRJob
from mrjob.step import MRStep
    

class Countline(MRJob):

    
    def mapper(self, _, line):
        yield -1, 1 # count how many lines we have in the json file
    
    def combiner(self, key, counts):
        yield None, sum(counts)
    
    def reducer(self, key, counts):
        yield None, sum(counts)
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]


if __name__ == '__main__':
    Countline.run()
