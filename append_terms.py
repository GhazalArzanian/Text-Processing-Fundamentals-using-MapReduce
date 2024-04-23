#!/usr/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol

class append_terms(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol  
    #this prevents keys from being output, only the values. Since the input file already has keys and values,
    #the final output would return 'null' before each category name ootherwise.

    def mapper(self, _, line):
        line_data = line.strip()             #Extract category and term from input lines
        if line_data:
            category, terms_str = line_data.split(' ', 1)
            terms_dict = dict(item.split(':') for item in terms_str.split(' ')) #split terms:values on the : to extract only the term  
            yield "original_data", line_data  #yield original data
            for term in terms_dict.keys():
                yield "terms_sorted", term  #yield terms without values

    def reducer_init(self):
        self.terms = set() #initializes a set for the unique terms to add to for the following reducers before they are called on

    def reducer(self, key, values):
        if key == "original_data":
            for value in values:       #yields the original data as output
                yield None, value
        elif key == "terms_sorted":    
            for term in values:        #adds every unique term to the initialized set. Using a set prevents duplicates
                self.terms.add(term)

    def reducer_final(self):
        sorted_terms = sorted(self.terms)       #sorts the terms alphabetically
        if sorted_terms:                     
            yield None, ' '.join(sorted_terms)  #yields the sorted terms in the final line

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final)
        ]

if __name__ == '__main__':
    append_terms.run()


