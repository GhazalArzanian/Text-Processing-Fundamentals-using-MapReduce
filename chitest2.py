from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import operator
import os
import re


# Load stopwords
with open("countoutput.txt", "r") as count_file:
    for line in count_file:
        parts = line.strip().split("\t")  # Split the line on the tab character
        if parts[0] == "null":  # Check if the first part is 'null'
            DOCS_LENGTH = int(parts[1])
with open("stopwords.txt") as stopwords_file:
    STOPWORDS = set(line.strip() for line in stopwords_file)
    

class Chi(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner),
            MRStep (reducer= self.reducer2),
            MRStep (mapper= self.mapper2),
            MRStep (reducer= self.reducer3),
            MRStep (mapper= self.mapper4),
            MRStep(reducer=self.reducer_final_terms)
        ]
    
    def mapper(self, _, line):
        # Load the JSON object
        json_line = json.loads(line)

        # Extract the review and category
        review = json_line.get('reviewText', '')
        category = json_line.get('category', '')
        # Tokenise review text
        review_tok = re.findall('[A-Za-z]+', review)
        
        # Filter out stopwords and words with only one character
        for term in review_tok:
            # case-fold the word
            term = term.lower()
            if term in STOPWORDS: # skip stopwords
                return
                
            yield (category,term), 1 # to count how many TERMS we have => gives us A
            

    def combiner(self, key, counts): # reduce/group by (category, term)_sum of each unique category,term
        category, term = key
        s = sum(counts)
        yield term, (category,s)
    
    def reducer2 (self, term, values):   
        yield term, list(values)
    
    def mapper2(self, term, value_list):
   
        values_dict = {val[0]:  val[1] for val in value_list}
        new_dict={} # dic--> category: A,B
        for category, value in values_dict.items():
            new_dict[category]=[value, sum(values_dict.values())-value] 
                # category: { value=A, B=sum(values_dict.values())-A}
        for category, value in new_dict.items():
            yield category, (term,value[0],value[1]) # emit category, term, A, B
    
  
    def reducer3 (self, category, values):
        yield category, list(values) # reduce by category to receive list of lists
        
    def mapper4(self, category, value_list):
        values_bigdict_temp = {val[0]: val[1] for val in value_list}  # create dict to easier extract sum pro category
        sum_pro_category = sum(values_bigdict_temp.values())  # calculate the sum of docs pro category
        values_bigdict_withCN = {val[0]: [val[1], val[2], sum_pro_category - val[1], DOCS_LENGTH] for val in value_list}
        final_dict = {}
        for key, val in values_bigdict_withCN.items():
            A = val[0]
            B = val[1]
            C = val[2]
            N = val[3]
            D = N - A - B - C
            top = N * (A * D - B * C) ** 2
            bottom = (A + B) * (A + C) * (B + D) * (C + D)
            final_dict[key] = top / bottom

    # Emit all terms under a special key for final sorting
        for term in final_dict.keys():
            yield "zzzzzAll Terms", term
    
    # Emit category results with chi-squared values
        final_dict = dict(sorted(final_dict.items(), key=operator.itemgetter(1), reverse=True)[:75])
        yield category, final_dict

    
    
    def reducer_final_terms(self, key, values):
        if key == "zzzzzAll Terms":
        # Handling all terms for final output
            unique_terms = sorted(set(values))
            yield "All Terms", unique_terms
        else:
        # Handling category results
            final_dicts = list(values)  # Assuming values are the final_dicts from mapper4
        # You might want to process or simply pass through these dictionaries
            for final_dict in final_dicts:
                yield key, final_dict

        
    


if __name__ == '__main__':
    Chi.run()

