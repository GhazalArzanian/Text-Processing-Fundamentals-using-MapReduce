from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import csv
import logging
import sys
from mrjob.protocol import RawValueProtocol

class TermDistribution(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def __init__(self, *args, **kwargs):
        super(TermDistribution, self).__init__(*args, **kwargs)

        self.stopwords=set() #unique set for the stopwords
        self.category_distribution={}
        #reading in the stopword file. Right now works only for local testing. 
        #TODO: making it run in the cluster
        with open("stopwords.txt") as stopwords_file:
            for line in stopwords_file:
                self.stopwords.add(line.strip().lower())

    def init_category_distribution(self):
        with open("categories.txt") as stopwords_file:
            for line in stopwords_file:
                key, value=line.split()
                cleaned_key=key.strip('"')
                self.category_distribution[cleaned_key]=int(value)
        

    def steps (self):
        return [
            MRStep( mapper=self.mapper_prep,
                    combiner=self.combiner_sum,
                    reducer=self.reducer_sum),
            MRStep( mapper_init=self.mapper2_init,
                    mapper=self.mapper2,
                    combiner= self.combiner2,
                    reducer=self.reducer2)
       ]
        
    def mapper_prep(self, _, line):
        record = json.loads(line) #loading the json
        category=record.get('category', '') 
        text=record.get('reviewText', '')

        text_split=re.findall('[A-Za-z]+', text) #only keeping those words which contain only letters from the English alphabet

        unique_words=set() 

        #creating the list for the unique words in the review
        for word in text_split:
            word=word.lower()
            if word and word not in self.stopwords and len(word)>1:
                unique_words.add(word)

        #yielding back each word from the list
        #for word in unique_words:
        for word in unique_words:
            yield (word, category), 1

    def combiner_sum(self, keys, values): 
        term,category = keys
        s = sum(values)
        yield term, (category,s)

    def reducer_sum(self, keys, values):
        yield keys, list(values)

    def mapper2_init(self):#it calculated the total number of documents
        self.init_category_distribution()
        self.total_docs=0
        for key, value in self.category_distribution.items():
            self.total_docs+=int(value)

    def mapper2(self, key, values):
        cat_dist=self.category_distribution.copy()
        values_dict = {val[0]:  val[1] for val in values}
        #needed: 
        # number of documents -from txt
        # number of documents in category - from txt
        # all appearances of term - from different grouping we can get that
        # number of docs in category which contain term - we can get it from where!!

        # # gots (term, category) pairs with value
        total_term_count=0
        total_term_count = sum(values_dict.values())
        for category, count in values_dict.items():
            N=self.total_docs
            A=count #number of documents in category which contain term
            B=total_term_count-A #number of documents not in category which contain term
            C = cat_dist[category] - A  # number of documents in category which does not contain term
            D = N - A - C- B  # number of documents not in category which does not contain term
            if ((A + B) * (A + C) * (B + D) * (C + D)) == 0:
                chi = 0
            else:
                chi = (N * (A * D - B * C) * (A * D - B * C)) / ((A + B) * (A + C) * (B + D) * (C + D))
            yield category, {key: chi}
            
    def combiner2(self, key, values):
        combined_dictionary={}
        for value in values:
            for term, count in value.items():
                if term not in combined_dictionary: 
                    combined_dictionary[term]=0
                combined_dictionary[term]+=count
        yield key, combined_dictionary

    def reducer2(self, key, values):
        combined_dictionary={}
        for value in values:
            for term, count in value.items():
                if term not in combined_dictionary: 
                    combined_dictionary[term]=0
                combined_dictionary[term]+=count
                
        sorted_chi_values = sorted(combined_dictionary.items(), key=lambda x: x[1], reverse=True)[:75]
        
        formatted_output = f"<{key}> " + " ".join(f"{term}:{value:.2f}" for term, value in sorted_chi_values)
        
        yield None, formatted_output

if __name__ == '__main__':
    TermDistribution.run()