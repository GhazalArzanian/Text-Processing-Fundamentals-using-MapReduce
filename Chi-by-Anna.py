from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import csv

class Chi(MRJob):
    def __init__(self, *args, **kwargs):
        super(Chi, self).__init__(*args, **kwargs)
        self.stopwords=set()
        self.category_distribution={}
        self.word_category_distribution={}
        with open(r'C:\Users\annal\Documents\GitHub\Text-Processing-Fundamentals-using-MapReduce\stopwords.txt') as stopwords_file:
            for line in stopwords_file:
                self.stopwords.add(line.strip().lower())

    def steps (self):
       return [
            MRStep( mapper=self.mapper_prep,
                    combiner=self.combiner_sum,
                    reducer=self.reducer_sum),
            MRStep( mapper=self.mapper_final, 
                    reducer=self.reducer_build_contigency,
                    reducer_final=self.reducer_final)
       ] 
        
    def mapper_prep(self, _, line):
        record = json.loads(line)
        category=record.get('category', '')
        text=record.get('reviewText', '')

        text_split=re.findall('[A-Za-z]+', text)

        unique_words=set()

        yield ('category_count', category), 1

        for word in text_split:
            word=word.lower()
            if word and word not in self.stopwords and len(word)>1:
                unique_words.add(word)

        for word in unique_words:
            yield ('word_count', word, category), 1

    def combiner_sum(self, key, values): 
        yield key, sum(values)

    def reducer_sum(self, key, values):
        yield key,sum(values)

    def mapper_final(self, key, values):
        yield key, values


    #makes empty tables :(
    def reducer_build_contigency(self, key, values):
        if key[0]=='category_count':
            self.category_distribution[key[1]]=sum(values)
        elif key[0]=='word_count':
            if (key[1], key[2]) not in self.word_category_distribution:
                self.category_distribution[(key[1], key[2])]=0
            self.category_distribution[(key[1], key[2])]+=sum(values)
    
    def reducer_final(self):
        # print('xd')
        chi=0
        N=sum(self.category_distribution.values()) #number of documents
        for (term, category), count in self.word_category_distribution.items():

            category_count=self.category_distribution[category] #number of documents in category
            sum_of_term=sum(self.word_category_distribution.get((term, cat), 0) for cat in self.category_distribution.keys()) #all appearances of term

            A=self.word_category_distribution[(term, category)] #number of documents in category which contain term
            B=sum_of_term-A #number of documents not in category which contain term
            C=category_count-A #number of documents in category which does not contain term
            D=N-category_count-B #number of documents not in category which does not contain term
                
            chi=(N*(A*D-B*C)*(A*D-B*C))/((A+B)*(A+C)*(B+D)*(C+D))


            yield (term, category), chi

if __name__ == '__main__':
    Chi.run()