from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re

class Chi(MRJob):
    def __init__(self, *args, **kwargs):
        self.stopwords=set()
        with open("stopwords.txt") as stopwords_file:
            for line in stopwords_file:
                self.stopwords.add(line.strip().lower())

    def steps (self):
       return [
           MRStep(mapper=self.mapper_categories,
                  reducer=self.reducer_categories),
           MRStep(mapper=self.mapper_prep,
                  combiner=self.combiner_prep,
                  reducer=self.reducer_prep),
       ] 
    
    def mapper_categories(self, _, line):
        record = json.loads(line)
        category = record['category']
        yield category, 1

    def reducer_categories(self, category, counts):
        yield category, sum(counts)    
        
    def mapper_prep(self, _, line):
        record = json.loads(line)
        category=record['category']
        text=record['reviewText']

        #using regex to filter and split the reviews
        #/s whitespace, tab etc.
        #/d numbers
        delimiters = r'[\s\d()\[\]{}.,;:+=\-_"\'`~#@&*%€$§\\/]+'

        text_split=re.split(delimiters, text)

        unique_words=set()

        for word in text_split:
            word=word.lower()
            if word and word not in self.stopwords and len(word)>1:
                unique_words.add(word)

        for word in unique_words:
            yield (word, category), 1

    def combiner_prep(self, word_category, count): 
        category, word = word_category
        yield word, (category,sum(count))

    def reducer_prep(self, word, values):
        category_counts={}
        for category, count in values:
            if category in category_counts:
                category_counts[category]+=count
            else:
                category_counts[category]=count
        yield word, category_counts

