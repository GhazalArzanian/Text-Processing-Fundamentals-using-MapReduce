from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import csv
import logging

class Chi(MRJob):
    def __init__(self, *args, **kwargs):
        super(Chi, self).__init__(*args, **kwargs)

        self.stopwords=set() #unique set for the stopwords
        self.total_docs=0
        #reading in the stopword file. Right now works only for local testing. 
        #TODO: making it run in the cluster
        with open(r'C:\Users\annal\Documents\GitHub\Text-Processing-Fundamentals-using-MapReduce\stopwords.txt') as stopwords_file:
            for line in stopwords_file:
                self.stopwords.add(line.strip().lower())

    def steps (self):
       return [
            MRStep( mapper_init=self.mapper_init,
                    mapper=self.mapper_prep,
                    mapper_final=self.mapper_final,
                    combiner=self.combiner_sum,
                    reducer=self.reducer_sum),
            MRStep( mapper=self.mapper2,
                    combiner= self.combiner2,
                    reducer=self.reducer2),
            # MRStep( mapper=self.mapper_final, 
            #         reducer=self.reducer_build_contigency,
            #         #reducer_final=self.reducer_final
            #         ),
            # MRStep(reducer=self.reducer_final)
       ]
    def mapper_init (self):
        self.total_docs=0 
    
    def mapper_final(self):
        yield "total_docs", self.total_docs
        
    def mapper_prep(self, _, line):
        record = json.loads(line) #loading the json
        self.total_docs+=1 
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
            yield ('word_count', word, category), 1

    def combiner_sum(self, key, values): #values(category, unique_words, 1)
        if key=="total_docs":
            yield key, values
        else:


    def reducer_sum(self, key, values):
        yield key, (values[0], values[1],sum(values[2])) #summing globally

    def mapper2(self, key, values):
        N=values[2]
        category=values[0]

        for term in values[1]:
            yield (term, category), (N, 1) #adding the values to the reducer

    def combiner2 (self, key, values):
        yield key, (values[0], sum(values[1])) 

    def reducer2(self, key, values):
        yield key, (values[0], sum(values[1])) 

    def save_key_value_to_file(self, key, filename):
    # Write data to file in append mode, so each new entry is added to the file
        with open(filename, 'a') as f:
            json.dump(key, f)
            f.write('\n')  # Ensure each entry is on a new line for readability

    #makes empty tables :( for each key, it will be called again and again!!
    #so legyártja egyszer, aztán még egyszer.. F
    #valami olyasmi kell hogy átadom a kulcsot, a valuet, és a maradék adatot is így rögtön egy word-category párhoz
    def reducer_build_contigency(self, key, values):
        #building the contigency table for later use
        if key[0]=='category_count':
            self.category_distribution[key[1]]=sum(values)
            with open( r'C:\Users\annal\Documents\GitHub\Text-Processing-Fundamentals-using-MapReduce\category_log.txt', 'a') as f:
                f.write(str(len(self.category_distribution.keys()))+'\n')  # Ensure each entry is on a new line for readability
        elif key[0]=='word_count':
            if (key[1], key[2]) not in self.word_category_distribution:
                self.word_category_distribution[(key[1], key[2])]=0
            self.word_category_distribution[(key[1], key[2])]+=sum(values)
        else:
            print(1)
    
    def reducer_final(self):
        chi=0

        #needed: 
        # number of documents
        # number of documents in category
        # all appearances of term
        # number of docs in category which contain term
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