from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import csv
import logging

class Categories(MRJob):
    def mapper(self, _, line):
        record = json.loads(line) #loading the json
        category=record.get('category', '') 
        yield category, 1
    def combiner (self, category, value):
        yield category, sum(value)
    def reducer (self, category, value):
        yield category, sum(value)

if __name__ == '__main__':
    Categories.run()