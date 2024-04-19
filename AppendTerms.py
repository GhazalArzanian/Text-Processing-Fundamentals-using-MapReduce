from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import json

class AppendTerms(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol  # Output lines exactly as yielded

    def mapper(self, _, line):
        # Parsing the input line to extract category and terms
        line_data = line.strip()
        if line_data:
            category, terms_str = line_data.split(' ', 1)
            category = category.strip('<>')  # Remove <> around the category
            terms_dict = dict(item.split(':') for item in terms_str.split(' '))
            yield "data", line_data  # Emit original data
            for term in terms_dict.keys():
                yield "terms", term  # Emit terms

    def reducer_init(self):
        self.terms = set()

    def reducer(self, key, values):
        if key == "data":
            for value in values:
                yield None, value
        elif key == "terms":
            for term in values:
                self.terms.add(term)

    def reducer_final(self):
        sorted_terms = sorted(self.terms)
        if sorted_terms:  # Check if there are any terms to output
            yield None, ', '.join(sorted_terms)  # Emit sorted terms in one line

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final)
        ]

if __name__ == '__main__':
    AppendTerms.run()


