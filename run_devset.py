import os

os.system("python categories.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json > categories.txt")
os.system("python term_distribution_final.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar --file stopwords.txt --file categories.txt -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json > output1.txt")
os.system("python append_terms.py output1.txt > output.txt")
