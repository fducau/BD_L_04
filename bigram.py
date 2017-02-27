from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import string

def bigrams_map(sentence):
    words = sentence.split(" ")
    N = len(words)
    return [((words[i], words[i+1]),1) for i in range(N-1)]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    sentences = lines.glom() \
                     .map(lambda x: " ".join(x)) \
                     .flatMap(lambda x: x.split("."))

    #Your code goes here
    trans = string.maketrans('','')

    sentences = sentences.map(lambda x: x.lower()) \
                         .map(lambda x: x.translate(trans, string.punctuation)) \
                         .map(lambda x: x.strip())

    bigrams = sentences.flatMap(bigrams_map)
    # bigrams = sc.parallelize(bigrams)
    bigrams = bigrams.reduceByKey(lambda x, y: x + y)
    # Sort by value
    bigrams = bigrams.sortBy(lambda x: x[1])

    bigrams.saveAsTextFile("bc.out")

    sc.stop()
