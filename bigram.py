from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext

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
    sentences = sentences.map(lambda x: x.lower()) \
                         .map(lambda x: x.strip())

    bigrams = sentences.flatMap(bigrams_from_sentence)
    bigrams = bigrams.reduceByKey(lambda x, y: x+y)

    bigrams.saveAsTextFile("bc.out")

    sc.stop()
