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

    # Create a exclusion set for punctuation characters
    exclude = set(string.punctuation)

    # Convert everything to lowercase
    sentences = sentences.map(lambda x: x.lower())
    # Remove punctuation symbols and blank spaces
    sentences = sentences.map(lambda x: ''.join(ch for ch in x if ch not in exclude)) \
                         .map(lambda x: x.strip())

    bigrams = sentences.flatMap(bigrams_map)

    bigrams = bigrams.reduceByKey(lambda x, y: x + y)
    # Sort by value
    bigrams = bigrams.sortBy(lambda x: x[1], ascending=False).take(100)
    bigrams = sc.parallelize(bigrams)

    bigrams.saveAsTextFile("bc.out")

    sc.stop()
