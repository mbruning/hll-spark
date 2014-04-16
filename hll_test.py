""" example of how to use hyperloglog data structure
in the context of spark partitions """

import hyperloglog
from pyspark import SparkContext

sc = SparkContext("local", "test")

test = [1,2,2,3,4,4,5,4,6,5]

values = sc.parallelize(test, 4)

# the actual hll object
hll = hyperloglog.HyperLogLog(0.01)

class HLLWrapper(object):
    """
        a simple wrapper class
        to be able to map spark partitions 
        to the wrapped hll object
    """

    def __init__(self, hll):
        self._hll = hll

    def __call__(self, val):
        for v in val:
            self._hll.add(str(v))
            yield self._hll
     
    @staticmethod   
    def update(x, y):
        x.update(y)
        return x

acc = values.mapPartitions(HLLWrapper(hll))
count = acc.reduce(HLLWrapper.update)
assert(len(count) == len(set(test)))

