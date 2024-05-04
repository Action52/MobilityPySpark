from abc import abstractmethod
from collections import defaultdict
from typing import Any, Callable, Optional, Iterable, Tuple

from pyspark.rdd import RDD, Partitioner, portable_hash, K, V
from pyspark.serializers import pack_long, BatchedSerializer
from pyspark.shuffle import get_used_memory
from pyspark.traceback_utils import SCCallSiteSync

from pyspark.sql import DataFrame

class MobilityPartitioner(Partitioner):
    """
    Abstract method to define a partition strategy. Follows the definition
    from official Spark Partitioner.
    """

    def __call__(self, k: Any, *args):
        return self.partitionFunc(k, *args) % self.numPartitions

    @abstractmethod
    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        pass

    @abstractmethod
    def get_partition(self, key: int, data: Any, utc_time="UTC", **kwargs) -> int:
        """Return the partition index for the given key. Pass any kwargs as needed. """
        pass


class MobilityRDD(RDD):
    def partitionBy(
        self: "RDD[Tuple[K, V]]",
        numPartitions: Optional[int],
        partitionFunc: Callable[[K], int] = portable_hash,
        **kwargs
    ) -> "RDD[Tuple[K, V]]":
        """
        Partitions the RDD according to the mobility data.

        Parameters
        ----------
        numPartitions : int, optional
            the number of partitions in new :class:`RDD`
        partitionFunc : function, optional, default `portable_hash`
            function to compute the partition index

        Returns
        -------
        :class:`RDD`
            a :class:`RDD` partitioned using the specified partitioner

        See Also
        --------
        :meth:`RDD.repartition`
        :meth:`RDD.repartitionAndSortWithinPartitions`

        Examples
        --------
        >>> j = 0
        >>> sets = pairs.partitionBy(2).glom().collect()
        >>> len(set(sets[0]).intersection(set(sets[1])))
        """
        if numPartitions is None:
            numPartitions = self._defaultReducePartitions()
        partitioner = Partitioner(numPartitions, partitionFunc)
        if self.partitioner == partitioner:
            return self

        # Transferring O(n) objects to Java is too expensive.
        # Instead, we'll form the hash buckets in Python,
        # transferring O(numPartitions) objects to Java.
        # Each object is a (splitNumber, [objects]) pair.
        # In order to avoid too huge objects, the objects are
        # grouped into chunks.
        outputSerializer = self.ctx._unbatched_serializer

        limit = self._memory_limit() / 2

        def add_shuffle_key(split: int, iterator: Iterable[Tuple[K, V]]) -> Iterable[bytes]:

            buckets = defaultdict(list)
            c, batch = 0, min(10 * numPartitions, 1000)  # type: ignore[operator]

            for k, v in iterator:
                buckets[partitionFunc(k, data=v) % numPartitions].append((k, v))  # type: ignore[operator]
                c += 1

                # check used memory and avg size of chunk of objects
                if c % 1000 == 0 and get_used_memory() > limit or c > batch:
                    n, size = len(buckets), 0
                    for split in list(buckets.keys()):
                        yield pack_long(split)
                        d = outputSerializer.dumps(buckets[split])
                        del buckets[split]
                        yield d
                        size += len(d)

                    avg = int(size / n) >> 20
                    # let 1M < avg < 10M
                    if avg < 1:
                        batch = min(sys.maxsize, batch * 1.5)  # type: ignore[assignment]
                    elif avg > 10:
                        batch = max(int(batch / 1.5), 1)
                    c = 0

            for split, items in buckets.items():
                yield pack_long(split)
                yield outputSerializer.dumps(items)

        keyed = self.mapPartitionsWithIndex(add_shuffle_key, preservesPartitioning=True)
        keyed._bypass_serializer = True  # type: ignore[attr-defined]
        assert self.ctx._jvm is not None

        with SCCallSiteSync(self.context):
            pairRDD = self.ctx._jvm.PairwiseRDD(keyed._jrdd.rdd()).asJavaPairRDD()
            jpartitioner = self.ctx._jvm.PythonPartitioner(numPartitions, id(partitionFunc))
        jrdd = self.ctx._jvm.PythonRDD.valueOfPair(pairRDD.partitionBy(jpartitioner))
        rdd: "RDD[Tuple[K, V]]" = RDD(jrdd, self.ctx, BatchedSerializer(outputSerializer))
        rdd.partitioner = partitioner
        return rdd