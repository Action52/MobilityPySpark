from abc import ABC, abstractmethod
from typing import Any

from pyspark import RDD

class MobilityPartition(ABC):
    """ Abstract method to define a partition strategy. Follows the definition from official Spark Partitioner. """
    @abstractmethod
    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        pass

    @abstractmethod
    def get_partition(self, key: int, data: Any, utc_time="UTC", **kwargs) -> int:
        """Return the partition index for the given key. Pass any kwargs as needed. """
        pass
