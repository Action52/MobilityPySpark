import pickle
from typing import Any, Generic, TypeVar

from pyspark.sql.types import UserDefinedType, BinaryType

from pymeos import TGeogPointInst

T = TypeVar('T')


class MeosDatatype(Generic[T], UserDefinedType):
    """
    Wrapper for PyMEOS Datatypes.
    """

    @classmethod
    def sqlType(cls) -> BinaryType:
        """
        Returns the SQL data type corresponding to this UDT, which is BinaryType in this case.
        """
        return BinaryType()

    def serialize(self, obj: Any) -> bytes:
        """
        Serializes the Python object to a byte array using pickle.

        :param obj: The object to serialize.
        :return: The serialized object as a byte array.
        """
        return pickle.dumps(obj)

    def deserialize(self, datum: bytes) -> Any:
        """
        Deserializes a byte array back into a Python object using pickle.

        :param datum: The byte array to deserialize.
        :return: The deserialized Python object.
        """
        return pickle.loads(datum)


class TGeogPointInstUDT(MeosDatatype[TGeogPointInst]):
    pass