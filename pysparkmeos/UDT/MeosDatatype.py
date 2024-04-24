import pickle
from typing import Any, Generic, TypeVar

from pyspark.sql.types import UserDefinedType, BinaryType, StringType

from pymeos import TGeogPointInst, TGeomPointInst, TFloatInst, STBox, TFloat, TPoint

T = TypeVar('T')


class MeosDatatypeUDT(Generic[T], UserDefinedType):
    """
    Wrapper for PyMEOS Datatypes.
    """

    @classmethod
    def sqlType(cls) -> StringType:
        """
        Returns the SQL data type corresponding to this UDT, which is StringType in this case.
        """
        return StringType()

    @classmethod
    def module(cls) -> str:
        return "pysparkmeos.UDT.MeosDatatype"

    def serialize(self, obj: T) -> str:
        """
        Serializes the Python object to a byte array using pickle.

        :param obj: The object to serialize.
        :return: The serialized object as a byte array.
        """
        return obj.__str__()

    def deserialize(self, datum: str) -> T:
        """
        Deserializes a string back into a Python object.
        Subclasses should provide an implementation of this method.
        """
        return self.from_string(datum)

    def from_string(self, datum: str) -> T:
        """
        Factory method to create an instance of T from a string.
        This method should be overridden by all subclasses.
        """
        raise NotImplementedError("Subclasses must implement from_string method.")

    def simpleString(self) -> str:
        return "meosdatatype"


class TGeogPointInstUDT(MeosDatatypeUDT[TGeogPointInst]):
    def simpleString(self) -> str:
        return "tgeogpointinst"
        
    def from_string(self, datum: str) -> TGeogPointInst:
        return TGeogPointInst(datum)


class TGeomPointInstUDT(MeosDatatypeUDT[TPoint]):
    def simpleString(self) -> str:
        return "tgeompointinst"

    def from_string(self, datum: str) -> TPoint:
        return TGeomPointInst(datum)
        

class TFloatInstUDT(MeosDatatypeUDT[TFloat]):
    def simpleString(self) -> str:
        return "tfloat"    

    def from_string(self, datum: str) -> TFloatInst:
        return TFloatInst(datum)


class STBoxUDT(MeosDatatypeUDT[STBox]):
    def simpleString(self) -> str:
        return "stbox"
        
    def from_string(self, datum: str) -> STBox:
        return STBox(datum)