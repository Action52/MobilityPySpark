import pickle
from typing import Any, Generic, TypeVar

from pyspark.sql.types import UserDefinedType, BinaryType, StringType

from pymeos import *

from shapely.geometry import Point, Polygon
from shapely import from_wkt, to_wkt, Geometry

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

"""
#########################
WRAPPERS
#########################
"""

class MeosWrap:  
    def __setstate__(self, state):
        pymeos_initialize()
        #print("Im being unpickled: ", state)
        self._inner = self(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        #print("Im being pickled: ", self.__str__())
        stringrepr = self.__str__()
        del self._inner
        return stringrepr


class TGeogPointInstWrap(TGeogPointInst):
    def __setstate__(self, state):
        pymeos_initialize()
        #print("Im being unpickled: ", state)
        self._inner = TGeogPointInst(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        #print("Im being pickled: ", self.__str__())
        stringrepr = self.__str__()
        del self._inner
        return stringrepr


class STBoxWrap(STBox):
    def __setstate__(self, state):
        pymeos_initialize()
        #print("Im being unpickled: ", state)
        self._inner = STBox(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        #print("Im being pickled: ", self.__str__())
        stringrepr = self.__str__()
        del self._inner
        return stringrepr


class TGeogPointSeqSetWrap(TGeogPointSeqSet):
    def __setstate__(self, state):
        pymeos_initialize()
        #print("Im being unpickled: ", state)
        self._inner = TGeogPointSeqSet(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        #print("Im being pickled: ", self.__str__())
        stringrepr = self.__str__()
        del self._inner
        return stringrepr


class TGeogPointSeqWrap(TGeogPointSeq):
    def __setstate__(self, state):
        pymeos_initialize()
        #print("Im being unpickled: ", state)
        self._inner = TGeogPointSeq(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        #print("Im being pickled: ", self.__str__())
        stringrepr = self.__str__()
        del self._inner
        return stringrepr

class TGeomPointInstWrap(TGeomPointInst, MeosWrap):
    pass


class TGeomPointSeqWrap(TGeomPointSeq, MeosWrap):
    pass


class TGeomPointSeqSetWrap(TGeomPointSeqSet, MeosWrap):
    pass


"""
#########################
UDTs
#########################
"""

class TGeogPointInstUDT(MeosDatatypeUDT[TGeogPointInstWrap]):
    def simpleString(self) -> str:
        return "tgeogpointinst"
        
    def from_string(self, datum: str) -> TGeogPointInstWrap:
        return TGeogPointInstWrap(datum)


class TGeomPointInstUDT(MeosDatatypeUDT[TGeomPointInstWrap]):
    def simpleString(self) -> str:
        return "tgeompointinst"

    def from_string(self, datum: str) -> TGeomPointInstWrap:
        return TGeomPointInstWrap(datum)


class TGeomPointSeqUDT(MeosDatatypeUDT[TGeomPointSeqWrap]):
    def simpleString(self) -> str:
        return "tgeompointseq"

    def from_string(self, datum: str) -> TGeomPointSeqWrap:
        return TGeomPointSeqWrap(datum)


class TGeomPointSeqSetUDT(MeosDatatypeUDT[TGeomPointSeqSetWrap]):
    def simpleString(self) -> str:
        return "tgeompointseqset"

    def from_string(self, datum: str) -> TGeomPointSeqSetWrap:
        return TGeomPointSeqSetWrap(datum)
        

class TFloatInstUDT(MeosDatatypeUDT[TFloatInst]):
    def simpleString(self) -> str:
        return "tfloatinst"    

    def from_string(self, datum: str) -> TFloatInst:
        return TFloatInst(datum)


class STBoxUDT(MeosDatatypeUDT[STBoxWrap]):
    def simpleString(self) -> str:
        return "stbox"
        
    def from_string(self, datum: str) -> STBoxWrap:
        return STBoxWrap(datum)


class TsTzSpanUDT(MeosDatatypeUDT[TsTzSpan]):
    def simpleString(self) -> str:
        return "tstzspan"

    def from_string(self, datum: str) -> TsTzSpan:
        return TsTzSpan(datum)


class TGeogPointSeqUDT(MeosDatatypeUDT[TGeogPointSeqWrap]):
    def simpleString(self) -> str:
        return "tgeogpointseq"

    def from_string(self, datum: str) -> TGeogPointSeqWrap:
        return TGeogPointSeqWrap(datum)


class TGeogPointSeqSetUDT(MeosDatatypeUDT[TGeogPointSeqSetWrap]):
    def simpleString(self) -> str:
        return "tgeogpointseqset"

    def from_string(self, datum: str) -> TGeogPointSeqSetWrap:
        return TGeogPointSeqSetWrap(datum)


class GeometryUDT(UserDefinedType):
    """
    Wrapper for shapely.geometry.Point Datatype.
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

    def serialize(self, obj: Geometry) -> str:
        """
        Serializes the Python object to a byte array using pickle.

        :param obj: The object to serialize.
        :return: The serialized object as a byte array.
        """
        return to_wkt(obj)

    def deserialize(self, datum: str) -> str:
        """
        Deserializes a string back into a Python object.
        Subclasses should provide an implementation of this method.
        """
        return from_wkt(datum)