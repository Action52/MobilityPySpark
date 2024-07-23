from pysparkmeos.UDT.MeosDatatype import *
from pymeos import *

from shapely.geometry import *
from shapely import Geometry


def udt_append():
    """
    This function applies 'monkey patching' to be able to map each PySparkUDT to
    its corresponding PyMEOS class.
    :return:
    """
    TGeogPointInst.__UDT__ = TGeogPointInstUDT()
    TGeogPointSeq.__UDT__ = TGeogPointSeqUDT()
    TGeogPointSeqSet.__UDT__ = TGeogPointSeqSetUDT()

    TFloatInst.__UDT__ = TFloatInstUDT()
    TFloat.__UDT__ = TFloatInstUDT()

    STBox.__UDT__ = STBoxUDT()
    TsTzSpan.__UDT__ = TsTzSpanUDT()

    TGeomPointInst.__UDT__ = TGeomPointInstUDT()
    TGeomPointSeq.__UDT__ = TGeomPointSeqUDT()
    TGeomPointSeqSet.__UDT__ = TGeomPointSeqSetUDT()

    TBoolInst.__UDT__ = TBoolInstUDT()

    Point.__UDT__ = GeometryUDT()
    Polygon.__UDT__ = GeometryUDT()
    MultiPolygon.__UDT__ = GeometryUDT()
