from pysparkmeos.UDT.MeosDatatype import *
from pymeos import TGeogPointInst, TGeomPointInst, TFloatInst, STBox, TPoint, TFloat

def udt_append():
    TGeogPointInst.__UDT__ = TGeogPointInstUDT()
    TPoint.__UDT__ = TGeogPointInstUDT()
    
    TFloatInst.__UDT__ = TFloatInstUDT()
    TFloat.__UDT__ = TFloatInstUDT()
    
    STBox.__UDT__ = STBoxUDT()
    # TGeomPointInst.__UDT__ = TGeomPointInstUDT()