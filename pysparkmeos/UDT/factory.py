from enum import Enum, auto
from typing import Type

from pysparkmeos

class MeosDataType(Enum):
    """
    Enum to manage mapping of PyMEOS datatype classes to their corresponding UDT classes.
    """
    TGeogPointInst = (TGeogPointInstUDT, auto())
    

    def __init__(self, udt_class: Type[MeosDatatype], _):
        self.udt_class = udt_class