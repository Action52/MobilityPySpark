from pymeos import *

"""
Wrappers to be able to serialize/deserialize PyMEOS classes with PySpark UDTs.
"""


class TGeogPointInstWrap(TGeogPointInst):
    def __setstate__(self, state):
        pymeos_initialize()
        self._inner = TGeogPointInst(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        return self.__str__()


class STBoxWrap(STBox):
    def __setstate__(self, state):
        pymeos_initialize()
        self._inner = STBox(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        return self.__str__()


class TGeogPointSeqSetWrap(TGeogPointSeqSet):
    def __setstate__(self, state):
        pymeos_initialize()
        self._inner = TGeogPointSeqSet(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        return self.__str__()



class TGeogPointSeqWrap(TGeogPointSeq):
    def __setstate__(self, state):
        pymeos_initialize()
        self._inner = TGeogPointSeq(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        return self.__str__()


class TGeomPointInstWrap(TGeomPointInst):
    def __setstate__(self, state):
        pymeos_initialize()
        self._inner = TGeomPointInst(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        return self.__str__()


class TGeomPointSeqWrap(TGeomPointSeq):
    def __setstate__(self, state):
        pymeos_initialize()
        self._inner = TGeomPointSeq(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        return self.__str__()


class TGeomPointSeqSetWrap(TGeomPointSeqSet):
    def __setstate__(self, state):
        pymeos_initialize()
        self._inner = TGeomPointSeqSet(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        return self.__str__()


class TBoolInstWrap(TBoolInst):
    def __setstate__(self, state):
        pymeos_initialize()
        self._inner = TBoolInst(state)._inner

    def __getstate__(self):
        pymeos_initialize()
        return self.__str__()
