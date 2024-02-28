from typing import overload

import common.constants as constants


class Identifier:

    def __init__(self, id : str, ssc : str = constants.SOURCE_SYSTEM_CD_DEFAULT) -> None:
        self._id = id
        self._ssc = str(ssc) if ssc is not None else constants.SOURCE_SYSTEM_CD_DEFAULT


    def get_key(self) -> str:
        return f"{self._id}:{self._ssc}"
    

    def get_id(self) -> str: return self._id


    def get_ssc(self) -> str: return self._ssc
