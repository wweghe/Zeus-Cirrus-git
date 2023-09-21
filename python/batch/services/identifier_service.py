from typing import Tuple

import common.constants as constants

from domain.identifier import Identifier

class IdentifierService:

    def __init__(self) -> None:
        pass


    def create_by_id_ssc(self, id : str, ssc : str = constants.SOURCE_SYSTEM_CD_DEFAULT) -> Identifier:
        return Identifier(
            id = id, 
            ssc = ssc if ssc is not None else constants.SOURCE_SYSTEM_CD_DEFAULT)
    

    def create_by_key(self, id_ssc : str) -> Identifier:
        if id_ssc is None or len(str(id_ssc)) == 0: return None
        parts = id_ssc.split(":")
        return Identifier(
            id = parts[0], 
            ssc = parts[1] if len(parts) > 1 else constants.SOURCE_SYSTEM_CD_DEFAULT)
    

    def create_by_key_value(self, id_ssc : dict[str, str]) -> Identifier:
        return Identifier(
            id = id_ssc["id"],
            ssc = id_ssc["ssc"])


    def compare(self, i1 : Identifier, i2 : Identifier) -> bool:
        return i1.get_key() == i2.get_key()