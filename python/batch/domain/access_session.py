from typing import Any, Dict, List, Tuple, Callable
from datetime import datetime


class AccessSession:

    def __init__(self, 
                 access_token : str,
                 token_decoded : Dict[str, Any],
                 refresh_token : str,
                 session_details : Dict[str, Any],
                 client_auth : Tuple[str, str]
                ) -> None:
        
        self.access_token = access_token
        self.token_decoded = token_decoded
        self.refresh_token = refresh_token
        self.session_details = session_details
        self.client_auth = client_auth


    def get_token_expiration_from_now(self) -> int:

        exp = self.token_decoded.get("exp")
        exp_dttm = datetime.utcfromtimestamp(exp)
    
        return (exp_dttm - datetime.utcnow()).total_seconds()


    def is_access_expiring(self, refresh_skew_sec : int) -> bool:
        return (self.get_token_expiration_from_now() - refresh_skew_sec) <= 0