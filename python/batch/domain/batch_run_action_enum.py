from enum import Enum


class BatchRunActionEnum(str, Enum):
    DELETE = 'DELETE'
    CREATE = 'CREATE'
    UPDATE = 'UPDATE'
    RUN = 'RUN'
    SKIP = 'SKIP'

    
