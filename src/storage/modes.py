from enum import Enum

class WriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"
