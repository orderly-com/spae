from enum import Enum, unique


@unique
class DataBaseType(Enum):
    MONGODB = 1
    POSTGRES = 2
