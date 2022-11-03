from enum import Enum

class CollectionNames(str, Enum):
    NODES = 'nodes'
    ATOM_TYPES = 'atom_types'
    LINKS_ARITY_1 = 'links_1'
    LINKS_ARITY_2 = 'links_2'
    LINKS_ARITY_N = 'links_n'

class FieldNames(str, Enum):
    NODE_NAME = 'name'
    TYPE_NAME = 'named_type'
    TYPE_NAME_HASH = 'named_type_hash'
    ID_HASH = '_id'
    TYPE = 'composite_type_hash'
    COMPOSITE_TYPE = 'composite_type'
    KEY_PREFIX = 'key'
    KEYS = 'keys'
