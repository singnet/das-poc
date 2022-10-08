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
    ID_HASH = '_id'
    TYPE = 'composite_type_hash'
    KEY_PREFIX = 'key'
    KEYS = 'keys'
