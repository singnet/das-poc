from enum import Enum

class CollectionNames(str, Enum):
    NODES = 'nodes'
    ATOM_TYPES = 'node_types'
    LINKS_ARITY_1 = 'links_2'
    LINKS_ARITY_2 = 'links_3'
    LINKS_ARITY_N = 'links'

class FieldNames(str, Enum):
    NODE_NAME = 'name'
    TYPE_NAME = 'name'
    ID_HASH = '_id'
    TYPE = 'type'
    KEY_PREFIX = 'key'
