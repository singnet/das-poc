from enum import Enum

class CollectionNames(str, Enum):
    INCOMING_SET = 'IncomingSet'
    OUTGOING_SET = 'OutgoingSet'
    PATTERNS = 'Patterns'
    TEMPLATES = 'Templates'

