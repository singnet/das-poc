import json
from typing import Optional, List, Any
from dataclasses import dataclass

@dataclass
class Expression:

    #TODO: Implement non-ordered
    toplevel: bool = False
    ordered: bool = True
    terminal_name: Optional[str] = None
    typedef_name: Optional[str] = None
    typedef_name_hash: Optional[str] = None
    symbol_name: Optional[str] = None
    named_type: Optional[str] = None
    named_type_hash: Optional[str] = None
    composite_type: Optional[List[Any]] = None
    composite_type_hash: Optional[str] = None
    elements: Optional[List[str]] = None
    hash_code: Optional[str] = None

    def __hash__(self):
        return hash(self.hash_code)

    def to_dict(self):
        assert(self.ordered)
        answer = {
            "_id": self.hash_code,
            "composite_type_hash": self.composite_type_hash
        }
        if self.typedef_name is not None:
            # expression is a typedef
            answer["named_type"] = self.typedef_name
            answer["named_type_hash"] = self.typedef_name_hash
        elif self.terminal_name is not None:
            # expression is a terminal
            answer["name"] = self.terminal_name
            answer["named_type"] = self.named_type
        else:
            # expression is a regular expression
            answer["is_toplevel"] = self.toplevel
            answer["composite_type"] = self.composite_type
            answer["named_type"] = self.named_type
            answer["named_type_hash"] = self.named_type_hash
            arity = len(self.elements)
            assert arity > 0
            if arity > 2:
                answer["keys"] = self.elements
            else:
                answer["key_0"] = self.elements[0]
                if arity > 1:
                    answer["key_1"] = self.elements[1]
        return answer

    def to_json(self):
        return json.dumps(self.to_dict(), sort_keys=False, indent=4)
