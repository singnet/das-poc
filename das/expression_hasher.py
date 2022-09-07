from typing import List, Any
from hashlib import md5

class ExpressionHasher:

    compound_separator = " "

    @staticmethod
    def _compute_hash(text: str) -> str:
        return md5(text.encode("utf-8")).digest().hex()

    @staticmethod
    def named_type_hash(name: str) -> str:
        return ExpressionHasher._compute_hash(name)

    @staticmethod
    def terminal_hash(named_type: str, terminal_name: str) -> str:
        return ExpressionHasher._compute_hash(
            ExpressionHasher.compound_separator.join([named_type, terminal_name]))

    @staticmethod
    def expression_hash(named_type_hash: str, elements: List[str]) -> str:
        return ExpressionHasher.composite_hash([named_type_hash, *elements])

    @staticmethod
    def composite_hash(hash_base: Any) -> str:
        if isinstance(hash_base, str):
            return hash_base
        elif isinstance(hash_base, list):
            if len(hash_base) == 1:
                return hash_base[0]
            else:
                return ExpressionHasher._compute_hash(ExpressionHasher.compound_separator.join(hash_base))
        else:
            raise ValueError(f"Invalid base to compute composite hash: {type(hash_base)}: {hash_base}")


class StringExpressionHasher:

    @staticmethod
    def _compute_hash(text: str) -> str:
        return str

    @staticmethod
    def named_type_hash(name: str) -> str:
        return f"<Type: {name}>"

    @staticmethod
    def terminal_hash(named_type: str, terminal_name: str) -> str:
        return f"<{named_type}: {terminal_name}>"

    @staticmethod
    def expression_hash(named_type_hash: str, elements: List[str]) -> str:
        return f"<{named_type_hash}: {elements}>"

    @staticmethod
    def composite_hash(hash_list: List[str]) -> str:
        if len(hash_list) == 1:
            return hash_list[0]
        return f"{hash_list}"
