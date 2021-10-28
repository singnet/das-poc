#!/usr/bin/env python3
import argparse
from collections import defaultdict
from hashlib import sha256
from typing import Union

from atomese2metta.parser import Parser
from atomese2metta.translator import Translator
from atomese2metta.translator import MettaDocument, AtomType, Expression, MSet


class Hasher:
    def __init__(self, document: MettaDocument, algorithm=sha256):
        self.document = document
        self.algorithm = algorithm
        self.atom_type_dict = dict()
        self.hash_index = defaultdict(set)

    def apply_alg(self, value: str) -> str:
        return self.algorithm(value.encode("utf-8")).digest().hex()

    def search_by_name(self, name: str) -> AtomType:
        return self.atom_type_dict.get(name, None)

    def get_type_signature(self, atom_type: AtomType) -> str:
        name = atom_type.symbol
        _atom_type = self.search_by_name(atom_type.type)

        atom_type_id = (_atom_type._id or "") if _atom_type is not None else ""

        return name + atom_type_id

    def get_expression_type_hash(self, expression: Expression) -> str:
        ids = []
        for e in expression:
            if isinstance(e, Expression):
                ids.append(self.get_expression_type_hash(e))
            elif isinstance(e, str):
                ids.append(self.search_by_name(e)._id)
            else:
                raise ValueError(e)

        if isinstance(expression, MSet):
            ids = sorted(ids)
            ids.insert(0, expression.SALT)

        return self.apply_alg("".join(ids))

    def get_expression_hash(self, expression: Union[Expression, str], level=0) -> str:
        if isinstance(expression, str):
            return self.apply_alg(expression)

        elif isinstance(expression, Expression):
            expression_type_hash = self.get_expression_type_hash(expression)
            keys_hashes = [
                self.get_expression_hash(key, level=level + 1) for key in expression
            ]

            signature = expression_type_hash + "".join(keys_hashes)
            hash_id = self.apply_alg(signature)
            expression._id = hash_id
            self.add_hash(expression)
            return hash_id

        else:
            raise ValueError(f"InvalidSymbol: {expression}")

    def hash_atom_types(self):
        for atom_type in self.document.types:
            value = self.get_type_signature(atom_type)
            _id = self.apply_alg(value)
            atom_type._id = _id
            self.atom_type_dict[atom_type.symbol] = atom_type
            self.add_hash(atom_type)

    def hash_expressions(self):
        for expression in self.document.body:
            expression.is_root = True
            self.get_expression_hash(expression)

    def add_hash(self, value):
        self.hash_index[value._id].add(value)


def main(filename):
    parser = Parser()

    with open(filename, "r") as f:
        parsed = parser.parse(f.read())

    document = Translator.build(parsed)

    hasher = Hasher(document)
    hasher.hash_atom_types()
    hasher.hash_expressions()

    print(repr(document))


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Hashing MettaDocumen ")
    parser.add_argument("filename", type=str, help="Input sample .scm filename")

    args = parser.parse_args()
    main(args.filename)
