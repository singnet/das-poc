from abc import ABC
import re
from collections.abc import MutableSequence
from typing import Iterable, Sequence, Union, Optional

from .collections import OrderedSet

Symbol = str
Unknown = Symbol("?")
Type = Symbol("Type")


class BaseExpression(ABC):
    SYMBOL = None
    SALT = None

    OPENER = "("
    CLOSER = ")"


class Expression(list, BaseExpression):

    def __init__(self, iterable, _id=None, is_root=False, type_hash=None):
        self.extend(iterable)
        self._id = _id
        self.type_hash = type_hash
        self.is_root = is_root

    def _signature(self):
        return f"{(self.SALT or 'EXPR')}:{':'.join(str(hash(e)) for e in self)}"

    def __hash__(self):
        return hash(self._signature())

    def __str__(self):
        return f'{self.OPENER}{" ".join([str(v) for v in self])}{self.CLOSER}'

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(list(self))}, _id={repr(self._id)}, is_root={repr(self.is_root)}, type_hash={repr(self.type_hash)})'

class MList(Expression):
    SYMBOL = "List"
    SALT = "LIST"


class MSet(Expression):
    SYMBOL = "Set"
    SALT = "SET"

    OPENER = "{"
    CLOSER = "}"


class AtomType(BaseExpression):
    def __init__(self, symbol: Symbol, mtype: Optional[Symbol] = Type, _id=None):
        self._id = _id
        self.symbol: Symbol = symbol
        self.type: Optional[Symbol] = mtype

    def __hash__(self):
        return hash(self.symbol + ":" + (self.type or ''))

    def __eq__(self, other):
        return self.symbol == other.symbol and self.type == other.type

    def __str__(self):
        return f"{self.OPENER}: {self.symbol} {self.type}{self.CLOSER}"

    def __repr__(self):
        return f"{self.__class__.__name__}(_id={repr(self._id)}, symbol={repr(self.symbol)}, mtype={repr(self.type)})"


class InvalidSymbol(Exception):
    pass


class Translator:
    _ALLOWED_LINKS = (
        "ContextLink",
        "EvaluationLink",
        "InheritanceLink",
        "ListLink",
        "MemberLink",
        "SetLink",
    )

    _ALLOWED_NODES = (
        "CellNode",
        "ChebiNode",
        "ChebiOntologyNode",
        "PredicateNode",
        "BiologicalProcessNode",
        "CellularComponentNode",
        "ConceptNode",
        "MolecularFunctionNode",
        "NcbiTaxonomyNode",
        "GeneNode",
        "ReactomeNode",
        "SmpNode",
        "UberonNode",
    )

    IGNORED_SYMBOLS = ("stv",)

    def __init__(self):
        self.atom_node_types = OrderedSet([AtomType(symbol='Unknown', mtype=None), AtomType(symbol='Type', mtype=None)])
        self.atom_nodes = OrderedSet()

    @classmethod
    def build(cls, parsed_expressions):
        translator = cls()

        body = translator.translate(parsed_expressions)
        node_types = translator.atom_node_types
        nodes = translator.atom_nodes

        return MettaDocument(node_types=node_types, nodes=nodes, body=body)

    @property
    def ALLOWED_LINKS(self):
        return self._ALLOWED_LINKS + tuple(
            self.symbol_name2metta(symbol) for symbol in self._ALLOWED_LINKS
        )

    @property
    def ALLOWED_NODES(self):
        return self._ALLOWED_NODES + tuple(
            self.symbol_name2metta(symbol) for symbol in self._ALLOWED_NODES
        )

    def is_node(self, symbol: Symbol) -> bool:
        return isinstance(symbol, Symbol) and symbol in self.ALLOWED_NODES

    def is_link(self, symbol: Symbol) -> bool:
        return isinstance(symbol, Symbol) and symbol in self.ALLOWED_LINKS

    def is_ignored_symbol(self, symbol: Symbol) -> bool:
        return symbol in self.IGNORED_SYMBOLS

    @staticmethod
    def replace_nodesymbol(type_, value) -> Symbol:
        if re.match(r'^".*"$', value):
            return f'"{type_}:{value[1:]}'
        return f"{type_}:{value}"

    @staticmethod
    def symbol_name2metta(symbol) -> Symbol:
        return re.sub(r"\s*Node$|\s*Link$", "", symbol)

    def translate(self, expressions) -> Union[Symbol, Expression, None]:
        first = expressions[0]
        rest = expressions[1:]

        if isinstance(first, MutableSequence):
            return Expression(map(self.translate, expressions))
        elif isinstance(first, Symbol):
            mtype = self.symbol_name2metta(first)
            if self.is_node(first):
                if len(rest) > 1:
                    raise ValueError(f"Node rest len is greater than 1: {rest}")

                symbol = self.replace_nodesymbol(mtype, rest[0])

                self.atom_node_types.add(AtomType(mtype))
                self.atom_nodes.add(AtomType(symbol, mtype=mtype))

                return symbol
            elif self.is_link(first):
                if mtype in (MList.SYMBOL, MSet.SYMBOL):
                    if mtype == MList.SYMBOL:
                        return MList(map(self.translate, rest))
                    elif mtype == MSet.SYMBOL:
                        return MSet(map(self.translate, rest))
                else:
                    self.atom_node_types.add(AtomType(mtype))
                    return Expression(
                        [
                            mtype,
                            *map(
                                self.translate,
                                filter(lambda e: not self.is_ignored_symbol(e[0]), rest),
                            ),
                        ]
                    )
            else:
                raise InvalidSymbol(first)
        else:
            raise InvalidSymbol(first)


class MettaDocument:
    def __init__(self, node_types: Sequence[AtomType], nodes: Sequence[AtomType], body: Sequence[Expression]):
        self.node_types = node_types
        self.nodes = nodes
        self.body = body

    @property
    def expressions(self, skip_base_types=True) -> Iterable[BaseExpression]:
        for node_type in self.node_types:
            if node_type.type is None and skip_base_types: continue
            yield node_type
        for node in self.nodes:
            yield node
        for expression in self.body:
            yield expression

    @property
    def types(self):
        for node_type in self.node_types:
            yield node_type
        for node in self.nodes:
            yield node


    def write_to(self, file):
        for line in self.expressions:
            file.write(str(line))
            file.write("\n")

    def __str__(self):
        return "\n".join(str(expr) for expr in self.expressions)

    def __repr__(self):
        return f'{self.__class__.__name__}(types={repr(self.types)}, body={repr(self.body)})'

    def __add__(self, other):
        node_types = self.node_types.union(other.node_types)
        nodes = self.nodes.union(other.nodes)
        body = self.body + other.body
        return self.__class__(node_types=node_types, nodes=nodes, body=body)

    def __iadd__(self, other):
        return self + other
