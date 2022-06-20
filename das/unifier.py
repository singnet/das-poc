import argparse
import os
from cmath import exp
from pprint import pprint
from typing import Any, List, Union

from atomese2metta.translator import (AtomType, Expression, MSet,
                                      UnorderedExpression)
from hashing import Hasher
from helpers import get_mongodb
from ply.lex import lex


class DAS:
    NODE_COLLS = [
        "node_types",
        "nodes",
    ]

    EXPRESSION_COLLS = [
        "links_1",
        "links_2",
        "links_3",
        "links",
    ]

    def __init__(self, db):
        self.db = db
        nodes = self.get_nodes()
        self.nodes = {
            node["_id"]: AtomType(node["name"], None, node["_id"]) for node in nodes
        }
        self.hasher = Hasher()
        for node in nodes:
            atom_type = self.nodes[node["_id"]]
            atom_type.type = self.nodes.get(node["type"], None)
            self.hasher.hash_atom_type(atom_type)

    def get_nodes(self):
        nodes = []
        for col_name in self.NODE_COLLS:
            col = self.db[col_name]
            nodes.extend(list(col.find({})))
        return nodes

    def get_expression_by_id(self, _id):
        if _id in self.nodes:
            return self.nodes[_id]
        expressions = []
        for coll_name in self.EXPRESSION_COLLS:
            expressions.extend(list(self.db[coll_name].find({"_id": _id})))

        if len(expressions) > 1:
            raise ValueError("expressions must have unique _id")

        if not expressions:
            return None

        return expressions[0]

    def match_expressions(self, expression):
        keys = [None if isinstance(e, Var) else e for e in expression]
        params = {
            f"key{i}": e._id for i, e in enumerate(keys, start=1) if e is not None
        }
        return map(self.obj2expression, self.db["links_3"].find(params))

    def query(self, query):
        return query.fetch(self)

    def obj2expression(self, obj):
        if "keys" in obj:
            keys = [self.get_expression_by_id[_id] for _id in obj["keys"]]
        else:
            keys = [
                self.get_expression_by_id(obj[k])
                for k in obj.keys()
                if k.startswith("key")
            ]
        return Expression(keys, _id=obj["_id"], is_root=obj["is_root"])
    

class TermLexError(Exception):
    pass


class MettaLex(object):
    # List of token names.   This is always required
    tokens = (
        "COLON",
        "LPAREN",
        "RPAREN",
        "LCBRACKET",
        "RCBRACKET",
        "STRING",
        "ID",
        "VAR",
    )

    # Regular expression rules for simple tokens
    t_COLON = r"\:"
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_LCBRACKET = r"\{"
    t_RCBRACKET = r"\}"

    def t_STRING(self, t):
        r'\"([^"]*?)\"'
        return t

    def t_ID(self, t):
        r"[a-zA-Z]+"
        return t

    def t_VAR(self, t):
        r"\$[a-zA-Z0-9]+"
        return t

    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r"\n+"
        t.lexer.lineno += len(t.value)

    # A string containing ignored characters (spaces and tabs)
    t_ignore = " \t"

    def t_error(self, t):
        raise AttributeError(
            "Illegal character '{}' at line {}".format(t.value[0], t.lexer.lineno)
        )
        # t.lexer.skip(1)

    def build(self, **kwargs):
        self.lexer = lex(module=self, **kwargs)

    def get_tokens(self, data: str):
        self.lexer.input(data)
        while True:
            tok = self.lexer.token()
            if not tok:
                break
            yield tok


class Term:
    pass


# In App, function names are always considered to be constants, not variables.
# This simplifies things and doesn't affect expressivity. We can always model
# variable functions by envisioning an apply(FUNCNAME, ... args ...).
class App(Term):
    def __init__(self, fname, args=()):
        self.fname = fname
        self.args = args

    def __str__(self):
        return "({0} {1})".format(self.fname, ",".join(map(str, self.args)))

    def __eq__(self, other):
        return (
            type(self) == type(other)
            and self.fname == other.fname
            and all(self.args[i] == other.args[i] for i in range(len(self.args)))
        )

    __repr__ = __str__


class Var(Term):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name

    __repr__ = __str__


class Const(Term):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value

    __repr__ = __str__


class ParseError(Exception):
    pass


def parse_expr(text) -> Expression:
    """Parses a expression from string text, returns an Expression."""
    term = [
        expr
        for e in list(MettaParser.parse(text))
        if e[0] == "EXPRESSION" and (expr := e[1]).is_root
    ]
    if len(term) > 1:
        raise ParseError("Must have only one Expression to parse")
    return term[0]


class MettaParser:
    LEX_CLASS = MettaLex
    HASHER_CLASS = Hasher
    NODE_TYPE = "NODE_TYPE"
    NODE = "NODE"
    EXPRESSION = "EXPRESSION"
    SET_FROM_REST = ("Similarity",)

    def __init__(self):
        self.lex = self.LEX_CLASS()
        self.lex.build()
        self.hasher = self.HASHER_CLASS()

    def _parse(self, text: str):
        list_stack: List[Any] = list()
        current: List[Union[AtomType, Expression]] = list()

        yield self.NODE_TYPE, AtomType(symbol="Unknown", mtype=None)
        yield self.NODE_TYPE, AtomType(symbol="Type", mtype=None)

        for token in self.lex.get_tokens(text):
            if token.type in ("LPAREN", "LCBRACKET"):
                pointer = []
                current.append(pointer)
                list_stack.append(current)
                current = pointer
            elif token.type in ("RPAREN", "RCBRACKET"):
                current = list_stack.pop()
                pointer = current.pop()
                if isinstance(pointer[0], str) and pointer[0] == ":":
                    _, symbol, type_ = pointer
                    type_ = self.hasher.search_by_name(type_)
                    atom_type = AtomType(symbol, type_)
                    if atom_type.type.symbol == "Type":
                        yield self.NODE_TYPE, atom_type
                    else:
                        yield self.NODE, atom_type
                else:
                    expression = []
                    for v in pointer:
                        if isinstance(v, str):
                            expression.append(self.hasher.search_by_name(v))
                        else:
                            expression.append(v)
                    if token.type == "RPAREN":
                        if (
                            isinstance(expression[0], AtomType)
                            and expression[0].symbol in self.SET_FROM_REST
                        ):
                            expression = UnorderedExpression(expression)
                        else:
                            expression = Expression(expression)
                    else:
                        expression = MSet(expression)

                    if len(list_stack) == 0:
                        expression.is_root = True
                    else:
                        current.append(expression)

                    yield self.EXPRESSION, expression
            elif token.type == "VAR":
                current.append(Var(token.value))
            else:
                current.append(token.value)

    @classmethod
    def parse(cls, text: str):
        return cls()._parse(text)


def occurs_check(v, term, subst):
    """Does the variable v occur anywhere inside term?

    Variables in term are looked up in subst and the check is applied
    recursively.
    """
    assert isinstance(v, Var)
    if v == term:
        return True
    elif isinstance(term, Var) and term.name in subst:
        return occurs_check(v, subst[term.name], subst)
    elif isinstance(term, Expression):
        return any(occurs_check(v, arg, subst) for arg in term)
    else:
        return False


def unify(x, y, subst):
    """Unifies term x and y with initial subst.

    Returns a subst (map of name->term) that unifies x and y, or None if
    they can't be unified. Pass subst={} if no subst are initially
    known. Note that {} means valid (but empty) subst.
    """
    if subst is None:
        return None
    elif x == y:
        return subst
    elif isinstance(x, Var):
        return unify_variable(x, y, subst)
    elif isinstance(y, Var):
        return unify_variable(y, x, subst)
    elif isinstance(x, Expression) and isinstance(y, Expression):
        if len(x) != len(y):
            return None
        else:
            for xi, yi in zip(x, y):
                subst = unify(xi, yi, subst)
            return subst
    else:
        return None


def unify_variable(v, x, subst):
    """Unifies variable v with term x, using subst.

    Returns updated subst or None on failure.
    """
    assert isinstance(v, Var)
    if v.name in subst:
        return unify(subst[v.name], x, subst)
    elif isinstance(x, Var) and x.name in subst:
        return unify(v, subst[x.name], subst)
    elif occurs_check(v, x, subst):
        return None
    else:
        # v is not yet in subst and can't simplify x. Extend subst.
        return {**subst, v.name: x}


def apply_unifier(x, subst):
    """Applies the unifier subst to term x.
    Returns a term where all occurrences of variables bound in subst
    were replaced (recursively); on failure returns None.
    """
    if subst is None:
        return None
    elif len(subst) == 0:
        return x
    elif isinstance(x, AtomType):
        return x
    elif isinstance(x, Var):
        if x.name in subst:
            return apply_unifier(subst[x.name], subst)
        else:
            return x
    elif isinstance(x, Expression):
        newargs = [apply_unifier(arg, subst) for arg in x]
        return Expression(newargs)
    else:
        return None


env = {}


def unify_m(expr, iterable, subst):
    return [unify(expr, i, subst) for i in iterable]


def main(mongodb_specs):
    db = get_mongodb(mongodb_specs)
    das = DAS(db)

    #  env["das"] = das

    text_1 = '(Inheritance $1 $2)'
    expression_1 = parse_expr(text_1)
    match_1 = list(das.match_expressions(expression_1))


    text_2 = '(Inheritance $2 $3)'
    expression_2 = parse_expr(text_2)


    subst_m = unify_m(expression_1, match_1, {})
    matches = []
    for subst in subst_m:
        expr = apply_unifier(expression_2, subst)
        m2 = list(das.match_expressions(expr))
        res = unify_m(expression_2, m2, subst)
        if any(res):
            matches.extend([e for e in res if e is not None])

    pprint([{k:v.symbol for k, v in m.items()} for m in matches])


def run():
    parser = argparse.ArgumentParser(
        "Unifying", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--hostname", help="mongo hostname to connect to")
    parser.add_argument("--port", help="mongo port to connect to")
    parser.add_argument("--username", help="mongo username")
    parser.add_argument("--password", help="mongo password")
    parser.add_argument("--database", "-d", help="mongo database name to connect to")

    args = parser.parse_args()

    mongodb_specs = {
        "hostname": args.hostname
        or os.environ.get("DAS_MONGODB_HOSTNAME", "localhost"),
        "port": args.port or os.environ.get("DAS_MONGODB_PORT", 27017),
        "username": args.username or os.environ.get("DAS_DATABASE_USERNAME", "dbadmin"),
        "password": args.password
        or os.environ.get("DAS_DATABASE_PASSWORD", "das#secret"),
        "database": args.database or os.environ.get("DAS_DATABASE_NAME", "das"),
    }

    main(mongodb_specs)


if __name__ == "__main__":
    run()
