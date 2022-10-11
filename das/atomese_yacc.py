"""
START -> LIST_OF_TOP_LEVEL_ATOMS

LIST_OF_TOP_LEVEL_ATOMS -> TOP_LEVEL_ATOM
    | LIST_OF_TOP_LEVEL_ATOMS TOP_LEVEL_ATOM

TOP_LEVEL_ATOM -> ATOM

ATOM -> NODE
    | LINK

NODE -> ATOM_OPENNING ATOM_TYPE NODE_NAME ATOM_CLOSING

LINK -> ATOM_OPENNING ATOM_TYPE ATOM_LIST ATOM_CLOSING

ATOM_LIST -> ATOM
    | ATOM_LIST ATOM

"""

from typing import List, Any, Optional
import ply.yacc as yacc
from das.atomese_lex import AtomeseLex
from das.exceptions import AtomeseSyntaxError, UndefinedSymbolError
from das.expression_hasher import ExpressionHasher
from das.expression import Expression
from das.base_yacc import BaseYacc

class AtomeseYacc(BaseYacc):

    ### Parser rules ###

    def p_START(self, p):
        """START : LIST_OF_TOP_LEVEL_ATOMS EOF
                 | EOF
                 |"""
        p[0] = 'SUCCESS'
        print(f"p_START: {p[0]}")

    def p_LIST_OF_TOP_LEVEL_ATOMS_base(self, p):
        """LIST_OF_TOP_LEVEL_ATOMS : TOP_LEVEL_ATOM"""
        #if self.check_mode or not self.action_broker:
        #    return
        p[0] = [p[1]]
        print(f"p_LIST_OF_TOP_LEVEL_ATOMS_base: {p[0]}")

    def p_LIST_OF_TOP_LEVEL_ATOMS_recursion(self, p):
        """LIST_OF_TOP_LEVEL_ATOMS : LIST_OF_TOP_LEVEL_ATOMS TOP_LEVEL_ATOM"""
        p[0] = [*p[1], p[2]]
        print(f"p_LIST_OF_TOP_LEVEL_ATOMS_recursion: {p[0]}")

    def p_TOP_LEVEL_ATOM(self, p):
        """TOP_LEVEL_ATOM : ATOM"""
        p[0] = p[1]
        print(f"p_TOP_LEVEL_ATOM: {p[0]}")

    def p_ATOM_node(self, p):
        """ATOM : NODE"""
        p[0] = p[1]
        print(f"p_ATOM_node: {p[0]}")

    def p_ATOM_link(self, p):
        """ATOM : LINK"""
        p[0] = p[1]
        print(f"p_ATOM_link: {p[0]}")

    def p_NODE(self, p):
        """NODE : ATOM_OPENNING ATOM_TYPE NODE_NAME ATOM_CLOSING"""
        p[0] = f"<{p[2]}: {p[3]}>"
        print(f"p_NODE: {p[0]}")

    def p_LINK(self, p):
        """LINK : ATOM_OPENNING ATOM_TYPE ATOM_LIST ATOM_CLOSING"""
        p[0] = f"({p[2]} {p[3]})"
        print(f"p_LINK: {p[0]}")

    def p_ATOM_LIST_base(self, p):
        """ATOM_LIST : ATOM"""
        p[0] = [p[1]]
        print(f"p_ATOM_LIST_base: {p[0]}")

    def p_ATOM_LIST_recursion(self, p):
        """ATOM_LIST : ATOM_LIST ATOM"""
        p[0] = [*p[1], p[2]]
        print(f"p_ATOM_LIST_recursion: {p[0]}")

    def p_error(self, p):
        error = f"Syntax error in line {self.lexer.lineno} " + \
                f"Current token: {p}"
        raise AtomeseSyntaxError(error)

    ### End of parser rules ###

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lex_wrap = AtomeseLex()
        super().setup()
        self.parser = yacc.yacc(module=self)
