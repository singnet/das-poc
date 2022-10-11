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
from das.metta_lex import BASIC_TYPE
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

    def p_LIST_OF_TOP_LEVEL_ATOMS_base(self, p):
        """LIST_OF_TOP_LEVEL_ATOMS : TOP_LEVEL_ATOM"""
        p[0] = [p[1]]
        if self.check_mode or not self.action_broker:
            return

    def p_LIST_OF_TOP_LEVEL_ATOMS_recursion(self, p):
        """LIST_OF_TOP_LEVEL_ATOMS : LIST_OF_TOP_LEVEL_ATOMS TOP_LEVEL_ATOM"""
        p[0] = [*p[1], p[2]]

    def p_TOP_LEVEL_ATOM(self, p):
        """TOP_LEVEL_ATOM : ATOM"""
        atom = p[1]
        p[0] = p[1]
        if self.check_mode or not self.action_broker:
            return
        if atom.elements is not None:
            atom.toplevel = True
            self.action_broker.new_top_level_expression(atom)

    def p_ATOM_node(self, p):
        """ATOM : NODE"""
        p[0] = p[1]

    def p_ATOM_link(self, p):
        """ATOM : LINK"""
        p[0] = p[1]

    def p_NODE(self, p):
        """NODE : ATOM_OPENNING ATOM_TYPE NODE_NAME ATOM_CLOSING"""
        if self.check_mode or not self.action_broker:
            p[0] = f"<{p[2]}: {p[3]}>"
            return
        node_type = p[2]
        node_name = p[3]
        if node_type not in self.types:
            self.types.add(node_type)
            expression = self._typedef(node_type, BASIC_TYPE)
            expression.toplevel = True
            self.action_broker.new_top_level_typedef_expression(expression)
        terminal_name = f"{node_type}:{node_name}"
        if terminal_name not in self.nodes:
            self.nodes.add(terminal_name)
            expression = self._typedef(terminal_name, node_type)
            expression.toplevel = True
            self.action_broker.new_top_level_typedef_expression(expression)
            expression = self._new_terminal(terminal_name)
            self.action_broker.new_terminal(expression)
        else:
            expression = self._new_terminal(terminal_name)
        p[0] = expression

    def p_LINK(self, p):
        """LINK : ATOM_OPENNING ATOM_TYPE ATOM_LIST ATOM_CLOSING"""
        link_type = p[2]
        targets = p[3]
        if self.check_mode or not self.action_broker:
            p[0] = f"<{p[2]}: {p[3]}>"
            return
        if link_type not in self.types:
            self.types.add(link_type)
            expression = self._typedef(link_type, BASIC_TYPE)
            expression.toplevel = True
            self.action_broker.new_top_level_typedef_expression(expression)
        head_expression = self._new_symbol(link_type)
        expression = self._nested_expression([head_expression, *targets])
        p[0] = expression

    def p_ATOM_LIST_base(self, p):
        """ATOM_LIST : ATOM"""
        atom = p[1]
        p[0] = [atom]
        if self.check_mode or not self.action_broker:
            return
        if atom.elements is not None:
            self.action_broker.new_expression(atom)

    def p_ATOM_LIST_recursion(self, p):
        """ATOM_LIST : ATOM_LIST ATOM"""
        atom = p[2]
        p[0] = [*p[1], atom]
        if self.check_mode or not self.action_broker:
            return
        if atom.elements is not None:
            self.action_broker.new_expression(atom)

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
        self.types = set()
        self.nodes = set()
        named_type_hash = self._get_named_type_hash(BASIC_TYPE)
        self.parent_type[named_type_hash] = named_type_hash
