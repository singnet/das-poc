"""
START -> LIST_OF_TOP_LEVEL_EXPRESSIONS

LIST_OF_TOP_LEVEL_EXPRESSIONS -> TOP_LEVEL_EXPRESSION
    | LIST_OF_TOP_LEVEL_EXPRESSIONS TOP_LEVEL_EXPRESSION

TOP_LEVEL_EXPRESSION -> TOP_LEVEL_TYPE_DEFINITION
    | TOP_LEVEL_EXPRESSION_DEFINITION

TOP_LEVEL_TYPE_DEFINITION -> EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING

TYPE_DESIGNATOR -> EXPRESSION_NAME
                 | BASIC_TYPE

TOP_LEVEL_EXPRESSION_DEFINITION -> EXPRESSION_OPENNING LIST_OF_EXPRESSIONS EXPRESSION_CLOSING

LIST_OF_EXPRESSIONS -> EXPRESSION
                     | LIST_OF_EXPRESSIONS EXPRESSION

EXPRESSION -> EXPRESSION_OPENNING LIST_OF_EXPRESSIONS EXPRESSION_CLOSING
            | EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
            | EXPRESSION_NAME
            | TERMINAL_NAME
"""

from typing import List, Any, Optional
import ply.yacc as yacc
from das.metta_lex import MettaLex
from das.exceptions import MettaSyntaxError, UndefinedSymbolError
from das.expression_hasher import ExpressionHasher
from das.expression import Expression
from das.base_yacc import BaseYacc

class MettaYacc(BaseYacc):

    ### Parser rules ###

    def p_START(self, p):
        """START : LIST_OF_TOP_LEVEL_EXPRESSIONS EOF
                 | EOF
                 |"""
        self._revisit_pending_symbols()
        missing_symbols = []
        missing_symbols.extend([name for (name, expression) in self.pending_terminal_names])
        missing_symbols.extend([name for (name, expression) in self.pending_expression_names])
        missing_symbols.extend([type_designator for ((name, type_designator), expression) in self.pending_named_types])
        if missing_symbols:
            raise UndefinedSymbolError(list(set(missing_symbols)))
        assert not self.pending_expressions
        p[0] = 'SUCCESS'

    def p_LIST_OF_TOP_LEVEL_EXPRESSIONS_base(self, p):
        """LIST_OF_TOP_LEVEL_EXPRESSIONS : TOP_LEVEL_EXPRESSION"""
        if self.check_mode or not self.action_broker:
            return
        p[0] = [p[1]]

    def p_LIST_OF_TOP_LEVEL_EXPRESSIONS_recursion(self, p):
        """LIST_OF_TOP_LEVEL_EXPRESSIONS :  LIST_OF_TOP_LEVEL_EXPRESSIONS TOP_LEVEL_EXPRESSION"""
        if self.check_mode or not self.action_broker:
            return
        p[0] = [*p[1], p[2]]

    def p_TOP_LEVEL_EXPRESSION_type(self, p):
        """TOP_LEVEL_EXPRESSION : TOP_LEVEL_TYPE_DEFINITION"""
        if self.check_mode or not self.action_broker:
            return
        p[0] = p[1]

    def p_TOP_LEVEL_EXPRESSION_expression(self, p):
        """TOP_LEVEL_EXPRESSION : TOP_LEVEL_EXPRESSION_DEFINITION"""
        if self.check_mode or not self.action_broker:
            return
        p[0] = p[1]
            

    def p_TOP_LEVEL_TYPE_DEFINITION(self, p):
        """TOP_LEVEL_TYPE_DEFINITION : EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
                                     | EXPRESSION_OPENNING TYPE_DEFINITION_MARK TERMINAL_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING"""
        if self.check_mode or not self.action_broker:
            return
        assert self.typedef_mark == p[2]
        name = p[3]
        type_designator = p[4]
        expression = self._typedef(name, type_designator)
        expression.toplevel = True
        self.action_broker.new_top_level_typedef_expression(expression)
        p[0] = expression

    def p_TYPE_DESIGNATOR_expression(self, p):
        """TYPE_DESIGNATOR : EXPRESSION_NAME"""
        if self.check_mode or not self.action_broker:
            return
        p[0] = p[1]

    def p_TYPE_DESIGNATOR_basic(self, p):
        """TYPE_DESIGNATOR : BASIC_TYPE"""
        if self.check_mode or not self.action_broker:
            return
        name = p[1]
        named_type_hash = self._get_named_type_hash(name)
        self.parent_type[named_type_hash] = named_type_hash
        p[0] = name

    def p_TOP_LEVEL_EXPRESSION_DEFINITION(self, p):
        """TOP_LEVEL_EXPRESSION_DEFINITION : EXPRESSION_OPENNING LIST_OF_EXPRESSIONS EXPRESSION_CLOSING"""
        if self.check_mode or not self.action_broker:
            return
        sub_expressions = p[2]
        expression = self._nested_expression(sub_expressions)
        expression.toplevel = True
        self.action_broker.update_line_number(self.lexer.lineno)
        self.action_broker.new_top_level_expression(expression)
        p[0] = expression
        

    def p_LIST_OF_EXPRESSIONS_base(self, p):
        """LIST_OF_EXPRESSIONS : EXPRESSION"""
        if self.check_mode or not self.action_broker:
            return
        p[0] = [p[1]]
        
    def p_LIST_OF_EXPRESSIONS_recursion(self, p):
        """LIST_OF_EXPRESSIONS : LIST_OF_EXPRESSIONS EXPRESSION"""
        if self.check_mode or not self.action_broker:
            return
        p[0] = [*p[1], p[2]]

    def p_EXPRESSION_sequence(self, p):
        """EXPRESSION : EXPRESSION_OPENNING LIST_OF_EXPRESSIONS EXPRESSION_CLOSING"""
        if self.check_mode or not self.action_broker:
            return
        sub_expressions = p[2]
        expression = self._nested_expression(sub_expressions)
        self.action_broker.new_expression(expression)
        p[0] = expression

    def p_EXPRESSION_type(self, p):
        """EXPRESSION : EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
                      | EXPRESSION_OPENNING TYPE_DEFINITION_MARK TERMINAL_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING"""
        if self.check_mode or not self.action_broker:
            return
        assert self.typedef_mark == p[2]
        name = p[3]
        type_designator = p[4]
        expression = self._typedef(name, type_designator)
        error = f"Error in line {self.lexer.lineno} " + \
                f"Invalid nested type definition: {name}. " + \
                f"Current token: {p}"
        raise MettaSyntaxError(error)
        

    def p_EXPRESSION_symbol(self, p):
        """EXPRESSION : EXPRESSION_NAME"""
        if self.check_mode or not self.action_broker:
            return
        expression_name = p[1]
        expression = self._new_symbol(expression_name)
        p[0] = expression

    def p_EXPRESSION_terminal(self, p):
        """EXPRESSION : TERMINAL_NAME"""
        if self.check_mode or not self.action_broker:
            return
        terminal_name = p[1]
        expression = self._new_terminal(terminal_name)
        self.action_broker.new_terminal(expression)
        p[0] = expression

    def p_error(self, p):
        error = f"Syntax error in line {self.lexer.lineno} " + \
                f"Current token: {p}"
        raise MettaSyntaxError(error)

    ### End of parser rules ###

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lex_wrap = MettaLex()
        super().setup()
        self.parser = yacc.yacc(module=self)
