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
from das.expression_hasher import ExpressionHasher
from das.expression import Expression
from das.metta_lex import BASIC_TYPE

class BaseYacc:

    def __init__(self, **kwargs):
        self.tokens = None
        self.lexer = None
        self.typedef_mark = ':'
        self.action_broker = kwargs.pop('action_broker', None)
        self.check_mode = False
        self.hasher = ExpressionHasher()
        self.pending_terminal_names = []
        self.pending_expression_names = []
        self.pending_named_types = []
        self.pending_expressions = []
        if kwargs.pop('use_action_broker_cache', False):
            self.named_type_hash = self.action_broker.named_type_hash
            self.named_types = self.action_broker.named_types
            self.symbol_hash = self.action_broker.symbol_hash
            self.terminal_hash = self.action_broker.terminal_hash
            self.parent_type = self.action_broker.parent_type
        else:
            self.named_type_hash = {}
            self.named_types = {}
            self.symbol_hash = {}
            self.terminal_hash = {}
            self.parent_type = {}
        basic_type_hash_code = ExpressionHasher._compute_hash(BASIC_TYPE)
        self.named_type_hash[BASIC_TYPE] = basic_type_hash_code
        self.parent_type[basic_type_hash_code] = basic_type_hash_code

    def setup(self):
        self.tokens = self.lex_wrap.tokens
        self.lexer = self.lex_wrap.lexer
        if self.action_broker is not None:
            expression = self._typedef(BASIC_TYPE, BASIC_TYPE)
            self.action_broker.new_top_level_typedef_expression(expression)
        
    def _get_terminal_hash(self, named_type, terminal_name):
        key = (named_type, terminal_name)
        terminal_hash = self.terminal_hash.get(key, None)
        if terminal_hash is None:
            terminal_hash = self.hasher.terminal_hash(*key)
            self.terminal_hash[key] = terminal_hash
        return terminal_hash

    def _get_named_type_hash(self, named_type):
        named_type_hash = self.named_type_hash.get(named_type, None)
        if named_type_hash is None:
            named_type_hash = self.hasher.named_type_hash(named_type)
            self.named_type_hash[named_type] = named_type_hash
        return named_type_hash

    def _nested_expression(self, sub_expressions, expression=None):
        if expression is None:
            expression = Expression()
        if any(sub_expression.hash_code is None for sub_expression in sub_expressions):
            self.pending_expressions.append((sub_expressions, expression))
            return expression
        if sub_expressions[0].named_type is not None:
            expression.named_type = sub_expressions[0].named_type
            expression.named_type_hash = sub_expressions[0].named_type_hash
            expression.composite_type = [
                sub_expression.composite_type \
                    if len(sub_expression.composite_type) > 1 \
                    else sub_expression.composite_type[0] \
                for sub_expression in sub_expressions]
            hashes = [sub_expression.composite_type_hash for sub_expression in sub_expressions]
            expression.composite_type_hash = self.hasher.composite_hash(hashes)
            expression.elements = [sub_expression.hash_code for sub_expression in sub_expressions[1:]]
            expression.hash_code = self.hasher.expression_hash(expression.named_type_hash, expression.elements)
        else:
            error = f"Syntax error in line {self.lexer.lineno} " + \
                    f"Non-typed expressions are not supported yet"
            print(error)
            assert False
        return expression

    def _typedef(self, name, type_designator, expression=None):
        assert name is not None
        assert type_designator is not None
        if expression is None:
            expression = Expression()
        type_designator_hash = self.named_type_hash.get(type_designator, None)
        if type_designator_hash is not None:
            named_type_hash = self._get_named_type_hash(name)
            typedef_mark_hash = self._get_named_type_hash(self.typedef_mark)
            self.parent_type[named_type_hash] = type_designator_hash
            self.named_types[name] = type_designator
            expression.typedef_name = name
            expression.typedef_name_hash = self._get_named_type_hash(name)
            expression.named_type = self.typedef_mark
            expression.named_type_hash = typedef_mark_hash
            expression.composite_type = [typedef_mark_hash, type_designator_hash, self.parent_type[type_designator_hash]]
            expression.composite_type_hash = self.hasher.composite_hash(expression.composite_type)
            expression.elements = [named_type_hash, type_designator_hash]
            expression.hash_code = self.hasher.expression_hash(expression.named_type_hash, expression.elements)
            self.symbol_hash[name] = expression.hash_code
        else:
            self.pending_named_types.append(((name, type_designator), expression))
        return expression

    def _new_terminal(self, terminal_name, expression=None):
        if expression is None:
            expression = Expression(terminal_name=terminal_name)
        named_type = self.named_types.get(terminal_name, None)
        if named_type:
            named_type_hash = self._get_named_type_hash(named_type)
            expression.named_type = named_type
            expression.named_type_hash = named_type_hash
            expression.composite_type = [named_type_hash]
            expression.composite_type_hash = named_type_hash
            expression.hash_code = self._get_terminal_hash(named_type, terminal_name)
        else:
            self.pending_terminal_names.append((terminal_name, expression))
        return expression

    def _new_symbol(self, expression_name, expression=None):
        if expression is None:
            expression = Expression()
        named_type = self.named_types.get(expression_name, None)
        if named_type:
            named_type_hash = self._get_named_type_hash(expression_name)
            expression.symbol_name = expression_name
            expression.named_type = expression_name
            expression.named_type_hash = named_type_hash
            expression.composite_type = [named_type_hash]
            expression.composite_type_hash = named_type_hash
            expression.hash_code = self.symbol_hash[expression_name]
        else:
            self.pending_expression_names.append((expression_name, expression))
        return expression

    def _revisit_pending_named_types(self):
        pending = self.pending_named_types
        self.pending_named_types = []
        dirty_flag = False
        for ((name, type_designator), expression) in pending:
            modified_expression = self._typedef(name, type_designator, expression)
            if modified_expression.hash_code is not None:
                dirty_flag = True
        return dirty_flag

    def _revisit_pending_terminal_names(self):
        pending = self.pending_terminal_names
        self.pending_terminal_names = []
        for (terminal_name, expression) in pending:
            modified_expression = self._new_terminal(terminal_name, expression)

    def _revisit_pending_expression_names(self):
        pending = self.pending_expression_names
        self.pending_expression_names = []
        for (expression_name, expression) in pending:
            modified_expression = self._new_symbol(expression_name, expression)

    def _revisit_pending_expressions(self):
        pending = self.pending_expressions
        self.pending_expressions = []
        dirty_flag = False
        for (sub_expressions, expression) in pending:
            modified_expression = self._nested_expression(sub_expressions, expression)
            if modified_expression.hash_code is not None:
                dirty_flag = True
        return dirty_flag

    def _revisit_pending_symbols(self):
        while self._revisit_pending_named_types():
            pass
        self._revisit_pending_terminal_names()
        self._revisit_pending_expression_names()
        while self._revisit_pending_expressions():
            pass

    def parse(self, input_string):
        self.file_name = ""
        return self.parser.parse(input_string)

    def parse_action_broker_input(self):
        self.file_name = self.action_broker.file_path
        input_string = self.action_broker.input_string
        return self.parser.parse(input_string)

    def check(self, input_string):
        self.file_name = ""
        self.check_mode = True
        answer = self.parser.parse(input_string)
        self.check_mode = False
        return answer
