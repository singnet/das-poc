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

class MettaYacc:

    ### Parser rules ###

    def p_START(self, p):
        """START : LIST_OF_TOP_LEVEL_EXPRESSIONS EOF
                 | EOF"""
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
        self.typedef_mark = ':'
        self.action_broker = kwargs.pop('action_broker', None)
        self.lex_wrap = MettaLex(**kwargs)
        self.tokens = self.lex_wrap.tokens
        self.lexer = self.lex_wrap.lexer
        self.parser = yacc.yacc(module=self, **kwargs)
        self.check_mode = False
        self.hasher = ExpressionHasher()
        self.pending_terminal_names = []
        self.pending_expression_names = []
        self.pending_named_types = []
        self.pending_expressions = []
        self.named_types = {}
        self.named_type_hash = {}
        self.symbol_hash = {}
        self.terminal_hash = {}
        self.parent_type = {}

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
            raise MettaSyntaxError(error)
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

    def eof_handler(self, t):
        if self.lexer.eof_reported_flag:
            if self.action_broker is None:
                return None
            self.lexer.eof_reported_flag = False
            next_input_chunk, file_name = self.action_broker.next_input_chunk()
            if next_input_chunk is None:
                return None
            else:
                self.lexer.lineno = 1
                self.lexer.file_name = file_name
                self.lexer.input(next_input_chunk)
                return self.lexer.token()
        else:
            self.lexer.input("")
            t.type = 'EOF'
            self.lexer.eof_reported_flag = True
            return t

    def parse(self, metta_string):
        self.file_name = ""
        return self.parser.parse(metta_string)

    def parse_action_broker_input(self):
        metta_string, file_name = self.action_broker.next_input_chunk()
        self.file_name = file_name
        return self.parser.parse(metta_string)

    def check(self, metta_string):
        self.file_name = ""
        self.check_mode = True
        answer = self.parser.parse(metta_string)
        self.check_mode = False
        return answer
