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
            | ATOM_NAME
"""

import ply.yacc as yacc
from das.my_metta_lex import MettaLex
from das.exceptions import MettaSyntaxError

class MettaYacc:

    def p_START(self, p):
        """START : LIST_OF_TOP_LEVEL_EXPRESSIONS EOF
                 | EOF"""
        p[0] = 'SUCCESS'
        if self.check_mode or not self.action_broker:
            return

    def p_LIST_OF_TOP_LEVEL_EXPRESSIONS_base(self, p):
        """LIST_OF_TOP_LEVEL_EXPRESSIONS : TOP_LEVEL_EXPRESSION"""
        p[0] = [p[1]]
        if self.check_mode or not self.action_broker:
            return

    def p_LIST_OF_TOP_LEVEL_EXPRESSIONS_recursion(self, p):
        """LIST_OF_TOP_LEVEL_EXPRESSIONS :  LIST_OF_TOP_LEVEL_EXPRESSIONS TOP_LEVEL_EXPRESSION"""
        p[0] = [*p[1], p[2]]
        if self.check_mode or not self.action_broker:
            return

    def p_TOP_LEVEL_EXPRESSION_type(self, p):
        """TOP_LEVEL_EXPRESSION : TOP_LEVEL_TYPE_DEFINITION"""
        p[0] = p[1]
        if self.check_mode or not self.action_broker:
            return
        self.action_broker.new_top_level_typedef_expression(p[0])

    def p_TOP_LEVEL_EXPRESSION_expression(self, p):
        """TOP_LEVEL_EXPRESSION : TOP_LEVEL_EXPRESSION_DEFINITION"""
        p[0] = p[1]
        if self.check_mode or not self.action_broker:
            return
        self.action_broker.new_top_level_expression(p[0])

    def p_TOP_LEVEL_TYPE_DEFINITION(self, p):
        """TOP_LEVEL_TYPE_DEFINITION : EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
                                     | EXPRESSION_OPENNING TYPE_DEFINITION_MARK ATOM_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING"""
        p[0] = f'Top-level <typedef {p[4]} {p[3]}>'
        if self.check_mode or not self.action_broker:
            return

    def p_TYPE_DESIGNATOR_expression(self, p):
        """TYPE_DESIGNATOR : EXPRESSION_NAME"""
        p[0] = p[1]
        if self.check_mode or not self.action_broker:
            return

    def p_TYPE_DESIGNATOR_basic(self, p):
        """TYPE_DESIGNATOR : BASIC_TYPE"""
        p[0] = p[1]
        if self.check_mode or not self.action_broker:
            return

    def p_TOP_LEVEL_EXPRESSION_DEFINITION(self, p):
        """TOP_LEVEL_EXPRESSION_DEFINITION : EXPRESSION_OPENNING LIST_OF_EXPRESSIONS EXPRESSION_CLOSING"""
        p[0] = f'Top-level <expression {p[2]}>'
        if self.check_mode or not self.action_broker:
            return

    def p_LIST_OF_EXPRESSIONS_base(self, p):
        """LIST_OF_EXPRESSIONS : EXPRESSION"""
        p[0] = [p[1]]
        if self.check_mode or not self.action_broker:
            return
        
    def p_LIST_OF_EXPRESSIONS_recursion(self, p):
        """LIST_OF_EXPRESSIONS : LIST_OF_EXPRESSIONS EXPRESSION"""
        p[0] = [*p[1], p[2]]
        if self.check_mode or not self.action_broker:
            return

    def p_EXPRESSION_sequence(self, p):
        """EXPRESSION : EXPRESSION_OPENNING LIST_OF_EXPRESSIONS EXPRESSION_CLOSING"""
        p[0] = f'<List {p[2]}>'
        if self.check_mode or not self.action_broker:
            return

    def p_EXPRESSION_type(self, p):
        """EXPRESSION : EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
                      | EXPRESSION_OPENNING TYPE_DEFINITION_MARK ATOM_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING"""
        p[0] = f'<typedef {p[4]} {p[3]}>'
        if self.check_mode or not self.action_broker:
            return

    def p_EXPRESSION_symbol(self, p):
        """EXPRESSION : EXPRESSION_NAME"""
        p[0] = p[1]
        if self.check_mode or not self.action_broker:
            return

    def p_EXPRESSION_atom(self, p):
        """EXPRESSION : ATOM_NAME"""
        p[0] = p[1]
        if self.check_mode or not self.action_broker:
            return

    def p_error(self, p):
        error = f"Syntax error in line {self.lexer.lineno} " + \
                f"Current token: {p}"
        raise MettaSyntaxError(error)

    def __init__(self, **kwargs):
        self.action_broker = kwargs.pop('action_broker', None)
        self.lex_wrap = MettaLex(**kwargs)
        self.tokens = self.lex_wrap.tokens
        self.lexer = self.lex_wrap.lexer
        self.parser = yacc.yacc(module=self, **kwargs)
        self.check_mode = False

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
