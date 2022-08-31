import ply.yacc as yacc
from das.my_metta_lex import MettaLex

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
            | SET_OPENNING LIST_OF_EXPRESSIONS SET_CLOSING
            | EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR
            | EXPRESSION_NAME
            | ATOM_NAME
"""

class MettaYacc:

    def p_START(self, p):
        """START : LIST_OF_TOP_LEVEL_EXPRESSIONS EOF"""
        p[0] = 'SUCCESS'
        if self.no_actions_flag:
            return

    def p_LIST_OF_TOP_LEVEL_EXPRESSIONS_base(self, p):
        """LIST_OF_TOP_LEVEL_EXPRESSIONS : TOP_LEVEL_EXPRESSION"""
        p[0] = [p[1]]
        if self.no_actions_flag:
            return

    def p_LIST_OF_TOP_LEVEL_EXPRESSIONS_recursion(self, p):
        """LIST_OF_TOP_LEVEL_EXPRESSIONS :  LIST_OF_TOP_LEVEL_EXPRESSIONS TOP_LEVEL_EXPRESSION"""
        p[0] = [*p[1], p[2]]
        if self.no_actions_flag:
            return

    def p_TOP_LEVEL_EXPRESSION_type(self, p):
        """TOP_LEVEL_EXPRESSION : TOP_LEVEL_TYPE_DEFINITION"""
        p[0] = p[1]
        if self.no_actions_flag:
            return
        print(p[0])

    def p_TOP_LEVEL_EXPRESSION_expression(self, p):
        """TOP_LEVEL_EXPRESSION : TOP_LEVEL_EXPRESSION_DEFINITION"""
        p[0] = p[1]
        if self.no_actions_flag:
            return
        print(p[0])

    def p_TOP_LEVEL_TYPE_DEFINITION(self, p):
        """TOP_LEVEL_TYPE_DEFINITION : EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
                                     | EXPRESSION_OPENNING TYPE_DEFINITION_MARK ATOM_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING"""
        p[0] = f'Top-level <typedef {p[4]} {p[3]}>'
        if self.no_actions_flag:
            return

    def p_TYPE_DESIGNATOR_expression(self, p):
        """TYPE_DESIGNATOR : EXPRESSION_NAME"""
        p[0] = p[1]
        if self.no_actions_flag:
            return

    def p_TYPE_DESIGNATOR_basic(self, p):
        """TYPE_DESIGNATOR : BASIC_TYPE"""
        p[0] = p[1]
        if self.no_actions_flag:
            return

    def p_TOP_LEVEL_EXPRESSION_DEFINITION(self, p):
        """TOP_LEVEL_EXPRESSION_DEFINITION : EXPRESSION_OPENNING LIST_OF_EXPRESSIONS EXPRESSION_CLOSING"""
        p[0] = f'Top-level <expression {p[2]}>'
        if self.no_actions_flag:
            return

    def p_LIST_OF_EXPRESSIONS_base(self, p):
        """LIST_OF_EXPRESSIONS : EXPRESSION"""
        p[0] = [p[1]]
        if self.no_actions_flag:
            return
        
    def p_LIST_OF_EXPRESSIONS_recursion(self, p):
        """LIST_OF_EXPRESSIONS : LIST_OF_EXPRESSIONS EXPRESSION"""
        p[0] = [*p[1], p[2]]
        if self.no_actions_flag:
            return

    def p_EXPRESSION_sequence(self, p):
        """EXPRESSION : EXPRESSION_OPENNING LIST_OF_EXPRESSIONS EXPRESSION_CLOSING"""
        p[0] = f'<List {p[2]}>'
        if self.no_actions_flag:
            return

    def p_EXPRESSION_set(self, p):
        """EXPRESSION : SET_OPENNING LIST_OF_EXPRESSIONS SET_CLOSING"""
        p[0] = f'<Set {p[2]}>'
        if self.no_actions_flag:
            return

    def p_EXPRESSION_type(self, p):
        """EXPRESSION : EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
                      | EXPRESSION_OPENNING TYPE_DEFINITION_MARK ATOM_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING"""
        p[0] = f'<typedef {p[4]} {p[3]}>'
        if self.no_actions_flag:
            return

    def p_EXPRESSION_symbol(self, p):
        """EXPRESSION : EXPRESSION_NAME"""
        p[0] = p[1]
        if self.no_actions_flag:
            return

    def p_EXPRESSION_atom(self, p):
        """EXPRESSION : ATOM_NAME"""
        p[0] = p[1]
        if self.no_actions_flag:
            return

    def p_error(self, p):
        print(f"Syntax error in line {self.lexer.lineno}")
        print(f"Current token: {p} Current match: {self.lexer.lexmatch}")
        exit()

    def build(self, lex_wrap, **kwargs):
        self.tokens = lex_wrap.tokens
        self.lexer = lex_wrap.lexer
        self.parser = yacc.yacc(module=self, **kwargs)

    def parse(self, metta_string):
        self.no_actions_flag = False
        return self.parser.parse(metta_string)

    def check(self, metta_string):
        self.no_actions_flag = True
        return self.parser.parse(metta_string)
