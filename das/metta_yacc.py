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

from dataclasses import dataclass
from typing import List, Any, Optional
import ply.yacc as yacc
from das.my_metta_lex import MettaLex
from das.exceptions import MettaSyntaxError
from das.expression_hasher import ExpressionHasher

@dataclass
class Expression:
    toplevel: bool = False
    ordered: bool = True
    atom_name: Optional[str] = None
    named_type: Optional[str] = None
    named_type_hash: Optional[str] = None
    composite_type: Optional[List[Any]] = None
    composite_type_hash: Optional[str] = None
    elements: Optional[List['Expression']] = None
    hash_code: Optional[str] = None

class MettaYacc:

    ### Parser rules ###

    def p_START(self, p):
        """START : LIST_OF_TOP_LEVEL_EXPRESSIONS EOF
                 | EOF"""
        p[0] = 'SUCCESS'
        if self.check_mode or not self.action_broker:
            return

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
        self.action_broker.new_top_level_typedef_expression(p[0])
        p[0] = p[1]

    def p_TOP_LEVEL_EXPRESSION_expression(self, p):
        """TOP_LEVEL_EXPRESSION : TOP_LEVEL_EXPRESSION_DEFINITION"""
        if self.check_mode or not self.action_broker:
            return
        self.action_broker.new_top_level_expression(p[0])
        p[0] = p[1]
            

    def p_TOP_LEVEL_TYPE_DEFINITION(self, p):
        """TOP_LEVEL_TYPE_DEFINITION : EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
                                     | EXPRESSION_OPENNING TYPE_DEFINITION_MARK ATOM_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING"""
        if self.check_mode or not self.action_broker:
            return
        assert self.typedef_mark == p[2]
        name = p[3]
        type_designator = p[4]
        new_expression = self._typedef(name, type_designator)
        new_expression.toplevel = True
        p[0] = new_expression

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
        new_expression = self._nested_expression(sub_expressions)
        new_expression.toplevel = True
        p[0] = new_expression
        

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
        new_expression = self._nested_expression(sub_expressions)
        p[0] = new_expression

    def p_EXPRESSION_type(self, p):
        """EXPRESSION : EXPRESSION_OPENNING TYPE_DEFINITION_MARK EXPRESSION_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING
                      | EXPRESSION_OPENNING TYPE_DEFINITION_MARK ATOM_NAME TYPE_DESIGNATOR EXPRESSION_CLOSING"""
        if self.check_mode or not self.action_broker:
            return
        assert self.typedef_mark == p[2]
        name = p[3]
        type_designator = p[4]
        new_expression = self._typedef(name, type_designator)
        p[0] = new_expression
        

    def p_EXPRESSION_symbol(self, p):
        """EXPRESSION : EXPRESSION_NAME"""
        if self.check_mode or not self.action_broker:
            return
        expression_name = p[1]
        named_type = self.named_types.get(expression_name, None)
        new_expression = Expression()
        if named_type:
            named_type_hash = self._get_named_type_hash(named_type)
            new_expression.named_type = named_type
            new_expression.named_type_hash = named_type_hash
            new_expression.composite_type = [named_type_hash]
            new_expression.composite_type_hash = named_type_hash
            new_expression.hash_code = self.symbol_hash[expression_name]
        else:
            self.pending_expression_names.append(expression_name)
        p[0] = new_expression

    def p_EXPRESSION_atom(self, p):
        """EXPRESSION : ATOM_NAME"""
        if self.check_mode or not self.action_broker:
            return
        atom_name = p[1]
        named_type = self.named_types.get(atom_name, None)
        new_expression = Expression(atom_name=atom_name)
        if named_type:
            named_type_hash = self._get_named_type_hash(named_type)
            new_expression.named_type = named_type
            new_expression.named_type_hash = named_type_hash
            new_expression.composite_type = [named_type_hash]
            new_expression.composite_type_hash = named_type_hash
            new_expression.hash_code = self._get_atom_hash(named_type, atom_name)
        else:
            self.pending_atom_names.append(atom_name)
        p[0] = new_expression

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
        self.pending_atom_names = []
        self.pending_expression_names = []
        self.pending_named_types = []
        self.named_types = {}
        self.named_type_hash = {}
        self.symbol_hash = {}
        self.atom_hash = {}
        self.parent_type = {}

    def _get_atom_hash(self, named_type, atom_name):
        key = (named_type, atom_name)
        atom_hash = self.atom_hash.get(key, None)
        if atom_hash is None:
            atom_hash = self.hasher.atom_hash(*key)
            self.atom_hash[key] = atom_hash
        return atom_hash

    def _get_named_type_hash(self, named_type):
        named_type_hash = self.named_type_hash.get(named_type, None)
        if named_type_hash is None:
            named_type_hash = self.hasher.named_type_hash(named_type)
            self.named_type_hash[named_type] = named_type_hash
        return named_type_hash

    def _nested_expression(self, sub_expressions):
        new_expression = Expression()
        if sub_expressions[0].named_type is not None:
            new_expression.named_type = sub_expressions[0].named_type
            new_expression.named_type_hash = sub_expressions[0].named_type_hash
            new_expression.composite_type = [sub_expression.composite_type for sub_expression in sub_expressions]
            hashes = [sub_expression.composite_type_hash for sub_expression in sub_expressions]
            new_expression.composite_type_hash = self.hasher.composite_hash(hashes)
            new_expression.elements = [sub_expression.hash_code for sub_expression in sub_expressions]
            new_expression.hash_code = self.hasher.expression_hash(new_expression.named_type_hash, new_expression.elements)
        else:
            error = f"Syntax error in line {self.lexer.lineno} " + \
                    f"Non-typed expressions are not supported yet"
            raise MettaSyntaxError(error)
        return new_expression

    def _typedef(self, name, type_designator):
        assert name is not None
        assert type_designator is not None
        new_expression = Expression()
        type_designator_hash = self.named_type_hash.get(type_designator, None)
        if type_designator_hash is not None:
            named_type_hash = self._get_named_type_hash(name)
            typedef_mark_hash = self._get_named_type_hash(self.typedef_mark)
            self.parent_type[named_type_hash] = type_designator_hash
            self.named_types[name] = type_designator
            new_expression.named_type = self.typedef_mark
            new_expression.named_type_hash = typedef_mark_hash
            new_expression.composite_type = [typedef_mark_hash, type_designator_hash, self.parent_type[type_designator_hash]]
            new_expression.composite_type_hash = self.hasher.composite_hash(new_expression.composite_type)
            new_expression.elements = [named_type_hash, type_designator_hash]
            new_expression.hash_code = self.hasher.expression_hash(new_expression.named_type_hash, new_expression.elements)
            self.symbol_hash[name] = new_expression.hash_code
        else:
            self.pending_named_types.append(name)
        return new_expression

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
