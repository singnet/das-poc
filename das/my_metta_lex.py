import ply.lex as lex
from das.exceptions import MettaLexerError
 
class MettaLex:
    def __init__(self, **kwargs):

        self.reserved = {
            'Type' : 'BASIC_TYPE',
        }

        self.tokens = [
            'EXPRESSION_OPENNING',
            'EXPRESSION_CLOSING',
            'TYPE_DEFINITION_MARK',
            'ATOM_NAME',
            # AQUI TODO: change atom -> terminal
            'EXPRESSION_NAME',
            'EOF',
        ] + list(self.reserved.values())

        self.t_TYPE_DEFINITION_MARK = r'\:'
        self.t_EXPRESSION_OPENNING = r'\('
        self.t_EXPRESSION_CLOSING = r'\)'

        self.lexer = lex.lex(module=self, **kwargs)
        self.lexer.eof_reported_flag = False
        self.action_broker = None
        self.eof_handler = self.default_eof_handler
        self.lexer.filename = ""

    def t_ATOM_NAME(self, t):
        r'\"[^\"]+\"'
        t.value = t.value[1:-1]
        return t

    def t_EXPRESSION_NAME(self, t):
        r'[^\W0-9]\w*'
        t.type = self.reserved.get(t.value,'EXPRESSION_NAME')
        return t

    t_ignore =' \t'

    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)

    def t_eof(self, t):
        return self.eof_handler(t)

    def default_eof_handler(self, t):
        if self.lexer.eof_reported_flag:
            return None
        else:
            self.lexer.input("")
            t.type = 'EOF'
            self.lexer.eof_reported_flag = True
            return t
     
    def t_error(self, t):
        source = f"File: {self.lexer.filename if self.lexer.filename else '<input string>'}"
        n = 80 if len(t.value) > 30 else len(t.value) - 1
        error_message = f"{source} - Illegal character at line {t.lexer.lineno}: '{t.value[0]}' " +\
                        f"Near: '{t.value[0:n]}...'"
        raise MettaLexerError(error_message)
