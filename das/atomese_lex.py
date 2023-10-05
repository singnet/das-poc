import ply.lex as lex
from function.das.exceptions import AtomeseLexerError
 
class AtomeseLex:
    def __init__(self, **kwargs):

        self.reserved = {
        }

        self.tokens = [
            'ATOM_OPENNING',
            'ATOM_CLOSING',
            'ATOM_TYPE',
            'NODE_NAME',
            'STV',
            'FLOAT',
            'COMMENT',
            'EOF',
        ] + list(self.reserved.values())

        self.t_ATOM_OPENNING = r'\('
        self.t_ATOM_CLOSING = r'\)'

        self.lexer = lex.lex(module=self, **kwargs)
        self.lexer.eof_reported_flag = False
        self.action_broker = None
        self.eof_handler = self.default_eof_handler
        self.lexer.filename = ""


    def t_NODE_NAME(self, t):
        r'\"[^\"]+\"'
        t.value = t.value[1:-1]
        return t

    def t_ATOM_TYPE(self, t):
        r'[^\W0-9]\w*'
        if t.value == 'STV' or t.value == 'stv':
            t.type = 'STV'
        else:
            if t.value.endswith("Node") or t.value.endswith("Link"):
                t.value = t.value[0:-4]
        return t

    t_FLOAT = r'\d+\.\d+'

    t_ignore =' \t'

    def t_COMMENT(self, t):
        r'\;.*'
        pass

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
        raise AtomeseLexerError(error_message)
