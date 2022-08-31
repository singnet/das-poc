import ply.lex as lex
 
class MettaLex:
    def __init__(self):

        self.reserved = {
            'Type' : 'BASIC_TYPE',
        }

        self.tokens = [
            'EXPRESSION_OPENNING',
            'EXPRESSION_CLOSING',
            'SET_OPENNING',
            'SET_CLOSING',
            'TYPE_DEFINITION_MARK',
            'ATOM_NAME',
            'EXPRESSION_NAME',
            'EOF',
        ] + list(self.reserved.values())

        self.t_TYPE_DEFINITION_MARK = r'\:'
        self.t_EXPRESSION_OPENNING = r'\('
        self.t_EXPRESSION_CLOSING = r'\)'
        self.t_SET_OPENNING = r'\{'
        self.t_SET_CLOSING = r'\}'

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
        if self.lexer.eof_flag:
            return None
        self.lexer.eof_flag = True
        self.lexer.input("")
        t.type = 'EOF'
        return t
     
    def t_error(self, t):
        print(f"Illegal character at line {t.lexer.lineno}: '{t.value[0]}'")
        n = 80 if len(t.value) > 30 else len(t.value) - 1
        print(f"Near: '{t.value[0:n]}...'")
        exit()
     
    def build(self,**kwargs):
        self.lexer = lex.lex(module=self, **kwargs)
        self.lexer.eof_flag = False
