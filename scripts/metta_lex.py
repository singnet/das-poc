from ply.lex import lex


class MettaLex(object):
    # List of token names.   This is always required
    tokens = (
        "COLON",
        "LPAREN",
        "RPAREN",
        "LCBRACKET",
        "RCBRACKET",
        "STRING",
        "ID",
    )

    # Regular expression rules for simple tokens
    t_COLON = r"\:"
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_LCBRACKET = r"\{"
    t_RCBRACKET = r"\}"

    def t_STRING(self, t):
        r'\"([^"]*?)\"'
        return t

    def t_ID(self, t):
        r"[a-zA-Z]+"
        return t

    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r"\n+"
        t.lexer.lineno += len(t.value)

    # A string containing ignored characters (spaces and tabs)
    t_ignore = " \t"

    def t_error(self, t):
        raise AttributeError("Illegal character '{}' at line {}".format(t.value[0], t.lexer.lineno))
        # t.lexer.skip(1)

    def build(self, **kwargs):
        self.lexer = lex(module=self, **kwargs)

    def get_tokens(self, data: str):
        self.lexer.input(data)
        while True:
            tok = self.lexer.token()
            if not tok:
                break
            yield tok.lexpos, tok.type, tok.value
