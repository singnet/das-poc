class MettaLexerError(Exception):
    def __init__(self, error_message: str):
        super().__init__(error_message)

class MettaSyntaxError(Exception):
    def __init__(self, error_message: str):
        super().__init__(error_message)
