class Transaction:

    def __init__(self):
        self.toplevel_expressions = []

    def add_toplevel_expression(self, expression: str):
        self.toplevel_expressions.append(expression)

    def metta_string(self):
        return "\n".join(self.toplevel_expressions)
