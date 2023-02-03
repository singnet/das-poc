from das.logger import logger
from das.expression_hasher import ExpressionHasher
from enum import Enum, auto

class State(str, Enum):
    READING_TYPES = auto()
    READING_TERMINALS = auto()
    READING_EXPRESSIONS = auto()

class CanonicalParser:

    def __init__(self):
        self.current_line_count = 1
        self.bulk_typedef_insertion = []
        self.bulk_terminal_insertion = []

    def _add_typedef(self, name, stype):
        print(f"typedef {name} {stype}")
            
    def _add_terminal(self, name, stype):
        print(f"terminal <{name}> {stype}")
            
    def _check(self, flag):
        if not flag:
            print(f"({self.current_state.name}) Line #{self.current_line_count}: {self.current_line}")
            assert False
        
    def parse(self, path):
        self.current_state = State.READING_TYPES
        with open(path, "r") as file:
            for line in file:
                self.current_line = line.strip()
                self.current_line_count += 1
                expression = self.current_line.split()
                if self.current_state == State.READING_TYPES:
                    self._check(expression[0] == "(:")
                    if expression[1].startswith("\""):
                        self.current_state = State.READING_TERMINALS
                    else:
                        self._check(len(expression) == 3)
                        type_name = expression[1]
                        stype = expression[-1].rstrip(")")
                        self._add_typedef(type_name, stype)
                if self.current_state == State.READING_TERMINALS:
                    if expression[0] == "(:":
                        terminal_name = " ".join(expression[1:-1]).strip("\"")
                        stype = expression[-1].rstrip(")")
                        self._add_terminal(terminal_name, stype)
                    else:
                        self.current_state = State.READING_EXPRESSIONS
                if self.current_state == State.READING_EXPRESSIONS:
                    self._check(expression[0] != "(:")
