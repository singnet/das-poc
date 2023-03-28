from typing import List, Tuple
from abc import ABC, abstractmethod
from das.expression import Expression
from das.database.db_interface import DBInterface
from das.parser_threads import SharedData

class ParserActions(ABC):

    @abstractmethod
    def new_top_level_expression(self, expression: Expression):
        pass

    @abstractmethod
    def new_expression(self, expression: Expression):
        pass

    @abstractmethod
    def new_terminal(self, expression: Expression):
        pass

    @abstractmethod
    def new_top_level_typedef_expression(self, expression: Expression):
        pass

    def __init__(self):
        self.named_type_hash = {}
        self.named_types = {}
        self.symbol_hash = {}
        self.terminal_hash = {}
        self.parent_type = {}
        self.current_line_number = 1

class MultiThreadParsing(ParserActions):

    def __init__(self, db: DBInterface, input_string: str,
        shared_data: SharedData, use_action_broker_cache=False):

        super().__init__()
        self.db = db
        self.file_path = ""
        self.input_string = input_string
        self.shared_data = shared_data
        if use_action_broker_cache:
            self.named_type_hash = db.named_type_hash
            self.named_types = db.named_types
            self.symbol_hash = db.symbol_hash
            self.terminal_hash = db.terminal_hash
            self.parent_type = db.parent_type

    def new_top_level_expression(self, expression: Expression):
        self.shared_data.add_regular_expression(expression)

    def new_expression(self, expression: Expression):
        self.shared_data.add_regular_expression(expression)

    def new_terminal(self, expression: Expression):
        self.shared_data.add_terminal(expression)

    def new_top_level_typedef_expression(self, expression: Expression):
        self.shared_data.add_typedef_expression(expression)

class KnowledgeBaseFile(MultiThreadParsing):

    def __init__(self, db: DBInterface, file_path: str, shared_data: SharedData):
        with open(file_path, "r") as file_handle:
            input_string = file_handle.read()
        super().__init__(db, input_string, shared_data)
        self.file_path = file_path
