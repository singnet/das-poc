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
        self.current_line_number = 1

class KnowledgeBaseFile(ParserActions):

    def __init__(self, db: DBInterface, file_path: str, shared_data: SharedData):
        super().__init__()
        self.db = db
        self.file_path = file_path
        with open(file_path, "r") as file_handle:
            self.input_string = file_handle.read()
        self.shared_data = shared_data

    def new_top_level_expression(self, expression: Expression):
        self.shared_data.add_regular_expression(expression)

    def new_expression(self, expression: Expression):
        self.shared_data.add_regular_expression(expression)

    def new_terminal(self, expression: Expression):
        self.shared_data.add_terminal(expression)

    def new_top_level_typedef_expression(self, expression: Expression):
        self.shared_data.add_typedef_expression(expression)
