from typing import List, Tuple
from abc import ABC, abstractmethod
from das.expression import Expression
from das.database.db_interface import DBInterface
from das.parser_threads import SharedData

class MettaParserActions(ABC):

    @abstractmethod
    def next_input_chunk(self) -> Tuple[str, str]:
        pass

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

    def update_line_number(self, current_line_number: int):
        self.current_line_number = current_line_number

class MultiFileKnowledgeBase(MettaParserActions):

    def __init__(self, db: DBInterface, file_list: List[str], shared_data: SharedData):
        super().__init__()
        self.db = db
        self.file_list = [f for f in file_list]
        self.shared_data = shared_data
        self.finished = False

    def next_input_chunk(self) -> Tuple[str, str]:
        file_path = self.file_list.pop(0) if self.file_list else None
        if file_path is None:
            self.finished = True
            return (None, None)
        with open(file_path, "r") as file_handle:
            text = file_handle.read()
        if text is None or text == "":
            return (None, None)
        else:
            return (text, file_path)

    def new_top_level_expression(self, expression: Expression):
        self.shared_data.add_regular_expression(expression)

    def new_expression(self, expression: Expression):
        self.shared_data.add_regular_expression(expression)

    def new_terminal(self, expression: Expression):
        self.shared_data.add_terminal(expression)

    def new_top_level_typedef_expression(self, expression: Expression):
        self.shared_data.add_typedef_expression(expression)
