from typing import List, Tuple
from abc import ABC, abstractmethod

class MettaParserActions(ABC):

    @abstractmethod
    def next_input_chunk(self) -> Tuple[str, str]:
        pass

    @abstractmethod
    def new_top_level_expression(self, expression: str):
        pass

    @abstractmethod
    def new_top_level_typedef_expression(self, expression: str):
        pass

class MultiFileKnowledgeBase(MettaParserActions):

    def __init__(self, file_list: List[str]):
        self.file_list = [f for f in file_list]

    def next_input_chunk(self) -> Tuple[str, str]:
        file_path = self.file_list.pop(0) if self.file_list else None
        if file_path is None:
            return (None, None)
        with open(file_path, "r") as file_handle:
            text = file_handle.read()
        return (text, file_path)

    def new_top_level_expression(self, expression: str):
        print(f"EXPR: <{expression}>")

    def new_top_level_typedef_expression(self, expression: str):
        print(f"TYPE: <{expression}>")
