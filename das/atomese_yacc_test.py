import pytest
from das.atomese_lex import AtomeseLex
from das.atomese_lex_test import lex_test_data as test_data
from das.atomese_yacc import AtomeseYacc
from das.parser_actions import ParserActions
from das.exceptions import UndefinedSymbolError

class ActionBroker(ParserActions):
    def __init__(self, data=None):
        self.count_toplevel_expression = 0
        self.count_nested_expression = 0
        self.count_terminal = 0
        self.count_type = 0
        self.file_path = ""
        self.input_string = data

    def new_expression(self, expression: str):
        self.count_nested_expression += 1

    def new_terminal(self, expression: str):
        self.count_terminal += 1

    def new_top_level_expression(self, expression: str):
        self.count_toplevel_expression += 1

    def new_top_level_typedef_expression(self, expression: str):
        self.count_type += 1

def test_parser():
    yacc_wrap = AtomeseYacc()
    result = yacc_wrap.check(test_data)
    assert result == "SUCCESS"

def _action_broker():

    action_broker = ActionBroker(test_data)
    yacc_wrap = AtomeseYacc(action_broker=action_broker)
    result = yacc_wrap.check(test_data)
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 0
    assert action_broker.count_type == 0

    action_broker = ActionBroker()
    yacc_wrap = AtomeseYacc(action_broker=action_broker)
    result = yacc_wrap.parse(test_data)
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 1
    assert action_broker.count_type == 8

    action_broker = ActionBroker(test_data)
    yacc_wrap = AtomeseYacc(action_broker=action_broker)
    result = yacc_wrap.parse_action_broker_input()
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 1
    assert action_broker.count_type == 8
