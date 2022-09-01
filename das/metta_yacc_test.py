import pytest
from das.my_metta_lex import MettaLex
from das.metta_lex_test import lex_test_data as test_data
from das.metta_yacc import MettaYacc
from das.metta_parser_actions import MettaParserActions

def test_parser():
    yacc_wrap = MettaYacc()
    result = yacc_wrap.check(test_data)
    assert result == "SUCCESS"

def test_action_broker():

    class ActionBroker(MettaParserActions):
        def __init__(self, data=None):
            self.count_expression = 0
            self.count_type = 0
            self.data = data

        def next_input_chunk(self):
            answer = self.data
            self.data = None
            return (answer, "")

        def new_top_level_expression(self, expression: str):
            self.count_expression += 1

        def new_top_level_typedef_expression(self, expression: str):
            self.count_type += 1

    action_broker = ActionBroker(test_data)
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.check(test_data)
    assert result == "SUCCESS"
    assert action_broker.count_expression == 0
    assert action_broker.count_type == 0

    action_broker = ActionBroker()
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.parse(test_data)
    assert result == "SUCCESS"
    assert action_broker.count_expression == 1
    assert action_broker.count_type == 7

    action_broker = ActionBroker(test_data)
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.parse_action_broker_input()
    assert result == "SUCCESS"
    assert action_broker.count_expression == 1
    assert action_broker.count_type == 7
