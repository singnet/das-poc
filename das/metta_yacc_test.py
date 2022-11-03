import pytest
from das.metta_lex import MettaLex
from das.metta_lex_test import lex_test_data as test_data
from das.metta_yacc import MettaYacc, Expression
from das.parser_actions import ParserActions
from das.exceptions import UndefinedSymbolError
from das.expression_hasher import ExpressionHasher
from das.metta_lex import BASIC_TYPE

class ActionBroker(ParserActions):
    def __init__(self, data=None):
        self.count_toplevel_expression = 0
        self.count_nested_expression = 0
        self.count_terminal = 0
        self.count_type = 0
        self.file_path = ""
        self.input_string = data

    #def next_input_chunk(self):
    #    answer = self.data
    #    self.data = None
    #    return (answer, "")

    def new_expression(self, expression: str):
        self.count_nested_expression += 1

    def new_terminal(self, expression: str):
        self.count_terminal += 1

    def new_top_level_expression(self, expression: str):
        self.count_toplevel_expression += 1

    def new_top_level_typedef_expression(self, expression: str):
        self.count_type += 1

def test_parser():
    yacc_wrap = MettaYacc()
    result = yacc_wrap.check(test_data)
    assert result == "SUCCESS"

def test_action_broker():

    action_broker = ActionBroker(test_data)
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.check(test_data)
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 0
    assert action_broker.count_type == 1

    action_broker = ActionBroker()
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.parse(test_data)
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 1
    assert action_broker.count_type == 9

    action_broker = ActionBroker(test_data)
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.parse_action_broker_input()
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 1
    assert action_broker.count_type == 9

def test_terminal_hash():

    yacc_wrap = MettaYacc()
    named_type1 = 'blah1'
    terminal_name1 = 'bleh1'
    named_type2 = 'blah2'
    terminal_name2 = 'bleh2'

    assert(len(yacc_wrap.terminal_hash) == 0)

    h = yacc_wrap._get_terminal_hash(named_type1, terminal_name1)
    assert(len(yacc_wrap.terminal_hash) == 1)
    assert h == yacc_wrap._get_terminal_hash(named_type1, terminal_name1)
    assert(len(yacc_wrap.terminal_hash) == 1)

    h = yacc_wrap._get_terminal_hash(named_type2, terminal_name2)
    assert(len(yacc_wrap.terminal_hash) == 2)
    assert h == yacc_wrap._get_terminal_hash(named_type2, terminal_name2)
    assert yacc_wrap._get_terminal_hash(named_type1, terminal_name1) \
        != yacc_wrap._get_terminal_hash(named_type2, terminal_name2)
    assert(len(yacc_wrap.terminal_hash) == 2)

    h = yacc_wrap._get_terminal_hash(named_type1, terminal_name2)
    assert(len(yacc_wrap.terminal_hash) == 3)
    assert h == yacc_wrap._get_terminal_hash(named_type1, terminal_name2)
    assert yacc_wrap._get_terminal_hash(named_type1, terminal_name1) \
        != yacc_wrap._get_terminal_hash(named_type1, terminal_name2)
    assert yacc_wrap._get_terminal_hash(named_type2, terminal_name2) \
        != yacc_wrap._get_terminal_hash(named_type1, terminal_name2)
    assert(len(yacc_wrap.terminal_hash) == 3)

    h = yacc_wrap._get_terminal_hash(named_type2, terminal_name1)
    assert(len(yacc_wrap.terminal_hash) == 4)
    assert h == yacc_wrap._get_terminal_hash(named_type2, terminal_name1)
    assert yacc_wrap._get_terminal_hash(named_type1, terminal_name1) \
        != yacc_wrap._get_terminal_hash(named_type2, terminal_name1)
    assert yacc_wrap._get_terminal_hash(named_type2, terminal_name2) \
        != yacc_wrap._get_terminal_hash(named_type2, terminal_name1)
    assert yacc_wrap._get_terminal_hash(named_type1, terminal_name2) \
        != yacc_wrap._get_terminal_hash(named_type2, terminal_name1)
    assert(len(yacc_wrap.terminal_hash) == 4)
    
def test_named_type_hash():

    yacc_wrap = MettaYacc()
    named_type1 = 'blah1'
    named_type2 = 'blah2'

    assert(len(yacc_wrap.named_type_hash) == 1)

    h = yacc_wrap._get_named_type_hash(named_type1)
    assert(len(yacc_wrap.named_type_hash) == 2)
    assert h == yacc_wrap._get_named_type_hash(named_type1)
    assert(len(yacc_wrap.named_type_hash) == 2)

    h = yacc_wrap._get_named_type_hash(named_type2)
    assert(len(yacc_wrap.named_type_hash) == 3)
    assert h == yacc_wrap._get_named_type_hash(named_type2)
    assert yacc_wrap._get_named_type_hash(named_type1) \
        != yacc_wrap._get_named_type_hash(named_type2)
    assert(len(yacc_wrap.named_type_hash) == 3)

def test_nested_expression():

    yacc_wrap = MettaYacc()

    expression1 = Expression(
        toplevel=False,
        ordered=True,
        terminal_name=None,
        typedef_name=None,
        named_type='Similarity',
        named_type_hash='Similarity Hash',
        composite_type=['Typedef Similarity Type'],
        composite_type_hash='Typedef Similarity Type Hash',
        elements=None,
        hash_code='h1')

    expression2 = Expression(
        toplevel=False,
        ordered=True,
        terminal_name='c1',
        typedef_name=None,
        named_type='Concept',
        named_type_hash='Concept Hash',
        composite_type=['Concept'],
        composite_type_hash='Concept Hash',
        elements=None,
        hash_code='h2')

    expression3 = Expression(
        toplevel=False,
        ordered=True,
        terminal_name='c2',
        typedef_name=None,
        named_type='Concept',
        named_type_hash='Concept Hash',
        composite_type=['Concept'],
        composite_type_hash='Concept Hash',
        elements=None,
        hash_code='h3')

    composite1 = yacc_wrap._nested_expression([expression1, expression2, expression3])

    assert not composite1.toplevel
    assert composite1.ordered
    assert composite1.terminal_name is None
    assert composite1.named_type == 'Similarity'
    assert composite1.named_type_hash == 'Similarity Hash'
    assert composite1.composite_type == ['Typedef Similarity Type', 'Concept', 'Concept']
    assert composite1.composite_type_hash is not None
    assert composite1.elements == ['h2', 'h3']
    assert composite1.hash_code is not None

    composite2 = yacc_wrap._nested_expression([expression1, expression3, expression2])
    assert composite2.composite_type_hash == composite1.composite_type_hash
    assert composite2.hash_code != composite1.hash_code

    composite3 = yacc_wrap._nested_expression([expression1, composite1, composite2])
    assert not composite3.toplevel
    assert composite3.ordered
    assert composite3.terminal_name is None
    assert composite3.named_type == 'Similarity'
    assert composite3.named_type_hash == 'Similarity Hash'
    assert composite3.composite_type == [
        'Typedef Similarity Type',
        ['Typedef Similarity Type', 'Concept', 'Concept'],
        ['Typedef Similarity Type', 'Concept', 'Concept']]
    assert composite3.composite_type_hash is not None
    assert composite3.composite_type_hash != composite1.composite_type_hash
    assert composite3.elements == [composite1.hash_code, composite2.hash_code]
    assert composite3.hash_code is not None
    assert composite3.hash_code != composite1.hash_code
    assert composite3.hash_code != composite2.hash_code

def test_typedef():
    yacc_wrap = MettaYacc()
    assert len(yacc_wrap.pending_named_types) == 0

    name = 'Concept'
    type_designator = 'Type'
    expression1 = yacc_wrap._typedef(name, type_designator)
    typedef_mark = yacc_wrap.typedef_mark
    typedef_mark_hash = ExpressionHasher._compute_hash(typedef_mark)
    basic_type_hash = ExpressionHasher._compute_hash(BASIC_TYPE)
    name_hash = ExpressionHasher._compute_hash(name)
    assert len(yacc_wrap.pending_named_types) == 0
    assert not expression1.toplevel
    assert expression1.ordered
    assert expression1.terminal_name is None
    assert expression1.named_type == typedef_mark
    assert expression1.named_type_hash == typedef_mark_hash
    assert expression1.composite_type == \
        [typedef_mark_hash, basic_type_hash, basic_type_hash]
    assert expression1.composite_type_hash == \
        ExpressionHasher.expression_hash(
            typedef_mark_hash, [basic_type_hash, basic_type_hash])
    assert expression1.elements == [name_hash, basic_type_hash]
    assert expression1.hash_code == \
        ExpressionHasher.expression_hash(
            typedef_mark_hash, [name_hash, basic_type_hash])

    assert len(yacc_wrap.named_type_hash) == 3
    h1 = yacc_wrap._get_named_type_hash(type_designator)
    yacc_wrap.parent_type[h1] = h1
    assert len(yacc_wrap.named_type_hash) == 3
    assert len(yacc_wrap.parent_type) == 2
    
    name = 'Concept'
    type_designator = 'Type'
    expression2 = yacc_wrap._typedef(name, type_designator)
    h2 = yacc_wrap.named_type_hash[name]
    h3 = yacc_wrap.named_type_hash[':']
    assert len(yacc_wrap.named_type_hash) == 3
    assert len(yacc_wrap.parent_type) == 2
    assert yacc_wrap.parent_type[h2] == h1
    assert not expression2.toplevel
    assert expression2.ordered
    assert expression2.terminal_name is None
    assert expression2.named_type == ':'
    assert expression2.named_type_hash is not None
    assert expression2.composite_type == [h3, h1, h1]
    assert expression2.composite_type_hash is not None
    assert expression2.elements == [h2, h1]
    assert expression2.hash_code is not None

    name = 'Similarity'
    type_designator = 'Type'
    expression3 = yacc_wrap._typedef(name, type_designator)
    h4 = yacc_wrap.named_type_hash[name]
    assert len(yacc_wrap.named_type_hash) == 4
    assert len(yacc_wrap.parent_type) == 3
    assert yacc_wrap.parent_type[h4] == h1
    assert not expression3.toplevel
    assert expression3.ordered
    assert expression3.terminal_name is None
    assert expression3.named_type == ':'
    assert expression3.named_type_hash is not None
    assert expression3.composite_type == [h3, h1, h1]
    assert expression3.composite_type_hash is not None
    assert expression3.composite_type_hash == expression2.composite_type_hash
    assert expression3.elements == [h4, h1]
    assert expression3.hash_code is not None
    assert expression3.hash_code != expression2.hash_code
    
    name = 'Concept'
    type_designator = 'Type'
    expression4 = yacc_wrap._typedef(name, type_designator)
    assert h2 == yacc_wrap.named_type_hash[name]
    assert len(yacc_wrap.named_type_hash) == 4
    assert len(yacc_wrap.parent_type) == 3
    assert yacc_wrap.parent_type[h2] == h1
    assert expression4 == expression2

    name = 'Similarity2'
    type_designator = 'Similarity'
    expression5 = yacc_wrap._typedef(name, type_designator)
    h5 = yacc_wrap.named_type_hash[name]
    assert len(yacc_wrap.named_type_hash) == 5
    assert len(yacc_wrap.parent_type) == 4
    assert yacc_wrap.parent_type[h5] == h4
    assert not expression5.toplevel
    assert expression5.ordered
    assert expression5.terminal_name is None
    assert expression5.named_type == ':'
    assert expression5.named_type_hash is not None
    assert expression5.composite_type == [h3, h4, h1]
    assert expression5.composite_type_hash is not None
    assert expression5.composite_type_hash != expression2.composite_type_hash
    assert expression5.elements == [h5, h4]
    assert expression5.hash_code is not None
    assert expression5.hash_code != expression2.hash_code
    assert expression5.hash_code != expression3.hash_code

def test_pending_types():

    missing_type = """
        (: Evaluation Type)
        (: Predicate Type)
        (: Reactome Type)
        (: Concept Type)
        (: "Predicate:has_name" Predicate)
        (: "Reactome:R-HSA-164843" Reactome)
        (: "Concept:2-LTR circle formation" Concept)
        (
            Evaluation
            "Predicate:has_name"
            (
                Evaluation
                "Predicate:has_name"
                (
                    Set
                    "Reactome:R-HSA-164843"
                    "Concept:2-LTR circle formation"
                )
            )
        )
    """

    delayed_type1 = """
        (: Evaluation Type)
        (: Predicate Type)
        (: Reactome Type)
        (: Concept Type)
        (: "Predicate:has_name" Predicate)
        (: "Reactome:R-HSA-164843" Reactome)
        (: "Concept:2-LTR circle formation" Concept)
        (
            Evaluation
            "Predicate:has_name"
            (
                Evaluation
                "Predicate:has_name"
                (
                    Set
                    "Reactome:R-HSA-164843"
                    "Concept:2-LTR circle formation"
                )
            )
        )
        (: Set Type)
    """

    delayed_type2 = """
        (: Evaluation Type)
        (: Predicate Type)
        (: Reactome Type)
        (: Concept Type)
        (: Set2 Set)
        (: "Predicate:has_name" Predicate)
        (: "Reactome:R-HSA-164843" Reactome)
        (: "Concept:2-LTR circle formation" Concept)
        (
            Evaluation
            "Predicate:has_name"
            (
                Evaluation
                "Predicate:has_name"
                (
                    Set2
                    "Reactome:R-HSA-164843"
                    "Concept:2-LTR circle formation"
                )
            )
        )
        (: Set Type)
    """

    action_broker = ActionBroker()
    yacc_wrap = MettaYacc(action_broker=action_broker)
    with pytest.raises(UndefinedSymbolError) as exception:
        result = yacc_wrap.parse(missing_type)
        assert len(exception.missing_symbols) == 1
        'Set' in exception.missing_symbols

    action_broker = ActionBroker()
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.parse(delayed_type1)
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 1
    assert action_broker.count_type == 9

    action_broker = ActionBroker()
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.parse(delayed_type2)
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 1
    assert action_broker.count_type == 10

def test_pending_terminal_names():

    missing_type = """
        (: Evaluation Type)
        (: Reactome Type)
        (: Concept Type)
        (: Set Type)
        (: "Predicate:has_name" Predicate)
        (: "Reactome:R-HSA-164843" Reactome)
        (: "Concept:2-LTR circle formation" Concept)
        (
            Evaluation
            "Predicate:has_name"
            (
                Evaluation
                "Predicate:has_name"
                (
                    Set
                    "Reactome:R-HSA-164843"
                    "Concept:2-LTR circle formation"
                )
            )
        )
    """

    delayed_type1 = """
        (: Evaluation Type)
        (: Reactome Type)
        (: Concept Type)
        (: Set Type)
        (: "Predicate:has_name" Predicate)
        (: "Reactome:R-HSA-164843" Reactome)
        (: "Concept:2-LTR circle formation" Concept)
        (: Predicate Type)
        (
            Evaluation
            "Predicate:has_name"
            (
                Evaluation
                "Predicate:has_name"
                (
                    Set
                    "Reactome:R-HSA-164843"
                    "Concept:2-LTR circle formation"
                )
            )
        )
    """

    delayed_type2 = """
        (: Evaluation Type)
        (: Reactome Type)
        (: Concept Type)
        (: Set Type)
        (: "Predicate:has_name" Predicate)
        (: "Reactome:R-HSA-164843" Reactome)
        (: "Concept:2-LTR circle formation" Concept)
        (: Predicate Predicate2)
        (
            Evaluation
            "Predicate:has_name"
            (
                Evaluation
                "Predicate:has_name"
                (
                    Set
                    "Reactome:R-HSA-164843"
                    "Concept:2-LTR circle formation"
                )
            )
        )
        (: Predicate2 Type)
    """

    action_broker = ActionBroker()
    yacc_wrap = MettaYacc(action_broker=action_broker)
    with pytest.raises(UndefinedSymbolError) as exception:
        result = yacc_wrap.parse(missing_type)
        assert len(exception.missing_symbols) == 1
        'Set' in exception.missing_symbols

    action_broker = ActionBroker()
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.parse(delayed_type1)
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 1
    assert action_broker.count_type == 9

    action_broker = ActionBroker()
    yacc_wrap = MettaYacc(action_broker=action_broker)
    result = yacc_wrap.parse(delayed_type2)
    assert result == "SUCCESS"
    assert action_broker.count_toplevel_expression == 1
    assert action_broker.count_type == 10
