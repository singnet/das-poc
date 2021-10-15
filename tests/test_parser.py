from scripts.atomese2metta.parser import Parser, MultiprocessingParser


def test_when_split_string_to_two_chunks():
    text = (
        '(EvaluationLink\n'
        '    (PredicateNode "P1")\n'
        '    (ListLink\n'
        '        (CellNode "CL1")\n'
        '        (ConceptNode "CC1")))\n'
        '(EvaluationLink\n'
        '    (PredicateNode "P2")\n'
        '    (ListLink\n'
        '        (CellNode "CL2")\n'
        '        (ConceptNode "CC2")))\n'
    )
    assert list(MultiprocessingParser(1)._split_expressions(text)) == [
        '(EvaluationLink    (PredicateNode "P1")    (ListLink        (CellNode "CL1")        (ConceptNode "CC1")))',
        '(EvaluationLink    (PredicateNode "P2")    (ListLink        (CellNode "CL2")        (ConceptNode "CC2")))'
    ]

def test_when_parse_input_single_expression():
    text= (
        '(PredicateNode "P1")\n'
    )
    assert Parser().parse(text) == [['PredicateNode', '"P1"']]

def test_when_parse_input_two_expressions():
    text= (
        '(PredicateNode "P1")\n'
        '(PredicateNode "P2")\n'
    )
    assert Parser().parse(text) == [
        ['PredicateNode', '"P1"'],
        ['PredicateNode', '"P2"'],
    ]

def test_when_parse_input_single_expression_using_multiprocessing():
    text= (
        '(PredicateNode "P1")\n'
    )
    assert MultiprocessingParser(1).parse(text) == [['PredicateNode', '"P1"']]

def test_when_parse_input_two_expressions_using_multiprocessing():
    text= (
        '(PredicateNode "P1")\n'
        '(PredicateNode "P2")\n'
    )
    assert MultiprocessingParser().parse(text) == [
        ['PredicateNode', '"P1"'],
        ['PredicateNode', '"P2"'],
    ]

def test_when_given_same_input_normal_parser_and_multiprocessing_parser_should_return_same_result():
    text= (
        '(PredicateNode "P1")\n'
        '(PredicateNode "P2")\n'
    )
    assert Parser().parse(text) == MultiprocessingParser().parse(text)
