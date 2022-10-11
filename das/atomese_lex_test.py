import pytest
from das.atomese_lex import AtomeseLex

lex_test_data = """
(ContextLink
    (MemberLink
        (ChebiNode "ChEBI:10033")
        (ReactomeNode "R-HSA-6806664"))
    (EvaluationLink
        (PredicateNode "has_location")
        (ListLink
            (ChebiNode "ChEBI:10033")
            (ConceptNode "cytosol"))))
(EvaluationLink
    (PredicateNode "has_name")
    (ListLink
        (ChebiNode "ChEBI:10033")
        (ConceptNode "warfarin")))
(ContextLink
    (MemberLink
        (ChebiNode "ChEBI:10036")
        (ReactomeNode "R HSA 2142753"))
    (EvaluationLink
        (PredicateNode "has_location")
        (ListLink
            (ChebiNode "ChEBI:10036")
            (ConceptNode "endoplasmic reticulum lumen"))))"""


def test_lexer():
    wrap = AtomeseLex()
    #wrap.build()
    expected_tokens = [
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_OPENNING",
        "ATOM_TYPE",
        "NODE_NAME",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "ATOM_CLOSING",
        "EOF"
    ]

    wrap.lexer.input(lex_test_data)
    for expected_token in expected_tokens:
        token = wrap.lexer.token()
        assert token.type == expected_token
    assert not wrap.lexer.token()
