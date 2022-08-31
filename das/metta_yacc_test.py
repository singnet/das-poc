import pytest
from das.my_metta_lex import MettaLex
from das.metta_lex_test import lex_test_data as test_data
from das.metta_yacc import MettaYacc

def test_parser():
    lex_wrap = MettaLex()
    lex_wrap.build()
    lex_wrap.lexer.input(test_data)
    yacc_wrap = MettaYacc()
    yacc_wrap.build(lex_wrap)
    result = yacc_wrap.check(test_data)
    assert result == "SUCCESS"
