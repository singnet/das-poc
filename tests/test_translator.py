from das.atomese2metta.translator import Expression, MList, MSet


def test_given_a_expression_instance_then_should_return_a_string_with_parentheses():
  assert str(Expression()) == '()'


def test_given_a_mlist_instance_then_should_return_a_string_with_brackets():
  assert str(MList()) == '()'


def test_given_a_mset_instance_then_should_return_a_string_with_curly_brackets():
  assert str(MSet()) == '{}'
