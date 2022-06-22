import argparse

from ply.lex import lex

from das.helpers import get_logger

logger = get_logger()


class Lex(object):
  # List of token names.   This is always required
  tokens = (
    "INTEGER",
    "FLOAT",
    "LPAREN",
    "RPAREN",
    "STRING",
    "ID",
  )

  # Regular expression rules for simple tokens
  t_LPAREN = r"\("
  t_RPAREN = r"\)"

  def t_STRING(self, t):
    r'\"([^"]*?)\"'
    return t

  def t_ID(self, t):
    r"[a-zA-Z]+"
    return t

  def t_FLOAT(self, t):
    r"-?\d+\.\d*"
    t.value = float(t.value)
    return t

  def t_INTEGER(self, t):
    r"-?\d+"
    t.value = int(t.value)
    return t

  # Define a rule so we can track line numbers
  def t_newline(self, t):
    r"\n+"
    t.lexer.lineno += len(t.value)

  # A string containing ignored characters (spaces and tabs)
  t_ignore = " \t"

  def t_error(self, t):
    raise AttributeError("Illegal character '{}' at line {}".format(t.value[0], t.lexer.lineno))
    # if errors should be ignored, comment the raise exception line and uncomment the following
    # line:
    # t.lexer.skip(1)

  def build(self, **kwargs):
    self.lexer = lex(module=self, **kwargs)

  def get_tokens(self, data: str):
    self.lexer.input(data)
    while True:
      tok = self.lexer.token()
      if not tok:
        break
      yield tok.lexpos, tok.type, tok.value


def main():
  parser = argparse.ArgumentParser("Lexical analysis")
  parser.add_argument("filename", type=str, help="Input filename")

  args = parser.parse_args()

  m = Lex()
  m.build()
  with open(args.filename, "r") as fh:
    for (pos, token_type, value) in m.get_tokens(fh.read()):
      logger.info(pos, token_type, value)


if __name__ == "__main__":
  a = 1
  main()
