from db_interface import DBInterface
from stub_db import StubDB
from pattern_matcher import PatternMatchingAnswer, LogicalExpression, Node, Link


def match(db_api: DBInterface, expression: LogicalExpression):
    print(f"Matching {expression}")
    answer: PatternMatchingAnswer = PatternMatchingAnswer()
    print(expression.match(db_api, answer))
    print(answer)
    print('--------------------------------------------------------------------------------')

print('---------------------------- Integration tests ---------------------------------')

db: DBInterface = StubDB()

n1 = Node('Concept', 'human')
n2 = Node('Concept', 'mammal')
match(db, n1)
match(db, Link('Inheritance', [n1, n2]))
match(db, Link('Similarity', [n1, n2]))

match(db, Link('Similarity', [Node('Concept', 'snake'), Node('Concept', 'earthworm')]))
match(db, Link('Similarity', [Node('Concept', 'earthworm'), Node('Concept', 'snake')]))

n1 = Node('Concept', 'dinosaur')
n2 = Node('Concept', 'reptile')
n3 = Node('Concept', 'triceratops')
l1 = Link('Inheritance', [n1, n2])
l2 = Link('Inheritance', [n3, n1])
l3 = Link('List', [l1, l2])
l4 = Link('List', [l2, l1])
s3 = Link('Set', [l1, l2])
s4 = Link('Set', [l2, l1])
match(db, l3)
match(db, l4)
match(db, s3)
match(db, s4)

#mammal = Node('Concept', 'mammal')
#human = Node('Concept', 'human')
#chimp = Node('Concept', 'chimp')
#monkey = Node('Concept', 'monkey')
#inh1 = Link('Inheritance', [human, mammal])
#inh2 = Link('Inheritance', [monkey, mammal])
#inh3 = Link('Inheritance', [chimp, mammal])
#sim1 = Link('Similarity', [human, monkey])
#sim2 = Link('Similarity', [chimp, monkey])
#match(db, inh1)
#match(db, inh2)
#match(db, inh3)
#match(db, sim1)
#match(db, sim2)
#p1 = Link('Inheritance', [Variable('v1'), mammal])



