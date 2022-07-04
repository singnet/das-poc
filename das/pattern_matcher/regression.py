from das.pattern_matcher.db_interface import DBInterface
from das.pattern_matcher.pattern_matcher import (And, Link, LogicalExpression,
                                                 Node, Not, Or, LinkTemplate,
                                                 PatternMatchingAnswer,
                                                 Variable, TypedVariable)
from das.pattern_matcher.stub_db import StubDB
from das.pattern_matcher.couch_mongo_db import CouchMongoDB
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from das.helpers import get_mongodb

def match(db_api: DBInterface, expression: LogicalExpression):
    print(f"Matching {expression}")
    answer: PatternMatchingAnswer = PatternMatchingAnswer()
    print(expression.matched(db_api, answer))
    print(answer)
    print(
        "--------------------------------------------------------------------------------"
    )


if __name__ == "__main__":
    print(
        "---------------------------- Integration tests ---------------------------------"
    )

    mongodb_specs = {
        "hostname": "mongo",
        "port":  27017,
        "username": "dbadmin",
        "password": "das#secret",
        "database": "TOY",
    }
    couchbase_specs = {
        "hostname": "couchbase",
        "username": "dbadmin",
        "password": "das#secret",
    }
    cluster = Cluster(
        f'couchbase://{couchbase_specs["hostname"]}',
        authenticator=PasswordAuthenticator(
            couchbase_specs["username"], couchbase_specs["password"]
        ),
    )

    db: DBInterface = StubDB()
    #db: DBInterface = CouchMongoDB(cluster.bucket("das"), get_mongodb(mongodb_specs))

    n1 = Node("Concept", "human")
    n2 = Node("Concept", "mammal")
    match(db, n1)
    match(db, Link("Inheritance", [n1, n2], True))
    match(db, Link("Similarity", [n1, n2], False))

    match(
        db,
        Link("Similarity", [Node("Concept", "snake"), Node("Concept", "earthworm")], False),
    )
    match(
        db,
        Link("Similarity", [Node("Concept", "earthworm"), Node("Concept", "snake")], False),
    )

    n1 = Node("Concept", "dinosaur")
    n2 = Node("Concept", "reptile")
    n3 = Node("Concept", "triceratops")
    l1 = Link("Inheritance", [n1, n2], True)
    l2 = Link("Inheritance", [n3, n1], True)
    l3 = Link("List", [l1, l2], True)
    l4 = Link("List", [l2, l1], True)
    s3 = Link("Set", [l1, l2], False)
    s4 = Link("Set", [l2, l1], False)
    match(db, l3)
    match(db, l4)
    match(db, s3)
    match(db, s4)

    animal = Node("Concept", "animal")
    mammal = Node("Concept", "mammal")
    human = Node("Concept", "human")
    chimp = Node("Concept", "chimp")
    monkey = Node("Concept", "monkey")
    ent = Node("Concept", "ent")

    inh1 = Link("Inheritance", [human, mammal], True)
    inh2 = Link("Inheritance", [monkey, mammal], True)
    inh3 = Link("Inheritance", [chimp, mammal], True)
    sim1 = Link("Similarity", [human, monkey], False)
    sim2 = Link("Similarity", [chimp, monkey], False)
    match(db, inh1)
    match(db, inh2)
    match(db, inh3)
    match(db, sim1)
    match(db, sim2)
    match(db, Link("Inheritance", [Variable("V1"), mammal], True))
    match(db, Link("Inheritance", [Variable("V1"), Variable("V2")], True))
    match(db, Link("Inheritance", [Variable("V1"), Variable("V1")], True))
    match(db, Link("Inheritance", [Variable("V2"), Variable("V1")], True))
    match(db, Link("Inheritance", [mammal, Variable("V1")], True))
    match(db, Link("Inheritance", [animal, Variable("V1")], True))
    match(db, Link("Similarity", [Variable("V1"), Variable("V2")], False))
    match(db, Link("Similarity", [human, Variable("V1")], False))
    match(db, Link("Similarity", [Variable("V1"), human], False))
    match(db, Link("List", [human, ent, Variable("V1"), Variable("V2")], True))
    match(db, Link("List", [human, Variable("V1"), Variable("V2"), ent], True))
    match(db, Link("List", [ent, Variable("V1"), Variable("V2"), human], True))
    match(db, Link("Set", [human, ent, Variable("V1"), Variable("V2")], False))
    match(db, Link("Set", [human, Variable("V1"), Variable("V2"), ent], False))
    match(db, Link("Set", [ent, Variable("V1"), Variable("V2"), human], False))
    match(db, Link("Set", [monkey, Variable("V1"), Variable("V2"), chimp], False))

    inh1 = Link("Inheritance", [Variable("V1"), Variable("V2")], True)
    inh2 = Link("Inheritance", [Variable("V2"), Variable("V3")], True)
    match(db, inh1)
    match(db, inh2)
    match(db, Not(Link("Inheritance", [human, mammal], True)))
    match(db, Not(Link("Inheritance", [Variable("V1"), mammal], True)))
    match(db, Not(Link("Inheritance", [Variable("V1"), human], True)))
    match(db, And([inh1, inh2]))

    match(
        db,
        And(
            [
                Link("Inheritance", [Variable("V1"), Variable("V2")], True),
                Link("Similarity", [Variable("V1"), Variable("V2")], False),
            ]
        ),
    )
    match(
        db,
        And(
            [
                Link("Inheritance", [Variable("V1"), Variable("V3")], True),
                Link("Inheritance", [Variable("V2"), Variable("V3")], True),
                Link("Similarity", [Variable("V1"), Variable("V2")], False),
            ]
        ),
    )
    match(
        db,
        And(
            [
                Link("Inheritance", [Variable("V1"), Variable("V3")], True),
                Link("Inheritance", [Variable("V2"), Variable("V3")], True),
                Not(Link("Similarity", [Variable("V1"), Variable("V2")], False)),
            ]
        ),
    )

    match(
        db,
        And(
            [
                Link(
                    "Set",
                    [Variable("V1"), Variable("V2"), Variable("V3"), Variable("V4")],
                    False,
                ),
                Link("Similarity", [Variable("V1"), Variable("V2")], False),
            ]
        ),
    )

    match(
        db,
        And(
            [
                Link(
                    "Set",
                    [Variable("V1"), Variable("V2"), Variable("V3"), Variable("V4")],
                    False,
                ),
                Not(Link("Similarity", [Variable("V1"), Variable("V2")], False)),
            ]
        ),
    )

    match(
        db,
        And(
            [
                Link(
                    "Set",
                    [Variable("V1"), Variable("V2"), Variable("V3"), Variable("V4")],
                    False,
                ),
                Link("Inheritance", [Variable("V1"), Variable("V2")], True),
            ]
        ),
    )

    match(
        db,
        And(
            [
                Link(
                    "Set",
                    [Variable("V1"), Variable("V2"), Variable("V3"), Variable("V4")],
                    False,
                ),
                Not(Link("Inheritance", [Variable("V1"), Variable("V2")], True)),
            ]
        ),
    )

    match(
        db,
        And(
            [
                Link(
                    "Set",
                    [Variable("V1"), Variable("V2"), Variable("V3"), Variable("V4")],
                    False,
                ),
                Not(Link("Inheritance", [Variable("V1"), Variable("V2")], True)),
                Link("Similarity", [Variable("V1"), Variable("V2")], False),
            ]
        ),
    )

    match(
        db,
        Or(
            [
                Link(
                    "Set",
                    [Variable("V1"), Variable("V2"), Variable("V3"), Variable("V4")],
                    False,
                ),
                Link("Similarity", [Variable("V1"), Variable("V2")], False),
            ]
        ),
    )

    match(
        db,
        Or(
            [
                Not(Link("Inheritance", [Variable("V1"), Variable("V2")], True)),
                Link(
                    "Set",
                    [Variable("V1"), Variable("V2"), Variable("V3"), Variable("V4")],
                    False,
                ),
            ]
        ),
    )

    match(
        db,
        And(
            [
                Link(
                    "Set",
                    [Variable("V1"), Variable("V2"), Variable("V3"), Variable("V4")],
                    False,
                ),
                Not(
                    Or(
                        [
                            Link("Inheritance", [Variable("V1"), Variable("V2")], True),
                            Link("Similarity", [Variable("V1"), Variable("V2")], False),
                        ]
                    )
                ),
            ]
        ),
    )

    match(
        db,
        And(
            [
                Or(
                    [
                        Link(
                            "Set",
                            [
                                Variable("V1"),
                                Variable("V2"),
                                Variable("V3"),
                                Variable("V4"),
                            ],
                            False,
                        ),
                        Link(
                            "List",
                            [
                                Variable("V1"),
                                Variable("V2"),
                                Variable("V3"),
                                Variable("V4"),
                            ],
                            True,
                        ),
                    ]
                ),
                Not(
                    Or(
                        [
                            Link("Inheritance", [Variable("V1"), Variable("V2")], True),
                            Link("Similarity", [Variable("V1"), Variable("V2")], False),
                        ]
                    )
                ),
            ]
        ),
    )

    print(db.get_all_nodes('Concept'))
    print(db.get_all_nodes('blah'))

    match(db, LinkTemplate("Inheritance", [TypedVariable("V1", "Concept"), TypedVariable("V2", "Concept")], True))
    match(db, LinkTemplate("Similarity", [TypedVariable("V1", "Concept"), TypedVariable("V2", "Concept")], True))

    print(
        "\n\n\n\n================================================================================\n"
    )

    match(db, Link("Inheritance", [Variable("V1"), Variable("V2")], True))
    match(db, LinkTemplate("Inheritance", [TypedVariable("V1", "Concept"), TypedVariable("V2", "Concept")], True))
    match(db, Link("List", [Variable("V1"), Variable("V2")], True))
    match(db, LinkTemplate("List", [TypedVariable("V1", "Concept"), TypedVariable("V2", "Concept")], True))

    match(db, Link("Similarity", [Node("Concept", "human"), Variable("V1")], False))
    match(db, Link("Similarity", [Variable("V1"), Node("Concept", "human")], False))


