from function.das.pattern_matcher.pattern_matcher import Node, Link, And, Or, Not, Variable, LogicalExpression

class LogicalExpressionParser:
    def from_dict(self, query: dict) -> LogicalExpression:
        key = next(iter(query))
        value = query[key]

        if key == "Variable":
            return Variable(value["variable_name"])
        elif key == "Node":
            return Node(value["node_type"], value["node_name"])
        elif key == "Link":
            ordered = value.get("ordered", False)
            targets = [self.from_dict(target) for target in value["targets"]]
            return Link(value["link_type"], targets, ordered)
        elif key == "And":
            conditions = [self.from_dict(condition) for condition in value]
            return And(conditions)
        elif key == "Or":
            conditions = [self.from_dict(condition) for condition in value]
            return Or(conditions)
        elif key == "Not":
            condition = self.from_dict(value)
            return Not(condition)
