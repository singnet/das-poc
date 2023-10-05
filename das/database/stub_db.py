import re
from typing import List, Any, Tuple

from function.das.database.db_interface import DBInterface
from function.das.pattern_matcher.pattern_matcher import WILDCARD


def _build_node_handle(node_type: str, node_name: str) -> str:
    return f'<{node_type}: {node_name}>'

def _split_node_handle(node_handle: str) -> Tuple[str, str]:
    v = re.split('[<: >]', node_handle)
    return tuple([v[1], v[3]])

def _build_link_handle(link_type: str, target_handles: List[str]) -> str:
    if link_type == 'Similarity' or link_type == 'Set':
        target_handles.sort()
    return f'<{link_type}: {target_handles}>'
    
class StubDB(DBInterface):

    def __init__(self):

        human = _build_node_handle('Concept', 'human')
        monkey = _build_node_handle('Concept', 'monkey')
        chimp = _build_node_handle('Concept', 'chimp')
        snake = _build_node_handle('Concept', 'snake')
        earthworm = _build_node_handle('Concept', 'earthworm')
        rhino = _build_node_handle('Concept', 'rhino')
        triceratops = _build_node_handle('Concept', 'triceratops')
        vine = _build_node_handle('Concept', 'vine')
        ent = _build_node_handle('Concept', 'ent')
        mammal = _build_node_handle('Concept', 'mammal')
        animal = _build_node_handle('Concept', 'animal')
        reptile = _build_node_handle('Concept', 'reptile')
        dinosaur = _build_node_handle('Concept', 'dinosaur')
        plant = _build_node_handle('Concept', 'plant')
    
        self.all_nodes = [human, monkey, chimp, snake, earthworm, rhino, triceratops, vine, ent, mammal, animal, reptile, dinosaur, plant]

        self.all_links = [
            ['Similarity', human, monkey],
            ['Similarity', human, chimp],
            ['Similarity', chimp, monkey],
            ['Similarity', snake, earthworm],
            ['Similarity', rhino, triceratops],
            ['Similarity', snake, vine],
            ['Similarity', human, ent],
            ['Inheritance', human, mammal],
            ['Inheritance', monkey, mammal],
            ['Inheritance', chimp, mammal],
            ['Inheritance', mammal, animal],
            ['Inheritance', reptile, animal],
            ['Inheritance', snake, reptile],
            ['Inheritance', dinosaur, reptile],
            ['Inheritance', triceratops, dinosaur],
            ['Inheritance', earthworm, animal],
            ['Inheritance', rhino, mammal],
            ['Inheritance', vine, plant],
            ['Inheritance', ent, plant],
            ['List', _build_link_handle('Inheritance', [dinosaur, reptile]), _build_link_handle('Inheritance', [triceratops, dinosaur])],
            ['Set', _build_link_handle('Inheritance', [dinosaur, reptile]), _build_link_handle('Inheritance', [triceratops, dinosaur])],
            ['List', human, ent, monkey, chimp],
            ['List', human, mammal, triceratops, vine],
            ['List', human, monkey, chimp],
            ['List', triceratops, ent, monkey, snake],
            ['Set', triceratops, vine, monkey, snake],
            ['Set', triceratops, ent, monkey, snake],
            ['Set', human, ent, monkey, chimp],
            #['Set', vine, monkey, plant, triceratops],
            ['Set', mammal, monkey, human, chimp],
            ['Set', human, monkey, chimp]
        ]
        self.template_index = {}
        for link in self.all_links:
            key = [link[0]]
            for target in link[1:]:
                node_type, node_name = _split_node_handle(target)
                key.append(node_type)
            key = str(key)
            v = self.template_index.get(key, [])
            v.append([_build_link_handle(link[0], link[1:]), link[1:]])
            self.template_index[key] = v

    def __repr__(self):
        return '<StubDB>'

    def node_exists(self, node_type: str, node_name: str) -> bool:
        return _build_node_handle(node_type, node_name) in self.all_nodes

    def link_exists(self, link_type: str, targets: List[str]) -> bool:
        return _build_link_handle(link_type, targets) in [_build_link_handle(link[0], link[1:]) for link in self.all_links]

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = _build_node_handle(node_type, node_name)
        for node in self.all_nodes:
            if node == node_handle:
                return node
        return None

    def is_ordered(self, handle: str) -> bool:
        for link in self.all_links:
            if _build_link_handle(link[0], link[1:]) == handle:
                return link[0] != 'Similarity' and link[0] != 'Set'
        return True

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        for link in self.all_links:
            if link[0] == link_type and len(target_handles) == (len(link) - 1):
                if link_type == 'Similarity':
                    if all(target in target_handles for target in link[1:]):
                        return _build_link_handle(link_type, link[1:])
                elif link_type == 'Inheritance':
                    for i in range(0, len(target_handles)):
                        if target_handles[i] != link[i + 1]:
                            break
                    else:
                        return _build_link_handle(link_type, target_handles)
                else:
                    raise ValueError(f"Invalid link type: {link_type}")
        return None

    def get_link_targets(self, handle: str) -> List[str]:
        for link in self.all_links:
            if _build_link_handle(link[0], link[1:]) == handle:
                return link[1:]
        return None

    def get_matched_links(self, link_type: str, target_handles: List[str]):
        answer = []
        for link in self.all_links:
            if len(target_handles) == (len(link) - 1) and link[0] == link_type:
                if link[0] == 'Similarity' or link[0] == 'Set':
                    if all(target == WILDCARD or target in link[1:] for target in target_handles):
                        link_target_handles = link[1:]
                        link_target_handles.sort
                        answer.append([_build_link_handle(link[0], link[1:]), link_target_handles])
                elif link[0] == 'Inheritance' or link[0] == 'List':
                    for i in range(0, len(target_handles)):
                        if target_handles[i] != WILDCARD and target_handles[i] != link[i + 1]:
                            break
                    else:
                        answer.append([_build_link_handle(link[0], link[1:]), link[1:]])
                else:
                    raise ValueError(f"Invalid link type: {link[0]}")
        return answer

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        return self.all_nodes if node_type == 'Concept' else []
        if node_type != 'Concept':
            return []
        answer = []
        for node in self.all_nodes:
            if names:
                t, n = _split_node_handle(node)
                answer.append(n)
            else:
                answer.append(node)
        return answer

    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        assert len(template) == 3
        return self.template_index.get(str(template), [])
        
    def get_node_name(self, node_handle: str) -> str:
        _, name = _split_node_handle(node)
        return name

    def get_matched_node_name(self, node_type: str, substring: str) -> str:
        answer = []
        if node_type == 'Concept':
            for node in self.all_nodes:
                _, name = _split_node_handle(node)
                if substring in name:
                    answer.append(node)
        return answer

    def get_matched_type(self, link_named_type: str):
        pass

    def get_atom_as_dict(self, handle: str, arity: int):
        pass

    def get_atom_as_deep_representation(self, handle: str, arity: int):
        pass

    def count_atoms(self):
        return (len(self.all_nodes), len(self.all_links))
