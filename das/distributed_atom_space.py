"""
Distributed Atom Space

"""

import os
from das.metta_parser_actions import MultiFileKnowledgeBase
from das.metta_yacc import MettaYacc

class DistributedAtomSpace:

    def __init__(self, **kwargs):
        self._read_knowledge_base(kwargs)

    def _get_file_list(self, file_name, dir_name):
        """
        Build a list of file names according to the passed parameters.
        If file_name is not none, a list with a single file name is built
        (provided the the file is .metta). If a dir name is passed, all .metta
        files in that dir (no recursion) are returned in the list.
        Only .metta files are considered. 

        file_name and dir_name should not be simultaneously None or not None. This
        check is made in the caller.
        """
        answer = []
        if file_name:
            if os.path.exists(file_name):
                answer.append(file_name)
            else:
                raise ValueError(f"Invalid file name: {file_name}")
        else:
            if os.path.exists(dir_name):
                for file_name in os.listdir(dir_name):
                    path = "/".join([dir_name, file_name])
                    if os.path.exists(path):
                        answer.append(path)
            else:
                raise ValueError(f"Invalid folder name: {dir_name}")
        answer = [f for f in answer if f.endswith(".metta")]
        if len(answer) == 0:
            raise ValueError(f"No MeTTa files found")
        return answer
        
    def _read_knowledge_base(self, kwargs):
        """
        Called in constructor, this method parses one or more files passed
        by kwargs and feed the databases with all MeTTa expressions.
        """
        knowledge_base_file_name = kwargs.get("knowledge_base_file_name", None)
        knowledge_base_dir_name = kwargs.get("knowledge_base_dir_name", None)
        if not knowledge_base_file_name and not knowledge_base_dir_name:
            raise ValueError("Either 'knowledge_base_file_name' or 'knowledge_base_dir_name' should be provided")
        if knowledge_base_file_name and knowledge_base_dir_name:
            raise ValueError("'knowledge_base_file_name' and 'knowledge_base_dir_name' can't be set simultaneously")
        knowledge_base_file_list = self._get_file_list(knowledge_base_file_name, knowledge_base_dir_name)
        parser_actions_broker = MultiFileKnowledgeBase(knowledge_base_file_list)
        parser = MettaYacc(action_broker=parser_actions_broker)
        parser.parse_action_broker_input()
