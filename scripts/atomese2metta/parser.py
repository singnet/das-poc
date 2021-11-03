import os
import pickle

from io import StringIO, TextIOWrapper
import re
import multiprocessing
from typing import Iterator, Optional, Union

from pyparsing import OneOrMore, nestedExpr

from lex import Lex


class Parser:
    @staticmethod
    def _parse(text):
        return OneOrMore(nestedExpr()).parseString(text).asList()

    def parse(self, text):
        return self._parse(text)


class LexParser(Parser):
    @staticmethod
    def _parse(text):
        lex = Lex()
        lex.build()

        list_stack = []
        current = []

        for (_, token_type, value) in lex.get_tokens(text):
            if token_type == "LPAREN":
                _pointer = []
                current.append(_pointer)
                list_stack.append(current)
                current = _pointer
            elif token_type == "RPAREN":
                current = list_stack.pop()
            else:
                current.append(value)

        if (len_list_stack := len(list_stack)) > 0:
            raise ValueError(f"list_stack length invalid: {len_list_stack}")

        return current


class MultiprocessingParser(Parser):
    def __init__(self, chunk_size: Optional[int] = None, cpus: Optional[int] = None):
        self.chunk_size = chunk_size if chunk_size is not None else 1
        self.cpus = cpus if cpus is not None else multiprocessing.cpu_count()
        self.manager = multiprocessing.Manager()
        self.workers_data = {i: self.manager.dict() for i in range(self.cpus)}
        self._counter_file = 0

    def _next_file_name(self):
        self._counter_file += 1
        return f"output_{self._counter_file}.pickle"

    @staticmethod
    def _count_paren_diff(text: str) -> int:
        text = re.sub(r"(\".*\")", "", text)
        return text.count("(") - text.count(")")

    def _split_expressions(
        self, file: Union[list[str], str, StringIO, TextIOWrapper]
    ) -> Iterator[str]:
        if isinstance(file, str):
            file = file.split("\n")

        counter = 0
        # TODO: Make expressions a set type.
        expressions = []
        expression = ""

        for line in file:
            if not line:
                continue
            counter += self._count_paren_diff(line)
            expression += line.replace("\n", "")
            if counter == 0:
                expressions.append(expression)
                expression = ""

                if len(expressions) >= self.chunk_size:
                    yield "".join(expressions)
                    expressions = []

        if len(expressions) > 0:
            yield "".join(expressions)

    @staticmethod
    def _parse(expressions: str, d):
        d["parser"] = OneOrMore(nestedExpr()).parseString(expressions).asList()

    def _resolve_workers(self, workers) -> str:
        result = []
        for worker in workers:
            worker.join()
        for _, data in self.workers_data.items():
            if "parser" in data:
                result.append(data["parser"])

        filename = self._next_file_name()
        with open(filename, "wb") as file:
            pickle.dump(sum(result, start=[]), file)
        return filename

    def parse(self, file: Union[list[str], str, StringIO, TextIOWrapper]):
        workers = []
        result = []
        for i, expressions in enumerate(self._split_expressions(file)):
            proc_num = i % self.cpus
            worker = multiprocessing.Process(
                target=self._parse, args=(expressions, self.workers_data[proc_num])
            )
            workers.append(worker)
            worker.start()

            if proc_num >= self.cpus - 1:
                result.append(self._resolve_workers(workers))
                workers = []

        if len(workers) > 0:
            result.append(self._resolve_workers(workers))

        data = sum([pickle.load(open(filename, "rb")) for filename in result], start=[])
        for filename in result:
            os.remove(filename)

        return data
