#!/usr/bin/env python3
import argparse
import math
from datetime import datetime
from os import path
from pathlib import Path

from atomese2metta.parser import LexParser, MultiprocessingParser
from atomese2metta.translator import Translator, AtomType, Type
from hashing import Hasher


def get_filesize_mb(file_path):
    return math.ceil(Path(file_path).stat().st_size / 1024 / 1024)


def human_time(delta) -> str:
    seconds = delta.seconds
    if seconds < 1:
        return f"{ delta.microseconds } microseconds"
    elif seconds < 60:
        return f"{seconds} second(s)"
    else:
        return "{:d}:{:02d} minute(s)".format(seconds // 60, seconds % 60)

def evaluate_hash(hash_dict: dict, output_file):
    collisions = []
    node_types = 0
    nodes = 0
    expressions_root = 0
    expressions_non_root = 0
    hash_count = 0
    all_hashes = 0

    for key, value in hash_dict.items():
        hash_count += 1
        all_hashes += len(value)
        value = set(value)
        if len(value) > 1:
            collisions.append((key, value))
            print("Collision:", key, value)

        expr = value.pop()
        with open(output_file, 'a') as f:
            f.write(key)
            f.write(' ')
            f.write(str(expr))
            f.write('\n')

        if isinstance(expr, AtomType):
            if expr.type in (None, Type):
                node_types += 1
            else:
                nodes += 1
        else:
            if expr.is_root:
                expressions_root += 1
            else:
                expressions_non_root += 1

    print("1 - Collisions", len(collisions))
    print("2 - NodeTypes:", node_types)
    print("3 - Nodes:", nodes)
    print("4 - Expressions (is_root=True):", expressions_root)
    print("5 - Subexpressions (is_root=False):", expressions_non_root)
    print("6 - Hash Count:", hash_count)
    print("7 - Including duplicated: ", all_hashes)


def main(filenames, output_name=None, output_dir='./'):
    if output_name is None:
        output_name = path.basename(filenames[-1]).replace(".scm", ".metta")

    output_dir = path.abspath(output_dir)

    d1 = datetime.now()

    mettas = []

    for file_path in filenames:
        d2 = datetime.now()
        print(f"Processing: {file_path}")
        print(f"File size: {get_filesize_mb(file_path)} MB")

        parser = LexParser()

        with open(file_path, "r") as f:
            parsed_expressions = parser.parse(f.read())

        print(f"Took {human_time((datetime.now() - d2))} to parse string.")

        mettas.append(Translator.build(parsed_expressions))
        print(f"Took {human_time((datetime.now() - d2))} to build Metta document.")
        print(f"Partial time of processing: {human_time((datetime.now() - d1))}")

    print(f"Took {human_time((datetime.now() - d1))} to proccess all files")

    metta = mettas[0] 
    if len(mettas) > 1:
        d3 = datetime.now()
        metta = sum(mettas[1:], start=metta)
        print(f"Took {human_time((datetime.now() - d3))} to merge MeTTa documents.")

    print("Hashing metta document")
    d4 = datetime.now()
    hasher = Hasher(metta)
    hasher.hash_atom_types()
    print(f"Took {human_time((datetime.now() - d4))} to hash atom types.")
    hasher.hash_expressions()
    print(f"Took {human_time((datetime.now() - d4))} to hash document.")

    output_file_path = path.join(output_dir, output_name)
    evaluate_hash(hasher.hash_index, output_file_path+'i')

    with open(output_file_path, "w") as f:
        metta.write_to(f)

    print(f"Outputing to {output_file_path}")
    print(f"Took {human_time((datetime.now() - d1))} to finish processing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Converter scm files to MeTTa")

    parser.add_argument('filenames', type=str, nargs='+')
    parser.add_argument('--output-name', type=str)
    parser.add_argument('--output-dir', type=str, default='./')
    args = parser.parse_args()

    main(**vars(args))
