#!/usr/bin/env python3

import math
from os import path
from sys import argv
from datetime import datetime
from pathlib import Path

from .atomese2metta.parser import MultiprocessingParser
from .atomese2metta.translator import Translator

def parseArgs(fn):
    def inner(args):
        if len(args) < 1:
            print("Must provide file path")
            exit(1)

        file_path = args[0]

        output_dir = path.abspath("./") if len(args) < 2 else args[1]

        return fn(file_path, output_dir)
    return inner


def get_filesize_mb(file_path):
    return math.ceil(Path(file_path).stat().st_size / 1024 / 1024)

@parseArgs
def main(file_path, output_dir):

    print(f"Processing: {file_path}")
    print(f"File size: {get_filesize_mb(file_path)} MB")

    d1 = datetime.now()
    parser = MultiprocessingParser(2000, cpus=8)

    with open(file_path, "r") as f:
        parsed_expressions = parser.parse(f)

    print(f"Took {(datetime.now() - d1).seconds} seconds to parse string.")

    metta = Translator.build(parsed_expressions)
    print(f"Took {(datetime.now() - d1).seconds} seconds to collect types.")

    output_file_path = path.join(
        output_dir, path.basename(file_path).replace(".scm", ".metta")
    )
    with open(output_file_path, "w") as f:
        metta.write_to(f)
    print(f"Outputing to {output_file_path}")

    print(f"Took {(datetime.now() - d1).seconds} seconds to finish processing.")

if __name__ == "__main__":
    main(argv[1:])
