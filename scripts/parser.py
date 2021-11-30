#!/usr/bin/env python3
import argparse
from datetime import datetime
from os import path

from atomese2metta.parser import LexParser
from atomese2metta.translator import Translator
from helpers import get_filesize_mb, human_time


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

    lex_parser = LexParser()

    with open(file_path, "r") as f:
      parsed_expressions = lex_parser.parse(f.read())

    print(f"Took {human_time((datetime.now() - d2))} to parse string.")

    mettas.append(Translator.build(parsed_expressions))
    print(f"Took {human_time((datetime.now() - d2))} to build Metta document.")
    print(f"Partial time of processing: {human_time((datetime.now() - d1))}")

  print(f"Took {human_time((datetime.now() - d1))} to process all files")

  metta = mettas[0]
  if len(mettas) > 1:
    d3 = datetime.now()
    metta = sum(mettas[1:], start=metta)
    print(f"Took {human_time((datetime.now() - d3))} to merge MeTTa documents.")

  output_file_path = path.join(output_dir, output_name)
  with open(output_file_path, "w") as f:
    metta.write_to(f)

  print(f"Outputting to {output_file_path}")
  print(f"Took {human_time((datetime.now() - d1))} to finish processing.")


if __name__ == "__main__":
  parser = argparse.ArgumentParser("Converter scm files to MeTTa")

  parser.add_argument('filenames', type=str, nargs='+')
  parser.add_argument('--output-name', type=str)
  parser.add_argument('--output-dir', type=str, default='./')
  args = parser.parse_args()

  main(**vars(args))
