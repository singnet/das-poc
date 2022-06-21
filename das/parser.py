#!/usr/bin/env python3
import argparse
from datetime import datetime
from os import path

from das.atomese2metta.parser import LexParser
from das.atomese2metta.translator import Translator
from das.helpers import get_filesize_mb, get_logger, human_time

logger = get_logger()


def main(filenames, output_name=None, output_dir='./'):
  if output_name is None:
    output_name = path.basename(filenames[-1]).replace(".scm", ".metta")

  output_dir = path.abspath(output_dir)

  d1 = datetime.now()

  mettas = []

  for file_path in filenames:
    d2 = datetime.now()
    logger.info(f"Processing: {file_path}")
    logger.info(f"File size: {get_filesize_mb(file_path)} MB")

    lex_parser = LexParser()

    with open(file_path, "r") as f:
      parsed_expressions = lex_parser.parse(f.read())

    logger.info(f"Took {human_time((datetime.now() - d2))} to parse string.")

    mettas.append(Translator.build(parsed_expressions))
    logger.info(f"Took {human_time((datetime.now() - d2))} to build Metta document.")
    logger.info(f"Partial time of processing: {human_time((datetime.now() - d1))}")

  logger.info(f"Took {human_time((datetime.now() - d1))} to process all files")

  metta = mettas[0]
  if len(mettas) > 1:
    d3 = datetime.now()
    metta = sum(mettas[1:], start=metta)
    logger.info(f"Took {human_time((datetime.now() - d3))} to merge MeTTa documents.")

  output_file_path = path.join(output_dir, output_name)
  with open(output_file_path, "w") as f:
    metta.write_to(f)

  logger.info(f"Outputting to {output_file_path}")
  logger.info(f"Took {human_time((datetime.now() - d1))} to finish processing.")


def run():
  parser = argparse.ArgumentParser("Converter scm files to MeTTa")

  parser.add_argument('filenames', type=str, nargs='+')
  parser.add_argument('--output-name', type=str)
  parser.add_argument('--output-dir', type=str, default='./')
  args = parser.parse_args()

  main(**vars(args))


if __name__ == "__main__":
  run()
