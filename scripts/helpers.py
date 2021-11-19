import math
from pathlib import Path

from atomese2metta.translator import AtomType, Type


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


def evaluate_hash(hash_dict: dict, output_file: str = '', logger=None):
  collisions = []
  node_types = 0
  nodes = 0
  expressions_root = 0
  expressions_non_root = 0
  hash_count = 0
  all_hashes = 0

  _print = logger.info if logger else print

  for key, value in hash_dict.items():
    hash_count += 1
    all_hashes += len(value)
    value = set(value)
    if len(value) > 1:
      collisions.append((key, value))
      _print(f"Collision: {key} {value}")

    expr = value.pop()
    if output_file:
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

  _print(f"----------- HASHES REPORT -----------")
  _print(f"1 - Collisions               : {len(collisions)}")
  _print(f"2 - NodeTypes                : {node_types}")
  _print(f"3 - Nodes                    : {nodes}")
  _print(f"4 - Expressions (is_root)    : {expressions_root}")
  _print(f"5 - Subexpressions (!is_root): {expressions_non_root}")
  _print(f"6 - Hash Count               : {hash_count}")
  _print(f"7 - Hash Count(w/ duplicated): {all_hashes}")
