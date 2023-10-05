from function.das.expression_hasher import ExpressionHasher
import os

def sort_file(file_name):
    os.system(f"sort -t , -k 1,1 {file_name} > {file_name}.sorted")
    os.rename(f"{file_name}.sorted", file_name)

def write_key_value(file, key, value):
    if isinstance(key, list):
        key = ExpressionHasher.composite_hash(key)
    if isinstance(value, list):
        value = "\t".join(value)
    line = f"{key}\t{value}"
    file.write(line)
    file.write("\n")

def key_value_generator(input_filename, *, block_size=None, merge_rest=False):
    last_key = ''
    last_list = []
    block_count = 0
    global KEY_VALUE_LINE_COUNTER
    KEY_VALUE_LINE_COUNTER = 0
    with open(input_filename, 'r') as fh:
        for line in fh:
            KEY_VALUE_LINE_COUNTER += 1
            line = line.strip()
            if line == '':
                continue
            if merge_rest:
                v = line.split("\t")
                key = v[0]
                value = ",".join(v[1:])
            else:
                key, value = line.split("\t")
            if last_key == key:
                last_list.append(value)
                if block_size and len(last_list) >= block_size:
                    yield last_key, last_list, block_count
                    block_count += 1
                    last_list = []
            else:
                if last_key != '':
                    yield last_key, last_list, block_count
                block_count = 0
                last_key = key
                last_list = [value]
    if last_key != '':
        yield last_key, last_list, block_count

def key_value_targets_generator(input_filename, *, block_size=None, merge_rest=False):
    last_key = ''
    last_list = []
    global KEY_VALUE_LINE_COUNTER
    KEY_VALUE_LINE_COUNTER = 0
    block_count = 0
    with open(input_filename, 'r') as fh:
        for line in fh:
            KEY_VALUE_LINE_COUNTER += 1
            line = line.strip()
            if line == '':
                continue
            key, value, *targets = line.split("\t")
            if last_key == key:
                last_list.append(tuple([value, tuple(targets)]))
                if block_size and len(last_list) >= block_size:
                    yield last_key, last_list, block_count
                    block_count += 1
                    last_list = []
            else:
                if last_key != '':
                    yield last_key, last_list, block_count
                block_count = 0
                last_key = key
                last_list = [tuple([value, tuple(targets)])]
    if last_key != '':
        yield last_key, last_list, block_count
