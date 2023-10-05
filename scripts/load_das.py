import argparse
from function.das.distributed_atom_space import DistributedAtomSpace

def run():
    parser = argparse.ArgumentParser(
        "Load MeTTa data into DAS", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--knowledge-base', type=str, help='Path to a file or directory with a MeTTA knowledge base')
    parser.add_argument('--canonical', help='Optimized load for canonical knowledge bases', action='store_true')

    args = parser.parse_args()

    das = DistributedAtomSpace()

    if args.knowledge_base:
        if args.canonical:
            das.load_canonical_knowledge_base(args.knowledge_base)
        else:
            das.load_knowledge_base(args.knowledge_base)

if __name__ == "__main__":
    run()
