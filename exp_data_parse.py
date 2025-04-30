import ast
import argparse


def parse_float_seqs(fn):
    parsed_data = []
    try:
        with open(fn, 'r') as file:
            for line in file:
                line = line.strip()
                if line:
                    try:
                        float_seq = ast.literal_eval(line)
                        if isinstance(float_seq, list):
                            parsed_data.append(float_seq)
                        else:
                            raise
                    except (ValueError):
                        print("ValueError")
    except FileNotFoundError:
        print(f"{fn} not found")
    return parsed_data


if __name__ == '__main__':
    # Example usage:
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, default='rt_ablation_delta2_20250414_144203.txt')
    args = parser.parse_args()

    # 1. Create a dummy text file for demonstration
    # with open("float_sequences.txt", "w") as f:
    #     f.write("[1.0, 1.2, 3.5]\n")
    #     f.write("[2.1, 2.5, 2.9, 3.3]\n")
    #     f.write("[0.5, 0.8]\n")
    #
    # 2. Parse the file
    # file_path = "float_sequences.txt"
    file_path = args.file
    parsed_sequences = parse_float_seqs(file_path)

    # 3. Print the parsed data
    print("Parsed data:")
    for sequence in parsed_sequences:
        print(sequence)
