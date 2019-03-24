import sys


def convert(source, target, filename):
    try:
        with open(filename, "r", encoding=source) as input_file:
            output_filename = filename + "_" + target
            with open(output_filename, "w", encoding=target) as output_file:
                lines = input_file.readlines()
                for line in lines:
                    output_file.write(line)
    except UnicodeDecodeError as e:
        print(e)


if __name__ == "__main__":
    convert(sys.argv[1], sys.argv[2], sys.argv[3])