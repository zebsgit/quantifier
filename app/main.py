from  quantifier import Quantifier
from argparse import ArgumentParser


def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument("--file", nargs='?', default=None, help="specify file path")
    return parser.parse_args()
  
def main():
    try:
        args = parse_arguments()
        revenueQuantifier = Quantifier(args.file)
        data = revenueQuantifier.ReadFile()
        if data.empty:
          raise Exception('No data in input file')
        transformed_data = revenueQuantifier.TransformData(data)
        print(transformed_data)
        revenueQuantifier.WriteToCSV(transformed_data)
        print('Job completed successfully')
    except Exception as e:
        raise
        print('Job completed with errors {}'.format(e))

if __name__ == "__main__":
    main()