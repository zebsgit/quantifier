from  quantifier import Quantifier
from argparse import ArgumentParser
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    """
    AWS Lambda function handler
    :param event: 
    :param context: The context in which the function is called.
    """
    print('Event: %s', event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    file = 's3://{}/{}'.format(bucket,key)
    process(file)
    return

def parse_arguments():
    """Function to parse argument from command line
    Returns:
        object: argument parser object
    """
    parser = ArgumentParser()
    parser.add_argument("-i", nargs='?', default=None, help="specify input file path")
    parser.add_argument("-o", nargs='?', default=None, help="specify output file path")
    return parser.parse_args()
  
def process(input,output=None):
    """Function to process the data
    Args:
        input (string): input file 
        output (string): output file
    Raises:
        Exception: Returns a message with error description
    """
    try:
        revenueQuantifier = Quantifier(input, output)
        data = revenueQuantifier.ReadFile()
        if data.empty:
          raise Exception('No data in input file')
        transformed_data = revenueQuantifier.TransformData(data)
        revenueQuantifier.WriteToCSV(transformed_data)
        print('Job completed successfully')
    except Exception as e:
        raise
        print('Job completed with errors {}'.format(e))

if __name__ == "__main__":
  args = parse_arguments()
  process(args.i,args.o)