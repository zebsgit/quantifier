from  quantifier import Quantifier
from argparse import ArgumentParser
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    """
    Accepts an action and a number, performs the specified action on the number,
    and returns the result.

    :param event: The event dict that contains the parameters sent when the function
                  is invoked.
    :param context: The context in which the function is called.
    :return: The result of the specified action.
    """
    print('Event: %s', event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    file = 's3://{}/{}'.format(bucket,key)
    process(filter)

def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument("--file", nargs='?', default=None, help="specify file path")
    return parser.parse_args()
  
def process(file):
    try:
        revenueQuantifier = Quantifier(file)
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

# if __name__ == "__main__":
#   args = parse_arguments()
#   process(args.file)