from report import PdfMaker
from logDataclasses import TestData
from logExtractor import PytestArtifactLogExtractor
import argparse
import re

def regex_type(pattern: str | re.Pattern):
    """Argument type for matching a regex pattern."""

    def closure_check_regex(arg_value):  
        if not re.match( pattern, arg_value):
            raise argparse.ArgumentTypeError("invalid value")
        return arg_value

    return closure_check_regex


def parser_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path',
                        required=True, 
                        default='*',
                        help='Path of the pytest output artifact')

    return parser.parse_args()  # Parse the arguments

if __name__ == '__main__':

    parser = parser_arguments()
    p  = PytestArtifactLogExtractor(parser.file_path)

    print(f'Extracting data out of {parser.file_path}')
    execution_entity, artifact, tests, execution_time, failures = p.log_to_df()
    t = TestData(execution_entity=[execution_entity], artifact=[artifact], tests=[tests], execution_time=[execution_time], failures=[failures])
#
    #print('Generating Relatory...')
    #pdf = PdfMaker(t)
    #pdf.create_pdf()
