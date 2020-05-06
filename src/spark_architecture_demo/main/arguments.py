from argparse import ArgumentParser
import datetime


class Arguments:

    def __init__(self):
        self.parser = ArgumentParser()
        self.parser.add_argument("--gender", help="Gender")

    def get_gender(self):
        args = self.parser.parse_args()
        return args.gender
