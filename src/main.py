import argparse
import sys
import os
from utils.FileLoader import FileLoader
from utils.FileRunner import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser("RepCRec")
    parser.add_argument("mode", type=str)
    parser.add_argument("-input", type=str, help="input source")
    parser.add_argument("-output", type=str, help="output source")
    args = parser.parse_args()

    mode, input_src, output_src = args.mode, args.input, args.output

    if args.mode == "r":
        run_by_file(input_src, output_src)
