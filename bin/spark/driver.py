"""
Wrapper script to use when running Python packages via egg file through spark-submit.

Rename this script to the fully qualified package and module name you want to run.
The module should provide a ``main`` function.

Usage:

  spark-submit --py-files <LIST-OF-EGGS> driver.py <PACKAGE>.<MODULE> <MODULE_ARGS>
"""

import os
import importlib
import sys


def main():
    filename = sys.argv[1]
    sys.argv.remove(filename)

    module = os.path.splitext(filename)[0]
    module = importlib.import_module(module)
    module.main()


if __name__ == '__main__':
    main()
