#!/usr/bin/env python
import pkg_resources
from scribedb import scribedb
import sys

dver = pkg_resources.get_distribution('scribedb').version
initver = scribedb.__version__

if dver != initver:
    print('Version in __init__ (%s) does not match '
          'distribution version (%s)' % (dver, initver))
    sys.exit(1)
