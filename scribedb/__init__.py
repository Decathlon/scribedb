
"""
scribeDB is a light tool which compares data at schema level.
Let us say we have two schemas deployed inside PostgreSQL
and Oracle RDBs.
A minimal usage example:
#
"""

from . import oracle, postgres, rdbms, scribedb

__version__ = '0.1.0'

__all__ = ['__version__', 'oracle', 'postgres', 'scribedb','rdbms']
