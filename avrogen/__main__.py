"""Usage:
  python -m avrogen /path/to/protocol.avdl [-o /path/to/output]
"""

from .protocol import write_protocol_files
from os.path import join
from sys import argv

def main():

	if "-o" in argv:
		avdl, _, output = argv[1:]
	else:
		avdl, output = argv[1], './'
	write_protocol_files(open(avdl, 'r').read(), output)

if __name__ == '__main__':
	try:
		exit(main())
	except Exception:
		print(__doc__)
		raise