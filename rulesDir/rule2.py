import sys

def handle(tup):
  sys.stderr.write(str(tup))
  return tup.values[0].lower()
