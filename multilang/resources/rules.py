#!/usr/bin/python

import storm, os, sys, time, json, zlib

class PyBolt(storm.BasicBolt):
  def initialize(self, stormconf, context):
    self.modules = {}

  # Import/Reload each module
  def define_rule(self, fn):
    module = fn.split('.')[0]
    if module not in self.modules: self.modules[module] = __import__(module)
    else: self.modules[module] = reload(self.modules[module])

  # Write rule to local fs
  def write_rule(self, fn, content):
    with open(fn, 'w') as fp: fp.write(content)

  def process(self, tuple):
    storm.log(tuple.component + ': ' + str(tuple.values))

    # Write rules to local fs and reload
    if tuple.component == 'rule-definitions':
      fn = tuple.values[0].split('/')[-1]
      self.write_rule(fn, tuple.values[1])
      self.define_rule(fn)
    # otherwise, apply each rule to each tuple
    else:
      for module in self.modules:
        output = self.modules[module].handle(tuple)
        storm.emit([output])

PyBolt().run()
