#!/usr/bin/python

import storm, os, sys, time, json

class PyBolt(storm.BasicBolt):
  def initialize(self, stormconf, context):
    self.conf = stormconf
    self.default_delimiter = stormconf['default_delimiter']

  def process(self, tuple):
    storm.log(tuple.component + ': ' + str(tuple.values))
    storm.emit(tuple.values[0].split(self.default_delimiter))

PyBolt().run()
