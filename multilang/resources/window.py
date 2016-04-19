#!/usr/bin/python

import storm, os, sys, time, json, zlib

class PyBolt(storm.BasicBolt):
  def initialize(self, stormconf, context):
    self.payloads = {}
    self.compression_level = 6
    self.filler_component = stormconf['filler_component']

  def process(self, tuple):
    storm.log(tuple.component + ': ' + str(tuple.values))
    #if filler arrives, add it to the window
    if tuple.component == self.filler_component:
      key = tuple.values[0]
      payload = zlib.compress(tuple.values[1], self.compression_level)
      if key in self.payloads: self.payloads[key].append(payload)
      else: self.payloads[key] = [payload]
      storm.log('Filler ' + key + ': ' + str(len(self.payloads[key])))
    #otherwise check for match enrichment keys and emit the enriched event
    else:
      key = tuple.values[0]
      storm.log('Non-filler: ' + str(tuple.values))
      if key in self.payloads:
        out = []
        for payload in self.payloads[key]: out.append(zlib.decompress(payload))
        storm.log('Emitting ' + str(len(out)) + ' payloads for ' + key)
        storm.emit([tuple.values, out])

PyBolt().run()
