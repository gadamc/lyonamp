#!/usr/bin/env python

from ROOT import *
import sys

k = KSamba2KData()
k.ConvertFile(sys.argv[1], sys.argv[2])
