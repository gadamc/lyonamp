#!/usr/bin/env python

from ROOT import *
import sys

k = KAmpKounselor()
kg = KGrandCanyonKAmpSite()
k.AddKAmpSite(kg)
k.RunKamp(sys.argv[1], sys.argv[2])
