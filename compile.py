import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from scipy import stats
import random
import math

residential = pd.read_pickle('./input/residential.plk')

comb_7_20 = pd.read_pickle('./input/combined_7_20.plk')
comb_11_20 = pd.read_pickle('./input/combined_11_20.plk')
comb_7_40 = pd.read_pickle('./input/combined_7_40.plk')
comb_11_40 = pd.read_pickle('./input/combined_11_40.plk')
comb_7_80 = pd.read_pickle('./input/combined_7_80.plk')
comb_11_80 = pd.read_pickle('./input/combined_11_80.plk')

comb_7_20['kW'] += residential['kW']
comb_7_20['kWh'] = comb_7_20['kW'] * 0.5

comb_11_20['kW'] += residential['kW']
comb_11_20['kWh'] = comb_11_20['kW'] * 0.5

comb_7_40['kW'] += residential['kW']
comb_7_40['kWh'] = comb_7_40['kW'] * 0.5

comb_11_40['kW'] += residential['kW']
comb_11_40['kWh'] = comb_11_40['kW'] * 0.5

comb_7_80['kW'] += residential['kW']
comb_7_80['kWh'] = comb_7_80['kW'] * 0.5

comb_11_80['kW'] += residential['kW']
comb_11_80['kWh'] = comb_11_80['kW'] * 0.5

comb_7_20.to_pickle('./input/comb_7_20.plk')
comb_11_20.to_pickle('./input/comb_11_20.plk')
comb_7_40.to_pickle('./input/comb_7_40.plk')
comb_11_40.to_pickle('./input/comb_11_40.plk')
comb_7_80.to_pickle('./input/comb_7_80.plk')
comb_11_80.to_pickle('./input/comb_11_80.plk')
