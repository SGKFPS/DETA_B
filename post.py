import pandas as pd
from datetime import datetime, timedelta
import numpy as np

FPS_BLUE = '#004A9C'
FPS_GREEN = '#45D281'
FPS_YELLOW = '#FEC001'
FPS_PURPLE = '#A365E0'

result = pd.read_pickle('./capacity_constraints_res.plk')

result['Reduced'] = result['kWh'] + result['Deltas'] - result['Solar (kWh)'] + result['Curtailed']
result['Cost'] = result['Reduced'] * result['Price'] / 100
result['Reduced'] = result['Reduced'] * 2

results = pd.DataFrame()

results['Mean'] = result.groupby(['Capacity', 'Duration','ID' ])['Reduced'].mean()
results['Max'] = result.groupby(['Capacity', 'Duration','ID' ])['Reduced'].max()
results['90'] = result.groupby(['Capacity', 'Duration','ID' ])['Reduced'].quantile(.9)
results['99'] = result.groupby(['Capacity', 'Duration','ID' ])['Reduced'].quantile(.99)
results['Total'] = result.groupby(['Capacity', 'Duration','ID' ])['Reduced'].sum()
results['Cost'] = result.groupby(['Capacity', 'Duration','ID' ])['Cost'].sum()
results['Capacity'] = result.groupby(['Capacity', 'Duration','ID' ])['Capacity_Breach'].sum()

results.to_csv('./capacity_results.csv')