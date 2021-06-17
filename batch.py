import pandas as pd
import glob
import linprog as lp
import itertools
import dask
import numpy as np
from dask.diagnostics import ProgressBar
from scipy.interpolate import interp1d

# storage = [10, 4.8, 13.5, 6.0, 6.0, 12]
# charge = [3.3, 2, 5, 2.5, 3.6, 5.5]
# discharge = [-3.3, -2.4, -5, -2.3, -3.6, -5.5]

# storage = [13.5]
# charge = [5]
# discharge = [-5]
# storage =  [75, 150, 225, 300]
# duration =  [1, 2, 4, 6, 8]


storage =  [140, 220, 280, 440, 560, 880]
duration =  [ 2, 4, 6]
capacity_constraint  = 1398

if __name__ == '__main__':
    # berk = glob.glob('./users/berk/*')
    # mar = glob.glob('./users/mary/*')

    # # print(berk)
    # # print(mar)

    # berks = []
    # for b in berk:
    #     berks.append([lp.loadData(b), b[b.find('berk/')+5:-4]])

    # mars = []
    # for m in mar:
    #     mars.append([lp.loadData(m), m[m.find('mary/')+5:-4]])
    # # data, starts, ends = lp.loadData(input)
    

    # days = []
    # for m in mars:
    #     print(m[1])
    #     for start, end in zip(m[0][1], m[0][2]):
    #         mask = (m[0][0]['Date'] >= start) & (m[0][0]['Date'] < end)
    #         temp = m[0][0].loc[mask].reset_index(drop=True)    
    #         days.append([temp, [start], [end], m[1], 'mary'])
    
    # for m in berks:
    #     print(m[1])
    #     for start, end in zip(m[0][1], m[0][2]):
    #         mask = (m[0][0]['Date'] >= start) & (m[0][0]['Date'] < end)
    #         temp = m[0][0].loc[mask].reset_index(drop=True)    
    #         days.append([temp, [start], [end], m[1], 'berk'])


    # mar = lp.loadData('./landlords/stmarys.plk')
    # ber = lp.loadData('./landlords/berkeley.plk')
    
    # mary_user_comb = lp.loadData('./users/mary_combined_22.plk')
    # berk_user_comb = lp.loadData('./users/berk_combined_22.plk')

    # mary_user_comb13 = lp.loadData('./users/mary_combined_22_13.plk')
    # berk_user_comb13 = lp.loadData('./users/berk_combined_22_13.plk')
    # mary_user_comb25 = lp.loadData('./users/mary_combined_22_25.plk')
    # berk_user_comb25 = lp.loadData('./users/berk_combined_22_25.plk')
    # mary_user_comb63 = lp.loadData('./users/mary_combined_22_63.plk')
    # berk_user_comb63 = lp.loadData('./users/berk_combined_22_63.plk')

    # comb = lp.loadData('./landlords/combined_22.plk')

    # comb13 = lp.loadData('./landlords/combined_22_13.plk')
    # comb25 = lp.loadData('./landlords/combined_22_25.plk')
    # comb63 = lp.loadData('./landlords/combined_22_63.plk')

    # hub = lp.loadData('./hub/hub.plk')

    comb_7_20 = lp.loadData('./input/comb_7_20.plk')
    comb_11_20 = lp.loadData('./input/comb_11_20.plk')
    comb_7_40 = lp.loadData('./input/comb_7_40.plk')
    comb_11_40 = lp.loadData('./input/comb_11_40.plk')
    comb_7_80 = lp.loadData('./input/comb_7_80.plk')
    comb_11_80 = lp.loadData('./input/comb_11_80.plk')

    dates = pd.date_range(start='2019-01-01', end='2020-01-01', freq='30T', closed='left')

    solars = []

    solar = pd.read_csv('./input/solar12.csv')
    # print(solar)
    solar = solar.astype(float)
    # solar = solar[np.abs(solar) < 0.00000001] = 0
    solar[solar < 0.00000001] = 0.0
    solar = solar['Solar'].repeat(2).reset_index(drop=True)
    solar = solar[:-48]
    s = pd.DataFrame()
    s['Solar'] = solar
    s['Date'] = dates
    # print(s[s['Solar'] > 2])
    solars.append(s)

    print(solars)

    # s = pd.DataFrame()
    # s['Date'] = dates
    # s['Solar'] = np.array(solar) * 2
    # solars.append(s)

    # s = pd.DataFrame()
    # s['Date'] = dates
    # s['Solar'] = np.array(solar) * 5
    # solars.append(s)

    # s = pd.DataFrame()
    # s['Date'] = dates
    # s['Solar'] = np.array(solar) * 10
    # solars.append(s)


    # print(solars)

    days = []

    for start, end in zip(comb_7_20[1], comb_7_20[2]):
        mask = (comb_7_20[0]['Date'] >= start) & (comb_7_20[0]['Date'] < end)
        temp = comb_7_20[0].loc[mask].reset_index(drop=True)    
        days.append([temp, [start], [end], '7_20'])

    for start, end in zip(comb_11_20[1], comb_11_20[2]):
        mask = (comb_11_20[0]['Date'] >= start) & (comb_11_20[0]['Date'] < end)
        temp = comb_11_20[0].loc[mask].reset_index(drop=True)    
        days.append([temp, [start], [end], '11_20'])

    for start, end in zip(comb_7_40[1], comb_7_40[2]):
        mask = (comb_7_40[0]['Date'] >= start) & (comb_7_40[0]['Date'] < end)
        temp = comb_7_40[0].loc[mask].reset_index(drop=True)    
        days.append([temp, [start], [end], '7_40'])

    for start, end in zip(comb_11_40[1], comb_11_40[2]):
        mask = (comb_11_40[0]['Date'] >= start) & (comb_11_40[0]['Date'] < end)
        temp = comb_11_40[0].loc[mask].reset_index(drop=True)    
        days.append([temp, [start], [end], '11_40'])

    for start, end in zip(comb_7_80[1], comb_7_80[2]):
        mask = (comb_7_80[0]['Date'] >= start) & (comb_7_80[0]['Date'] < end)
        temp = comb_7_80[0].loc[mask].reset_index(drop=True)    
        days.append([temp, [start], [end], '7_80'])

    for start, end in zip(comb_11_80[1], comb_11_80[2]):
        mask = (comb_11_80[0]['Date'] >= start) & (comb_11_80[0]['Date'] < end)
        temp = comb_11_80[0].loc[mask].reset_index(drop=True)    
        days.append([temp, [start], [end], '11_80'])
    # for start, end in zip(hub[1], hub[2]):
    #     mask = (hub[0]['Date'] >= start) & (hub[0]['Date'] < end)
    #     temp = hub[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'hub'])

    # for start, end in zip(comb[1], comb[2]):
    #     mask = (comb[0]['Date'] >= start) & (comb[0]['Date'] < end)
    #     temp = comb[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land13'])

    # for start, end in zip(comb25[1], comb25[2]):
    #     mask = (comb25[0]['Date'] >= start) & (comb25[0]['Date'] < end)
    #     temp = comb25[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land25'])

    # for start, end in zip(comb63[1], comb63[2]):
    #     mask = (comb63[0]['Date'] >= start) & (comb63[0]['Date'] < end)
    #     temp = comb63[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land63'])

    # for start, end in zip(mary_user_comb[1], mary_user_comb[2]):
    #     mask = (mary_user_comb[0]['Date'] >= start) & (mary_user_comb[0]['Date'] < end)
    #     temp = mary_user_comb[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land_berk_mary13'])
    
    # for start, end in zip(mary_user_comb25[1], mary_user_comb25[2]):
    #     mask = (mary_user_comb25[0]['Date'] >= start) & (mary_user_comb25[0]['Date'] < end)
    #     temp = mary_user_comb25[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land_berk_mary25'])
    
    # for start, end in zip(mary_user_comb63[1], mary_user_comb63[2]):
    #     mask = (mary_user_comb63[0]['Date'] >= start) & (mary_user_comb63[0]['Date'] < end)
    #     temp = mary_user_comb63[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land_berk_mary63'])

    # for start, end in zip(berk_user_comb[1], berk_user_comb[2]):
    #     mask = (berk_user_comb[0]['Date'] >= start) & (berk_user_comb[0]['Date'] < end)
    #     temp = berk_user_comb[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land_berk13'])

    # for start, end in zip(berk_user_comb25[1], berk_user_comb25[2]):
    #     mask = (berk_user_comb25[0]['Date'] >= start) & (berk_user_comb25[0]['Date'] < end)
    #     temp = berk_user_comb25[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land_berk25'])

    # for start, end in zip(berk_user_comb63[1], berk_user_comb63[2]):
    #     mask = (berk_user_comb63[0]['Date'] >= start) & (berk_user_comb63[0]['Date'] < end)
    #     temp = berk_user_comb63[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'land_berk63'])

    # for start, end in zip(mar[1], mar[2]):
    #     mask = (mar[0]['Date'] >= start) & (mar[0]['Date'] < end)
    #     temp = mar[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'mary'])
    
    # for start, end in zip(ber[1], ber[2]):
    #     mask = (ber[0]['Date'] >= start) & (ber[0]['Date'] < end)
    #     temp = ber[0].loc[mask].reset_index(drop=True)    
    #     days.append([temp, [start], [end], 'berk'])
    # days = []

    print(len(days))
    pbar = ProgressBar()
    dask.config.set(scheduler='processes', num_workers=12)

    result = []
    for st in storage:
        for dur in duration:
            for sol in solars:
                result += [dask.delayed(lp.runModel)(d[0], d[1], d[2], sol, maxCapacity=(st * dur),  initSOC=(st * dur) * 0.1, 
                charge=(st * 0.5), discharge=(-1 * st * 0.5), meta=[d[3], dur, sol['Solar'].max()], const=(capacity_constraint * 0.5)) for d in days]

    print("Waaaaaaat!?!?!?")
    print(len(result))
    pbar.register()
    result = dask.compute(*result)
    final = pd.concat(result)

    final = final.set_index(['ID', 'Capacity', 'Date'])

    print(final)

    final.to_pickle('./capacity_constraints_res.plk')