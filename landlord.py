import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from scipy import stats
import random
import math

def create_profiles(num_events, intervals):
    lamb_dash = float(num_events / intervals)

    event_num = []
    inter_event_times = []
    event_times = []
    event_time = 0

    for i in range(num_events):
        event_num.append(i)
        #Get a random probability value from the uniform distribution's PDF
        n = random.random()
        #Generate the inter-event time from the exponential distribution's CDF using the Inverse-CDF technique
        inter_event_time = -math.log(1.0 - n) / lamb_dash

        inter_event_times.append(inter_event_time)
        #Add the inter-event time to the running sum to get the next absolute event time
        event_time = event_time + inter_event_time
        event_times.append(event_time)

    interval_nums = []
    num_events_in_interval = []
    interval_num = 1
    num_events = 0

    j = 0

    for inter in range(1, intervals+1):
        # print("Inter " + str(inter))
        if j < len(event_times):
            while event_times[j] <= inter:
                event_time = event_times[j]
                num_events += 1
                j += 1
                if j >= len(event_times):
                    break

        
        interval_nums.append(inter - 1)
        num_events_in_interval.append(num_events)
        num_events = 0

    return np.stack([np.array(interval_nums), np.array(num_events_in_interval)], axis=0)

def compute_contribution(init_soc, rate, capacity):
    if init_soc < 0.85:
        per_rate = rate
    else:
        per_rate = (rate/((init_soc - 0.8) * 100))

    new_soc = init_soc + (per_rate/2)/capacity
    return new_soc, per_rate

dates = pd.date_range(start='2019-01-01', end='2020-01-01', freq='30T', closed='left')

prices = pd.read_csv('./input/prices.csv')
prices['Date'] = pd.to_datetime(prices['date'] + ' ' + prices['from'])
start_date = '2019-01-01'
end_date = '2020-01-01'

mask = (prices['Date'] >= start_date) & (prices['Date'] < end_date)
prices = prices.loc[mask].reset_index(drop=True)

events = []

for d in dates[::48]:
    charge_high = create_profiles(6000, 28)[1, :]
    charge_low = create_profiles(10, 20)[1, :]
    
    charge = np.concatenate((charge_high, charge_low))
    charge = np.roll(charge, 12).tolist()

    events += charge

EVs = pd.DataFrame()
EVs['Events'] = events

num_chargers = [0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0]
charger_soc = [0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0]
demand = []
actual_events = []
rate = 11
capacity = 40
socs = []
demands = []

for index, row in EVs.iterrows():
    events = int(row['Events'])
    # print(events)
    evs = 0
    for k in range(events):
        for i in range(len(num_chargers)):
            if num_chargers[i] == 0:
                charger_soc[i] = np.random.normal(0.5, 0.11)
                num_chargers[i] = 4
                evs += 1
                break
    actual_events.append(evs)

    charge = []
  

    for i in range(len(num_chargers)):
        if num_chargers[i] != 0:
            num_chargers[i] -= 1
            per_chage, contrib = compute_contribution(charger_soc[i], rate, capacity)
            charger_soc[i] = per_chage

        else:
            contrib = 0
            
        charge.append(contrib)
    demand.append(sum(charge))
    # socs.append(charger_soc)
    demands.append(np.array(charge))

demands = np.array(demands)

lights = [0] * 48
for i in range(48):
    if i < 14:
        lights[i] = 0.1
    if i > 36:
        lights[i] = 0.1
lights = lights * 365
access = [0.5] * 48 * 365

stmarys = pd.DataFrame()
stmarys['Dates'] = dates
stmarys['EV_demand'] = demand
stmarys['Events'] = actual_events
stmarys['External_light'] = lights
stmarys['Access'] = access
stmarys['Price'] = prices['unit_rate_incl_vat'].to_numpy()
stmarys['kW'] = stmarys['EV_demand'] + stmarys['External_light'] + stmarys['Access']


ext_lights = [0] * 48
for i in range(48):
    if i < 14:
        ext_lights[i] = 0.6
    if i > 36:
        ext_lights[i] = 0.6
ext_lights = ext_lights * 365

int_lights = [0] * 48
for i in range(48):
    if i < 14:
        int_lights[i] = 2.5
    if i > 36:
        int_lights[i] = 2.5
int_lights = int_lights * 365

water = [12.9] * 48 * 365
small_power = [12.6] * 48 * 365

lifts = []

for d in dates[::48]:
    charge_high_1 = create_profiles(80, 8)[1, :]
    charge_high_2 = create_profiles(100, 10)[1, :]
    charge_low_1 = create_profiles(20, 14)[1, :]
    charge_low_2 = create_profiles(10, 16)[1, :]
    
    charge = np.concatenate((charge_high_1, charge_low_1, charge_high_2, charge_low_2))
    charge = np.roll(charge, -12).tolist()
    
    lifts += charge

events = []

for d in dates[::48]:
    charge_high = create_profiles(6000, 28)[1, :]
    charge_low = create_profiles(10, 20)[1, :]
    
    charge = np.concatenate((charge_high, charge_low))
    charge = np.roll(charge, 12).tolist()

    events += charge

EVs = pd.DataFrame()
EVs['Events'] = events

num_chargers = [0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0]
charger_soc = [0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0]
demand = []
actual_events = []
rate = 11
capacity = 40
socs = []
demands = []

for index, row in EVs.iterrows():
    events = int(row['Events'])
    # print(events)
    evs = 0
    for k in range(events):
        for i in range(len(num_chargers)):
            if num_chargers[i] == 0:
                charger_soc[i] = np.random.normal(0.5, 0.11)
                num_chargers[i] = 4
                evs += 1
                break
    actual_events.append(evs)

    charge = []
  

    for i in range(len(num_chargers)):
        if num_chargers[i] != 0:
            num_chargers[i] -= 1
            per_chage, contrib = compute_contribution(charger_soc[i], rate, capacity)
            charger_soc[i] = per_chage

        else:
            contrib = 0
            
        charge.append(contrib)
    demand.append(sum(charge))
    # socs.append(charger_soc)
    demands.append(np.array(charge))

demands = np.array(demands)

berkeley = pd.DataFrame()
berkeley['Dates'] = dates
berkeley['EV_demand'] = demand
berkeley['EV_Events'] = actual_events
berkeley['External_light'] = ext_lights
berkeley['Internal_light'] = int_lights
berkeley['Small_Power'] = small_power
berkeley['Boosted_Water'] = water
berkeley['Lifts'] = np.array(lifts) * 9.0 * (3/60)
berkeley['Lifts_Events'] = lifts
berkeley['Price'] = prices['unit_rate_incl_vat'].to_numpy()
berkeley['kW'] = berkeley['EV_demand'] + berkeley['External_light'] + berkeley['Internal_light'] + berkeley['Small_Power'] + berkeley['Boosted_Water'] + berkeley['Lifts']

comb = pd.DataFrame()
comb['Dates'] = dates
comb['kW'] = berkeley['kW'] + stmarys['kW']
comb['Price'] = prices['unit_rate_incl_vat'].to_numpy()

berkeley.to_pickle('./input/berkeley_11_80.plk')
stmarys.to_pickle('./input/stmarys_11_80.plk')

comb.to_pickle('./input/combined_11_80.plk')