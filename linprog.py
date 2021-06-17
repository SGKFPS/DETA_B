""" DETA Storage Management Implementation. Linear programming implementation using 'CVX'
Flexible Power Systems Ltd.
"""
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import glob
from scipy.interpolate import interp1d
import cvxpy as cp
from cvxopt.modeling import variable, op, max, sum
import time
import math
import plotly
import plotly.graph_objs as go

dt = 0.5
# initialSoc = 0

def loadData(filename, freq='1D'):
    """ Load and prepare data for analysis.
    Computes Lambda, COP and COE and defines the frequency in the analysis
    with freq.

    Keyword arguments:
    filename -- the file that contains the dataset in pickle format
    freq -- the frequency to be used in pd.date_range
    """
    print(filename)
    data = pd.read_pickle(filename)

    # start_date = '2019-02-01-22'
    # end_date = '2020-02-01-22'

    # mask = (data['Date'] >= start_date) & (data['Date'] < end_date)
    # data = data.loc[mask]

    # print(data)

    start_date = pd.to_datetime(data.iloc[0, 0])
    end_date = pd.to_datetime(data.iloc[-1, 0])
    
    starts = pd.date_range(start_date, end_date, freq=freq) #- pd.offsets.MonthBegin(1)


    starts = starts.to_list()
    ends = pd.date_range(start_date + pd.Timedelta('1 day'), end_date + pd.Timedelta('1 day'), freq=freq)
    ends = ends.to_list()
    # print(ends)
    data['Date'] = pd.to_datetime(data['Dates'])

    data['Lambda'] = data['Price'] 

    #print(data)

    return data, starts, ends


def runModel(data, starts, ends, solar=None, maxCapacity=10., initSOC = 5., discharge = -5., charge = 5., meta=[], const=None):
    """ Run an instance of a model (day/month/year).
    This function is to be run in parallel using Dask (look at batch.py).
    It returns the storage device (dis)charing schedule, as well as the device SoC.

    Keyword arguments:
    data -- DataFrame that is the output of the loadData function - Column names are important
    starts -- list of start dates
    ends -- list of end dates
    solar -- DataFrame that contains the Solar generated demand
    maxCapacity -- Max Capacity of the storage device
    initSOC -- Initial SoC of the storage device
    discharge -- Discharge Rate of the storage device
    charge -- Charge Rate of the storage device
    meta -- set of identifiers to be added to the schedule to identify different setups
    """

    # print(data.columns)
    schedule = pd.DataFrame()

    schedule['Date'] = data['Date']
    schedule['Price'] = data['Lambda']
    schedule['kWh'] = data['kWh']
    schedule['ID'] = [meta[0]] * len(data['Date'])
    schedule['Rated Solar (kW)'] = [meta[2]] * len(data['Date'])
    schedule['Duration'] = [meta[1]] * len(data['Date'])

    schedule['Capacity'] = [maxCapacity / meta[1]] * len(data['Date'])
    schedule['Charge'] = [charge] * len(data['Date'])
    schedule['Discharge'] = [discharge] * len(data['Date'])
    # schedule['Capacity_Breach'] = [0] * len(data['Date'])

    
    # print('hehehe')

    delta = []
    totSOC = []
    curtailed = []

    # print(starts)
    # print(ends)
    breaches = []
    for (start, end) in zip(starts, ends):

        # pick subset of the dataset according to the dates
        dat = data.loc[(data['Date'] >= start) & (data['Date'] < end)]
        sol = solar.loc[(solar['Date'] >= start) & (solar['Date'] < end)]
        sol = sol.reset_index(drop=True)['Solar'].to_numpy() * dt
        dat = dat.reset_index(drop=True)

        dem = dat['kWh'].to_numpy()

        Kindex = dat.index.values.tolist()

        # print(Kindex)
        # print(sol)
        # print(type(solar))
 

        u = cp.Variable(len(Kindex))

        constraints = []


        l = cp.Parameter(len(dem), nonneg=True)
        l.value = dem

        p = l + u

        if isinstance(solar, pd.DataFrame) or isinstance(solar, pd.Series):
            # print('hehehe')

            g = cp.Parameter(len(dem), nonneg=True)
            g.value = sol

            c = cp.Variable(len(dem), nonneg=True)
        
            p += c - g

            constraints.append(c >= 0)
            constraints.append( c <= cp.pos(g - l) )



        constraints.append( u <= charge )
        constraints.append( u >= discharge )


 
        for k in Kindex:
            constraints.append( cp.sum(u[0:k]) + initSOC <= maxCapacity)
            constraints.append( cp.sum(u[0:k] ) + initSOC >= 0)
 
        constraints.append(p >= 0)
        constraints.append(cp.sum(u) == 0)
        # constraints.append(u >= -l + g) # contraint to charge battery from solar

        if const != None:
            constraints.append( p <= const)

        lamb = dat['Lambda'].to_numpy()
        

  

        objective = cp.Minimize(cp.sum(cp.multiply(lamb, p)) )

        
        problem = cp.Problem(objective, constraints)

        problem.solve()

        breach = []
        if problem.status in ["infeasible", "unbounded"]:
            constraints = []
            constraints.append(c >= 0)
            constraints.append( c <= cp.pos(g - l) )
            constraints.append( u <= charge )
            constraints.append( u >= discharge )

            for k in Kindex:
                constraints.append( cp.sum(u[0:k]) + initSOC <= maxCapacity)
                constraints.append( cp.sum(u[0:k] ) + initSOC >= 0)
 
            constraints.append(p >= 0)
            constraints.append(cp.sum(u) == 0)

            constraints.append( p <= const * 1.5)

            objective = cp.Minimize(cp.sum(cp.multiply(lamb, p)) )

        
            problem = cp.Problem(objective, constraints)

            problem.solve()
            breach = [1] * len(dat)

        else:
            breach = [0] * len(dat)


        breaches += breach
        soc = []

        for k in Kindex:
            res =  u.value[k]
            curtailed.append(c.value[k])
            delta.append(u.value[k])
            if k == 0:
                soc.append(res + initSOC)
            else:
                soc.append(res + soc[-1])

  
        totSOC += soc
 
        initSOC = soc[-1]


    schedule['SOC'] = totSOC
    schedule['Deltas'] = delta
    schedule['Curtailed'] = curtailed
    schedule['Solar (kWh)'] = sol
    schedule['Capacity_Breach'] = breaches

    return schedule


if __name__ == "__main__":

    shop = glob.glob("./week.csv")

    print (cp.installed_solvers())

    print(shop)

    data, starts, ends = loadData(shop[0])

    print(data['Date'])

    # print(starts)

    # print(ends)

    # start_time = time.time()
    # print(starts)
    # starts = starts[:-2]
    # print(starts)
    # ends = ends[:-2]
    sched = runModel(data, starts, ends)
    print(sched)

    sched.to_csv('./result.csv', index=False)

    # print("--- %s seconds ---" % (time.time() - start_time))

    # #start_time = time.time()

    # sched = testModel(data, starts, ends)

    # #print("--- %s seconds ---" % (time.time() - start_time))

    # #sched.loc[sched['Q_dot'] != schedold['Q_dot']]

    # sched.to_csv('./cnew.csv', index=False)
    # #schedold.to_csv('./old.csv', index=False)

    


    # heat = sched.values

    # print(sched)
    # # print(heat)
    # # print(heat[0, 0])

    # createHeatmap(heat)