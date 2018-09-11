import pandas as pd

def employment_cleaning(rp_calc):
    sdist_totemp_sum = (rp_calc.groupby('SDIST')['RETEMP'].sum() + rp_calc.groupby('SDIST')['SEREMP'].sum() + rp_calc.groupby('SDIST')['OTHEMP'].sum() + rp_calc.groupby('SDIST')['AGEMP'].sum() + rp_calc.groupby('SDIST')['MANEMP'].sum() + rp_calc.groupby('SDIST')['WHOEMP'].sum())
    a = (rp_calc.groupby('SDIST')['RETEMP'].sum()/sdist_totemp_sum).reset_index().rename(columns={0:'RETEMP_SDIST_AVG'})
    b = (rp_calc.groupby('SDIST')['SEREMP'].sum()/sdist_totemp_sum).reset_index().rename(columns={0:'SEREMP_SDIST_AVG'})
    c = (rp_calc.groupby('SDIST')['OTHEMP'].sum()/sdist_totemp_sum).reset_index().rename(columns={0:'OTHEMP_SDIST_AVG'})
    d = (rp_calc.groupby('SDIST')['AGEMP'].sum()/sdist_totemp_sum).reset_index().rename(columns={0:'AGEMP_SDIST_AVG'})
    e = (rp_calc.groupby('SDIST')['MANEMP'].sum()/sdist_totemp_sum).reset_index().rename(columns={0:'MANEMP_SDIST_AVG'})
    f = (rp_calc.groupby('SDIST')['WHOEMP'].sum()/sdist_totemp_sum).reset_index().rename(columns={0:'WHOEMP_SDIST_AVG'})
    dfs = [a,b,c,d,e,f]
    from functools import reduce
    EMP_AVG = reduce(lambda left,right: pd.merge(left,right), dfs)
    rp_calc = pd.merge(rp_calc,EMP_AVG, how = 'left')    
    bad_criteria = 'RP_TEMP >=5 & RETEMP + SEREMP + OTHEMP + AGEMP + MANEMP + WHOEMP == 0'
    bad_criteria_spots = rp_calc.eval(bad_criteria)
    rp_calc.loc[bad_criteria_spots,'RETEMP'] = rp_calc.loc[bad_criteria_spots,'RETEMP_SDIST_AVG']
    rp_calc.loc[bad_criteria_spots,'SEREMP'] = rp_calc.loc[bad_criteria_spots,'SEREMP_SDIST_AVG']
    rp_calc.loc[bad_criteria_spots,'OTHEMP'] = rp_calc.loc[bad_criteria_spots,'OTHEMP_SDIST_AVG']
    rp_calc.loc[bad_criteria_spots,'RETEMP'] = rp_calc.loc[bad_criteria_spots,'RETEMP_SDIST_AVG']
    rp_calc.loc[bad_criteria_spots,'RETEMP'] = rp_calc.loc[bad_criteria_spots,'RETEMP_SDIST_AVG']
    rp_calc.loc[bad_criteria_spots,'RETEMP'] = rp_calc.loc[bad_criteria_spots,'RETEMP_SDIST_AVG']
    return rp_calc