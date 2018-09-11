import pandas as pd

# RP stands for Regional Partner
def prep_data(abag, rp_taz, geom_given = True):
    """ Joins abag to the regional seed file data
    """
    
    cols = ['TAZ','TAZ1454','DIST','SDIST','COUNTY','TOTHH','TOTPOP',
                                    'HHPOP','EMPRES','HH1','HH2','HH3','HH4','TACRES','RESACRE','CIACRE','TEMP',
                                    'RETEMP','SEREMP','OTHEMP','AGEMP','MANEMP','WHOEMP','AGE0004','AGE0519',
                                    'AGE2044','AGE4564','AGE65','AGE0513','AGE1417','AGE1824','SFHH','MFHH', 'CITY']
    cols_to_retain = ['INC1','INC2','INC3','INC4','MHHINC', 'ESENR', 'HSENR', 'COLLENR', 'COLLENRF', 'COLLENRP']

    cols.extend(cols_to_retain)

    if geom_given:
        cols.append('geometry')
    else:
        pass
    
    
    rp_taz = rp_taz[cols].rename(columns={"TOTHH":"RP_TOTHH","TOTPOP":"RP_TOTPOP",
                                "HHPOP":"RP_HHPOP","EMPRES":"RP_EMPRES","RESACRE":'RP_RESACRE',"CIACRE":"RP_CIACRE","TEMP":"RP_TEMP",
                                         "AGE0004":"RP_AGE0004","AGE0519":"RP_AGE0519","AGE2044":"RP_AGE2044","AGE4564":"RP_AGE4564","AGE65":"RP_AGE65"})
    #Clear out Student calc numbers where TOTPOP is 0.
    criteria = "RP_TOTPOP==0&(AGE0513>0|AGE1417>0|AGE1824>0)"
    rp_taz.loc[rp_taz.eval(criteria), 'cleaning_rule_1'] = 1
    rp_taz.loc[rp_taz.eval(criteria), 'AGE0513'] = 0
    rp_taz.loc[rp_taz.eval(criteria), 'AGE1417'] = 0
    rp_taz.loc[rp_taz.eval(criteria), 'AGE1824'] = 0
    
    #Calculation district wide average number of students to population ratio
    okay_criteria = "RP_TOTPOP>AGE0513+AGE1417+AGE1824"
    AGE0513_avg_factor =  (rp_taz.query(okay_criteria).groupby('DIST')['AGE0513'].sum()/rp_taz.query(okay_criteria).groupby('DIST')['RP_TOTPOP'].sum()).reset_index().rename(columns={0:'AGE0513_factor'})
    AGE1417_avg_factor =  (rp_taz.query(okay_criteria).groupby('DIST')['AGE1417'].sum()/rp_taz.query(okay_criteria).groupby('DIST')['RP_TOTPOP'].sum()).reset_index().rename(columns={0:'AGE1417_factor'})    
    AGE1824_avg_factor =  (rp_taz.query(okay_criteria).groupby('DIST')['AGE1824'].sum()/rp_taz.query(okay_criteria).groupby('DIST')['RP_TOTPOP'].sum()).reset_index().rename(columns={0:'AGE1824_factor'})
    rp_taz = pd.merge(pd.merge(pd.merge(rp_taz, AGE0513_avg_factor),AGE1417_avg_factor),AGE1824_avg_factor)
    
    
    # Save percentages for later calc
    rp_taz['AGE0513_percent'] = rp_taz['AGE0513']/rp_taz['RP_TOTPOP']
    rp_taz['AGE1417_percent'] = rp_taz['AGE1417']/rp_taz['RP_TOTPOP']
    rp_taz['AGE1824_percent'] = rp_taz['AGE1824']/rp_taz['RP_TOTPOP']

    #If the Total Population for the zone is below the student population, overwrite percentages with district factors.
    bad_criteria = "RP_TOTPOP<AGE0513+AGE1417+AGE1824"
    rp_taz.loc[rp_taz.eval(bad_criteria), 'cleaning_rule_2'] = 1
    rp_taz.loc[rp_taz.eval(bad_criteria), 'AGE0513_percent'] = rp_taz.loc[rp_taz.eval(bad_criteria), 'AGE0513_factor']
    rp_taz.loc[rp_taz.eval(bad_criteria), 'AGE1417_percent'] = rp_taz.loc[rp_taz.eval(bad_criteria), 'AGE1417_factor']
    rp_taz.loc[rp_taz.eval(bad_criteria), 'AGE1824_percent'] = rp_taz.loc[rp_taz.eval(bad_criteria), 'AGE1824_factor']    
    
    #If the total population is greater than ten but the student population is 0, replace student percent with district average.
    age_0513_bad_criteria = "RP_TOTPOP>=10 & AGE0513==0"
    rp_taz.loc[rp_taz.eval(age_0513_bad_criteria), 'cleaning_rule_3'] = 1
    rp_taz.loc[rp_taz.eval(age_0513_bad_criteria), 'AGE0513_percent'] = rp_taz.loc[rp_taz.eval(age_0513_bad_criteria), 'AGE0513_factor']
    age_1417_bad_criteria = "RP_TOTPOP>=10 & AGE1417==0"
    rp_taz.loc[rp_taz.eval(age_1417_bad_criteria), 'cleaning_rule_3'] = 1
    rp_taz.loc[rp_taz.eval(age_1417_bad_criteria), 'AGE1417_percent'] = rp_taz.loc[rp_taz.eval(age_1417_bad_criteria), 'AGE1417_factor']
    age_1824_bad_criteria = "RP_TOTPOP>=10 & AGE1824==0"
    rp_taz.loc[rp_taz.eval(age_1824_bad_criteria), 'cleaning_rule_3'] = 1
    rp_taz.loc[rp_taz.eval(age_1824_bad_criteria), 'AGE1824_percent'] = rp_taz.loc[rp_taz.eval(age_1824_bad_criteria), 'AGE1824_factor']    
    
    #Join the RP shapefile to the abag 2010 dataset!  Pull this source input data directly from ABAG
    rp_calc = pd.merge(abag[['TAZ1454','TOTHH','HHPOP','TOTPOP','EMPRES','HHINCQ1','HHINCQ2','HHINCQ3','HHINCQ4','RESACRE','CIACRE','TOTEMP','SHPOP62P','AGE0004','AGE0519',
                                    'AGE2044','AGE4564','AGE65P']].rename(columns={"SHPOP62P":"Z2SHARE"}),
                                rp_taz)

    return rp_calc