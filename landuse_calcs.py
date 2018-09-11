import pandas as pd

def landuse_calcs(rp_calc):
    """ Takes the input dataframe and does transformations, share calculations, to derive numbers for projections.
    """
    rp_tothh = rp_calc.groupby(['TAZ1454','TAZ'])['RP_TOTHH'].sum().groupby(level = 0).transform(lambda x: x/x.sum()).reset_index()
    rp_hhpop = rp_calc.groupby(['TAZ1454','TAZ'])['RP_HHPOP'].sum().groupby(level = 0).transform(lambda x: x/x.sum()).reset_index()
    rp_totpop = rp_calc.groupby(['TAZ1454','TAZ'])['RP_TOTPOP'].sum().groupby(level = 0).transform(lambda x: x/x.sum()).reset_index()
    rp_empres = rp_calc.groupby(['TAZ1454','TAZ'])['RP_EMPRES'].sum().groupby(level = 0).transform(lambda x: x/x.sum()).reset_index()

    rp_resacre = rp_calc.groupby(['TAZ1454','TAZ'])['RP_RESACRE'].sum().groupby(level = 0).transform(lambda x: x/x.sum()).reset_index()
    rp_ciacre = rp_calc.groupby(['TAZ1454','TAZ'])['RP_CIACRE'].sum().groupby(level = 0).transform(lambda x: x/x.sum()).reset_index()
    rp_temp = rp_calc.groupby(['TAZ1454','TAZ'])['RP_TEMP'].sum().groupby(level = 0).transform(lambda x: x/x.sum()).reset_index()

    rp_tothh = rp_tothh.rename(columns={"RP_TOTHH":"RP_TOTHH_share"})
    rp_hhpop = rp_hhpop.rename(columns={"RP_HHPOP":"RP_HHPOP_share"})
    rp_totpop = rp_totpop.rename(columns={"RP_TOTPOP":"RP_TOTPOP_share"})
    rp_empres = rp_empres.rename(columns={"RP_EMPRES":"RP_EMPRES_share"})

    rp_resacre = rp_resacre.rename(columns={"RP_RESACRE":"RP_RESACRE_share"})
    rp_ciacre = rp_ciacre.rename(columns={"RP_CIACRE":"RP_CIACRE_share"})
    rp_temp = rp_temp.rename(columns={"RP_TEMP":"RP_TEMP_share"})

    vta_final= pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(rp_calc,rp_tothh),rp_hhpop),rp_totpop),rp_empres),rp_resacre),rp_ciacre),rp_temp)
    vta_final['abag_TOTHH_dist'] = vta_final['TOTHH']*vta_final['RP_TOTHH_share']
    vta_final['abag_HHPOP_dist'] = vta_final['HHPOP']*vta_final['RP_HHPOP_share']
    vta_final['abag_TOTPOP_dist'] = vta_final['TOTPOP']*vta_final['RP_TOTPOP_share']
    vta_final['abag_EMPRES_dist'] = vta_final['EMPRES']*vta_final['RP_EMPRES_share']
    
    vta_final['abag_RESACRE_dist'] = round(vta_final['RESACRE']*vta_final['RP_RESACRE_share'])
    vta_final['abag_CIACRE_dist'] = round(vta_final['CIACRE']*vta_final['RP_CIACRE_share'])
    vta_final['abag_TEMP_dist'] = round(vta_final['TOTEMP']*vta_final['RP_TEMP_share'])

#     vta_final['abag_RETEMP_dist'] = round(vta_final['RETEMP']/vta_final['RP_TEMP']*vta_final['abag_TEMP_dist'])
    vta_final['abag_RETEMP_dist'] = round(vta_final['RETEMP']/(vta_final['RETEMP'] + vta_final['SEREMP'] + vta_final['OTHEMP'] + vta_final['AGEMP'] + vta_final['MANEMP'] + vta_final['WHOEMP'])*vta_final['abag_TEMP_dist'])
    vta_final['abag_SEREMP_dist'] = round(vta_final['SEREMP']/(vta_final['RETEMP'] + vta_final['SEREMP'] + vta_final['OTHEMP'] + vta_final['AGEMP'] + vta_final['MANEMP'] + vta_final['WHOEMP'])*vta_final['abag_TEMP_dist'])
    vta_final['abag_OTHEMP_dist'] = round(vta_final['OTHEMP']/(vta_final['RETEMP'] + vta_final['SEREMP'] + vta_final['OTHEMP'] + vta_final['AGEMP'] + vta_final['MANEMP'] + vta_final['WHOEMP'])*vta_final['abag_TEMP_dist'])
    vta_final['abag_AGEMP_dist'] = round(vta_final['AGEMP']/(vta_final['RETEMP'] + vta_final['SEREMP'] + vta_final['OTHEMP'] + vta_final['AGEMP'] + vta_final['MANEMP'] + vta_final['WHOEMP'])*vta_final['abag_TEMP_dist'])
    vta_final['abag_MANEMP_dist'] = round(vta_final['MANEMP']/(vta_final['RETEMP'] + vta_final['SEREMP'] + vta_final['OTHEMP'] + vta_final['AGEMP'] + vta_final['MANEMP'] + vta_final['WHOEMP'])*vta_final['abag_TEMP_dist'])
    vta_final['abag_WHOEMP_dist'] = round(vta_final['WHOEMP']/(vta_final['RETEMP'] + vta_final['SEREMP'] + vta_final['OTHEMP'] + vta_final['AGEMP'] + vta_final['MANEMP'] + vta_final['WHOEMP'])*vta_final['abag_TEMP_dist'])

    bad_criteria = "abag_TOTHH_dist > 0 & abag_HHPOP_dist == 0 & TAZ <= 1490"
    vta_final.loc[vta_final.eval(bad_criteria), 'cleaning_rule_5'] = 1
    city_hhpop_tothh_average = (vta_final.groupby(["CITY"])['abag_HHPOP_dist'].sum() / vta_final.groupby(["CITY"])['abag_TOTHH_dist'].sum()).reset_index().rename(columns={0:'HHPOP/TOTHH'})
    vta_final = pd.merge(vta_final, city_hhpop_tothh_average, how = 'left')
    vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTPOP_dist'] = vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTHH_dist']*vta_final.loc[vta_final.eval(bad_criteria), 'HHPOP/TOTHH']
    vta_final.loc[vta_final.eval(bad_criteria), 'abag_HHPOP_dist'] = vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTHH_dist']*vta_final.loc[vta_final.eval(bad_criteria), 'HHPOP/TOTHH']

    bad_criteria = "abag_TOTHH_dist > 0 & abag_HHPOP_dist == 0 & TAZ > 1490"
    vta_final.loc[vta_final.eval(bad_criteria), 'cleaning_rule_6'] = 1    
    sdist_hhpop_tothh_average = (vta_final.groupby(["SDIST"])['abag_HHPOP_dist'].sum() / vta_final.groupby(["SDIST"])['abag_TOTHH_dist'].sum()).reset_index().rename(columns={0:'SD_HHPOP/TOTHH'})
    vta_final = pd.merge(vta_final, sdist_hhpop_tothh_average, how = 'left')
    vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTPOP_dist'] = vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTHH_dist']*vta_final.loc[vta_final.eval(bad_criteria), 'SD_HHPOP/TOTHH']
    vta_final.loc[vta_final.eval(bad_criteria), 'abag_HHPOP_dist'] = vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTHH_dist']*vta_final.loc[vta_final.eval(bad_criteria), 'SD_HHPOP/TOTHH']    

    #Make HH
    vta_final['abag_HH1_dist'] =round(vta_final['HH1']/(vta_final['HH1'] + vta_final['HH2'] + vta_final['HH3'] + vta_final['HH4'])*vta_final['abag_TOTHH_dist'])
    vta_final['abag_HH2_dist'] =round(vta_final['HH2']/(vta_final['HH1'] + vta_final['HH2'] + vta_final['HH3'] + vta_final['HH4'])*vta_final['abag_TOTHH_dist'])
    vta_final['abag_HH3_dist'] =round(vta_final['HH3']/(vta_final['HH1'] + vta_final['HH2'] + vta_final['HH3'] + vta_final['HH4'])*vta_final['abag_TOTHH_dist'])
    vta_final['abag_HH4_dist'] =round(vta_final['HH4']/(vta_final['HH1'] + vta_final['HH2'] + vta_final['HH3'] + vta_final['HH4'])*vta_final['abag_TOTHH_dist'])
    
    vta_final['abag_AGE0004_dist'] = round(vta_final['RP_AGE0004']/(vta_final['RP_AGE0004'] + vta_final['RP_AGE0519'] + vta_final['RP_AGE2044'] + vta_final['RP_AGE4564'] + vta_final['RP_AGE65'])*vta_final['abag_TOTPOP_dist'])
    vta_final['abag_AGE0519_dist'] = round(vta_final['RP_AGE0519']/(vta_final['RP_AGE0004'] + vta_final['RP_AGE0519'] + vta_final['RP_AGE2044'] + vta_final['RP_AGE4564'] + vta_final['RP_AGE65'])*vta_final['abag_TOTPOP_dist'])
    vta_final['abag_AGE2044_dist'] = round(vta_final['RP_AGE2044']/(vta_final['RP_AGE0004'] + vta_final['RP_AGE0519'] + vta_final['RP_AGE2044'] + vta_final['RP_AGE4564'] + vta_final['RP_AGE65'])*vta_final['abag_TOTPOP_dist'])
    vta_final['abag_AGE4564_dist'] = round(vta_final['RP_AGE4564']/(vta_final['RP_AGE0004'] + vta_final['RP_AGE0519'] + vta_final['RP_AGE2044'] + vta_final['RP_AGE4564'] + vta_final['RP_AGE65'])*vta_final['abag_TOTPOP_dist'])
    vta_final['abag_AGE65_dist'] = round(vta_final['RP_AGE65']/(vta_final['RP_AGE0004'] + vta_final['RP_AGE0519'] + vta_final['RP_AGE2044'] + vta_final['RP_AGE4564'] + vta_final['RP_AGE65'])*vta_final['abag_TOTPOP_dist'])

    vta_final['abag_AGE0513_dist'] = round(vta_final['AGE0513_percent']*vta_final['abag_TOTPOP_dist'])
    vta_final['abag_AGE1417_dist'] = round(vta_final['AGE1417_percent']*vta_final['abag_TOTPOP_dist'])
    vta_final['abag_AGE1824_dist'] = round(vta_final['AGE1824_percent']*vta_final['abag_TOTPOP_dist'])
    
    vta_final['abag_SFHH_dist'] = round(vta_final['SFHH']/(vta_final['SFHH'] + vta_final['MFHH'])*vta_final['abag_TOTHH_dist'])
    vta_final['abag_MFHH_dist'] = round(vta_final['MFHH']/(vta_final['SFHH'] + vta_final['MFHH'])*vta_final['abag_TOTHH_dist'])

    bad_criteria = "abag_HHPOP_dist > abag_TOTPOP_dist"
    vta_final.loc[vta_final.eval(bad_criteria), 'cleaning_rule_7'] = 1
    vta_final.loc[vta_final.eval(bad_criteria), 'abag_HHPOP_dist'] = vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTPOP_dist']

    bad_criteria = "TAZ <= 1490 & (abag_EMPRES_dist > abag_HHPOP_dist | abag_TOTHH_dist >= 50 & abag_EMPRES_dist == 0)"
    vta_final.loc[vta_final.eval(bad_criteria), 'cleaning_rule_8'] = 1
    city_empres_tothh_average = (vta_final.groupby(["CITY"])['abag_EMPRES_dist'].sum() / vta_final.groupby(["CITY"])['abag_TOTHH_dist'].sum()).reset_index().rename(columns={0:'EMPRES/TOTHH'})
    vta_final = pd.merge(vta_final, city_empres_tothh_average, how = 'left')
    vta_final.loc[vta_final.eval(bad_criteria), 'abag_EMPRES_dist'] = vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTHH_dist']*vta_final.loc[vta_final.eval(bad_criteria), 'EMPRES/TOTHH']
    
    bad_criteria = "TAZ > 1490 & (abag_EMPRES_dist > abag_HHPOP_dist | abag_TOTHH_dist >= 50 & abag_EMPRES_dist == 0)"
    vta_final.loc[vta_final.eval(bad_criteria), 'cleaning_rule_8'] = 1
    sdist_empres_tothh_average = (vta_final.groupby(["SDIST"])['abag_EMPRES_dist'].sum() / vta_final.groupby(["SDIST"])['abag_TOTHH_dist'].sum()).reset_index().rename(columns={0:'SD_EMPRES/TOTHH'})
    vta_final = pd.merge(vta_final, sdist_empres_tothh_average, how = 'left')
    vta_final.loc[vta_final.eval(bad_criteria), 'abag_EMPRES_dist'] = vta_final.loc[vta_final.eval(bad_criteria), 'abag_TOTHH_dist']*vta_final.loc[vta_final.eval(bad_criteria), 'SD_EMPRES/TOTHH']
    
    vta_final = vta_final.round({'abag_TOTHH_dist': 0, 'abag_HHPOP_dist': 0, 'abag_TOTPOP_dist': 0, 'abag_EMPRES_dist': 0})
    
    return vta_final