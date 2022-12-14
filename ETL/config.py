#Inputs
census = "CensusFlatFile2022.csv"
census_dd = "FFIEC_Census_File_Definitions_26AUG22.xlsx"
lar = "2021_public_lar_pa.csv"

#Outputs
outfile = 'census_lar_merge.csv'

#Values for Use
state_fips_code=42

#Key Files / Storage Accounts
STORAGEACCOUNTURL= "https://bdousncvbaddmv01sta.blob.core.windows.net"
STORAGEACCOUNTKEY= "JhbiDWfvxlthZngR4WkZDaaxhE/QMWj8bIXvdNH8MTvHYxNoSRq88UifxDawqeMlVpYuR3vpokQi52Ek1Ccqsw=="
CONTAINERNAME= "newcontainer"

#Dictionary and Column Definition
cols_census = [
        'census_tract_merge', 
        'MSA/MD median family income', 
        'MSA/MD median household income',
        'Tract median family income as a percentage of the MSA/MD median family income. 2 decimal places, truncated.', 
        'FFIEC Estimated MSA/MD median family income',
        'Income indicator, which identifies low, moderate, middle, and upper income areas',
        '''CRA poverty criteria. 'X' - Yes , ' ' (blank space) - No''',
        '''CRA unemployment criteria. 'X' - Yes , ' ' (blank space) - No''',
        'Minority population as percent of tract population rounded to two decimal places',
        'Total population White', 
        'Total population Black/African American', 
        'Total population American Indian or Alaska native', 
        'Total population Asian', 
        'Total population Native Hawaiian or other Pacific Islander', 
        'Total population some other race', 
        'Total housing units', 
        'Total housing units - urban   ', 
        'Total housing units - rural', 
        'Total occupied housing units', 
        'Total vacant housing units'
        ]

cols_census_dict = {
        'census_tract_merge': 'census_tract_merge', 
        'MSA/MD median family income': 'msa_med_family_income', 
        'MSA/MD median household income': 'msa_med_household_income',
        'Tract median family income as a percentage of the MSA/MD median family income. 2 decimal places, truncated.': 'pct_tract_msa_med_fam_income', 
        'FFIEC Estimated MSA/MD median family income': 'ffiec_est_msa_med_fam_income',
        'Income indicator, which identifies low, moderate, middle, and upper income areas': 'income_category',
        '''CRA poverty criteria. 'X' - Yes , ' ' (blank space) - No''': 'cra_poverty',
        '''CRA unemployment criteria. 'X' - Yes , ' ' (blank space) - No''': 'cra_unemployment',
        'Minority population as percent of tract population rounded to two decimal places': 'minority_pct_of_tract',
        'Total population White': 'total_pop_white', 
        'Total population Black/African American': 'total_pop_black', 
        'Total population American Indian or Alaska native': 'total_pop_aioan', 
        'Total population Asian': 'total_pop_asian', 
        'Total population Native Hawaiian or other Pacific Islander': 'total_pop_nhopi', 
        'Total population some other race': 'total_pop_other', 
        'Total housing units': 'total_hus', 
        'Total housing units - urban   ': 'total_hus_urban', 
        'Total housing units - rural': 'total_hus_rural', 
        'Total occupied housing units': 'total_hus_occupied', 
        'Total vacant housing units': 'total_hus_vacant'
}

cols_hmda = [
        'census_tract_merge', 
        'lei',
        'derived_msa_md',
        'conforming_loan_limit',
        'derived_loan_product_type',
        'derived_dwelling_category',
        'derived_ethnicity',
        'derived_race',
        'derived_sex',
        'action_taken',
        'purchaser_type',
        'preapproval',
        'loan_type',
        'loan_purpose',
        'lien_status',
        'open_end_line_of_credit',
        'business_or_commercial_purpose',
        'loan_amount',
        'combined_loan_to_value_ratio',
        'interest_rate',
        'rate_spread',
        'hoepa_status',
        'total_loan_costs',
        'property_value',
        'income',
        'debt_to_income_ratio',
        'applicant_credit_score_type',
        'applicant_age'
]