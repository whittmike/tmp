# =============================================================================
# import libraries
# =============================================================================
# --- lib imports
import numpy as np
import pandas as pd
import re
import json
import openai
import ast
from dotenv import dotenv_values

# --- system imports
import sys
import os
import time

# =============================================================================
# random functions we are gonna use
# =============================================================================
# --- im picky about column names
def name_repair(StrInput):
    '''
    Parameters
    ----------
    StrInput : TYPE string
        DESCRIPTION. takes a string with capital letters and imposes underscores and then lowers it

    Returns
    -------
    str_out : TYPE string
        DESCRIPTION. the new string we want
    '''
    str_out = re.sub( '(?<!^)(?=[A-Z])', '_', StrInput).lower()
    return str_out

# --- function to grab labor data
def get_labor_data(LaborInputLst, JobId):
    '''
    Parameters
    ----------
    LaborInputLst : TYPE list of dictionaries
        DESCRIPTION. the labor data
    JobId : TYPE int
        DESCRIPTION. the id pointing back to the job

    Returns
    -------
    labor_df : TYPE DataFrame
        DESCRIPTION.
    '''
    # --  easy parse to df since it doesnt have any children
    labor_df = pd.DataFrame(LaborInputLst)
    # -- lets make those columns pretty
    labor_df.columns = [name_repair(i) for i in labor_df.columns]
    # -- declare that job id so we can link back to job
    labor_df['job_id'] = [JobId for i in labor_df['id']]
    # -- return the labor df
    return labor_df

# --- function to grab parts data
def get_parts_data(PartsInputLst, JobId):
    '''
    Parameters
    ----------
    LaborInputLst : TYPE list of dictionaries
        DESCRIPTION. the parts data
    JobId : TYPE int
        DESCRIPTION. the id pointing back to the job

    Returns
    -------
    parts_df : TYPE DataFrame
        DESCRIPTION.
    '''
    # -- these are the columns for parts that we want
    parts_columns = ['id', 'quantity', 'brand', 'name', 'partNumber']
    # -- easy parse to df since it doesnt have any children
    parts_df = pd.DataFrame(PartsInputLst)[parts_columns]
    # -- lets make those columns pretty
    parts_df.columns =  [name_repair(i) for i in parts_df.columns]
    # -- declare that job id
    parts_df['job_id'] = [JobId for i in parts_df['id']]
    # -- return parts df
    return parts_df

# --- function to parse out job labor and parts data
def get_job_labor_parts_data(JobInput, RepairOrderId):
    '''
    Parameters
    ----------
    JobInput : TYPE dictionary 
        DESCRIPTION. taken directly from repair order dictionary as list and then passed through as a loop
    RepairOrderId : TYPE int
        DESCRIPTION. links back to repair_order_df

    Returns
    -------
    job_df : TYPE Dataframe
        DESCRIPTION.
    labor_df : TYPE Dataframe
        DESCRIPTION.
    parts_df : TYPE Dataframe
        DESCRIPTION.
    '''
    # -- our dictionary for main job content
    job_dct = {
        'id':[None],
        'repair_order_id':[RepairOrderId],
        'vehicle_id':[None],
        'customer_id':[None],
        'name':[None],
        'note':[None],
        'job_category_name':[None],
        'created_date':[None],
        'completed_date':[None],
        'updated_date':[None]
    }
    try:
        # -- list of what we want from jobs
        for i in ['id', 'repairOrderId', 'vehicleId', 'customerId', 'name', 'note', 'jobCategoryName', 'createdDate', 'completedDate', 'updatedDate']:
            job_dct[name_repair(i)] = [JobInput[i]]
        # -- set to dataframe
        job_df = pd.DataFrame(job_dct)
        # -- set job id
        input_job_id = JobInput['id']
    except:
        print(f'no job data for RepairOrderId {RepairOrderId}')
        input_job_id = 777
    # -- lets see if theres labor data
    labor_df = pd.DataFrame(
        {
            'id':[None],
            'name':[None],
            'rate':[None],
            'hours':[None],
            'complete':[None],
            'technician_id':[None],
            'job_id':[JobInput['id']]
        }
    )
    try: 
        labor_df = get_labor_data(JobInput['labor'], JobInput['id'])
    except:
        print(f'no labor data for JobId {input_job_id}')
    # -- lets see if theres parts data
    parts_df = pd.DataFrame(
        {
            'id':[None],
            'quantity':[None],
            'brand':[None],
            'name':[None],
            'part_number':[None],
            'job_id':[JobInput['id']]
        }
    )
    # -- try to get parts data
    try: 
         parts_df = get_parts_data(JobInput['parts'], JobInput['id'])
    except:
         print(f'no parts data for JobId {input_job_id}')
    # -- now lets return what we need
    return job_df, labor_df, parts_df

# =============================================================================
# grab that json data
# =============================================================================
# --- set working directory - wherever the json data is
#os.chdir('C:/Users/mwhittlesey/Desktop/TEK_METRIC')

# --- load dictionary with credentials
cred_dict = dotenv_values('.env')

# --- read json file
with open('./tekmetric_sample.json', 'r') as file:
    # Load the JSON data into a Python dictionary
    tek_metric_01 = json.load(file)

# --- we only want the content
# --- im assuming that for each 'transaction' this is the core of the data we are looking at
ContentInput = tek_metric_01['content'][0]

# =============================================================================
# ContentInput is hypothetically what we would want to loop through repair order by repair order
# =============================================================================
# --- this is our primary process it references other functions
def get_schema(ContentInput):
    '''
    Parameters
    ----------
    ContentInput : TYPE dict 
        DESCRIPTION. this is the raw json

    Returns
    -------
    repair_order_df : TYPE DataFrame
        DESCRIPTION.
    job_out : TYPE DataFrame
        DESCRIPTION.
    labor_out : TYPE DataFrame
        DESCRIPTION.
    parts_out : TYPE DataFrame
        DESCRIPTION.
    '''
    # --- put the main content in a dictionary
    repair_order_dct = {}
    # --- this is what we want from main content
    for i in ['id', 'repairOrderNumber', 'shopId', 'vehicleId', 'customerId', 'milesIn', 'milesOut', 'completedDate', 'postedDate']:
        # -- we are placing the dictionary value in a 'list' because thats what allows us to change it to a dataframe
        repair_order_dct[name_repair(i)] = [ContentInput[i]] 
    # --- lets put that dictionary into a df
    repair_order_df = pd.DataFrame(repair_order_dct)
    # --- lists to append to
    job_df_lst = []
    labor_df_lst = []
    parts_df_lst = []
    # --- lets loop through the jobs within the content
    for i in ContentInput['jobs']:
        jobs_i, labor_i, parts_i = get_job_labor_parts_data(i, i['id'])
        job_df_lst.append(jobs_i)
        labor_df_lst.append(labor_i)
        parts_df_lst.append(parts_i)
    # --- heres all the data from the repair
    job_out = pd.concat(job_df_lst)
    labor_out = pd.concat(labor_df_lst)
    parts_out = pd.concat(parts_df_lst)
    # --- return it all
    return repair_order_df, job_out, labor_out, parts_out

# --- lists to append to
repair_order_lst = []
job_lst = []
labor_lst = []
parts_lst = []

# --- loop through content
for i in tek_metric_01['content']:
    repair, job, labor, parts = get_schema(i)
    repair_order_lst.append(repair)
    job_lst.append(job)
    labor_lst.append(labor)
    parts_lst.append(parts)

# --- function for removing useless rows
def remove_none_id(InputDf):
    df_out = InputDf[InputDf['id'] != None]
    return df_out

# --- final df
RepairOrderDF = remove_none_id(pd.concat(repair_order_lst))
JobsDF = remove_none_id(pd.concat(job_lst))
LaborDF = remove_none_id(pd.concat(labor_lst))
PartsDF = remove_none_id(pd.concat(parts_lst))

# --- merge repair order id
job_df_join = JobsDF[['repair_order_id', 'id']]
job_df_join.columns = ['repair_order_id', 'job_id']
LaborDF = LaborDF.merge(job_df_join, on='job_id')
PartsDF = PartsDF.merge(job_df_join, on='job_id')

# --- get rid of rows we dont need
print(RepairOrderDF)
print(JobsDF)
print(LaborDF)
print(PartsDF)

# --- dictionary of df
output_dct = {
    'repair_orders': RepairOrderDF,
    'jobs': JobsDF,
    'labor': LaborDF,
    'parts': PartsDF
}

# --- fix string cols
for i in output_dct:
    output_dct[i] = output_dct[i].reset_index(drop=True)
    col_fix_list = [i for i in output_dct[i].columns if i in ['note','name','job_category_name']]
    if col_fix_list:
        for ii in col_fix_list:
            output_dct[i][ii].fillna('', inplace = True)

# --- remove what we dont need
del i, ii, file, col_fix_list, job, job_df_join, job_lst, labor, labor_lst, output_dct, parts, parts_lst, repair, repair_order_lst, tek_metric_01

# =============================================================================
# random functions for makin soup
# =============================================================================
# --- gets soup for job description
def get_job_description(str_1, str_2):
    job_desc_list = list(set([i for i in [str_1, str_2] if i != '' and isinstance(i, str)]))
    if job_desc_list:
        str_out = '\n'.join(job_desc_list)
        str_out = 'Job Description:\n'+str_out
    else:
        str_out = ''
    return str_out

# --- gets soup for job description
def get_job_category(str_1):
    if str_1 != ''  and isinstance(str_1, str):
        str_out = str_1
        str_out = 'Job Category:\n'+str_out
    else:
        str_out = ''
    return str_out

# --- function to parse out labor and parts soup
def parse_parts_labor_soup(SoupInputList, HeaderInput):
    tmp_lst = [i for i in list(set(SoupInputList)) if i != '' and isinstance(i, str)]
    if tmp_lst:
        str_out = '\n'.join(tmp_lst)
        str_out = HeaderInput+'\n'+str_out
    else:
        str_out = ''
    return str_out

# =============================================================================
# modify data
# =============================================================================
# --- redefine parts and labor data
PartsSoupDF = PartsDF.groupby(['job_id', 'repair_order_id'])['name'].apply(list).reset_index(name='soup_list')
LaborSoupDF = LaborDF.groupby(['job_id', 'repair_order_id'])['name'].apply(list).reset_index(name='soup_list')

# --- get parts and labor soup by job
LaborSoupDF['labor_soup'] = LaborSoupDF.apply(lambda x: parse_parts_labor_soup(x['soup_list'], 'Labor:'), axis = 1)
PartsSoupDF['parts_soup'] = PartsSoupDF.apply(lambda x: parse_parts_labor_soup(x['soup_list'], 'Parts Used:'), axis = 1)

# --- df for job soup
JobsSoupDF = JobsDF.copy()[['id', 'repair_order_id', 'name', 'note', 'job_category_name']].reset_index(drop = True)
JobsSoupDF.columns = ['job_id', 'repair_order_id', 'name', 'note', 'job_category_name']
JobsSoupDF['job_description_soup'] = JobsSoupDF.apply(lambda x: get_job_description(x['name'], x['note']), axis = 1)
JobsSoupDF['job_category_soup'] = JobsSoupDF.apply(lambda x: get_job_category(x['job_category_name']), axis = 1)

# =============================================================================
# functions for preparing ai input
# =============================================================================
# --- list of possible outputs
output_list = [
    'Engine Coolant',
    'Inspection',
    'Battery',
    'Secondary Coolant System',
    'Power Steering Fluid',
    'Brake Fluid',
    'Serpentine Belt',
    'Timing Belt',
    'Air Filter',
    'Cabin Filter',
    'Shocks',
    'Struts',
    'Tire Rotation',
    'Alignment',
    'Wipers',
    'Differential Fluid',
    'Transfer Case Fluid',
    'Transmission Fluid',
    'Engine Oil',
    'PCV Valve',
    'Spark Plugs',
    'Fuel Filter Gas',
    'Induction Clean GDI',
    'Induction Clean Port Fuel'
]

# --- function takes job id and prepares the data for ai
def dct_prepare_for_ai(JobIdInput, LaborSoupDfInput, PartsSoupDfInput, JobsSoupDfInput, OutputPossibleList):
    # --- parsed soup dct
    tmp_soup_dct = {
        'Job Description':JobsSoupDfInput[JobsSoupDfInput['job_id'] == JobIdInput]['job_description_soup'].iloc()[0],
        'Job Category':JobsSoupDfInput[JobsSoupDfInput['job_id'] == JobIdInput]['job_category_soup'].iloc()[0],
        'Labor':LaborSoupDfInput[LaborSoupDfInput['job_id'] == JobIdInput]['labor_soup'].iloc()[0],
        'Parts Used':PartsSoupDfInput[PartsSoupDfInput['job_id'] == JobIdInput]['parts_soup'].iloc()[0]
    }
    
    # --- get labor and parts soup
    default_soup_dct = {
        'Job Description': 'Job Description:\nThe transmission fluid is dark - Recommend a transmission fluid exchange',
        'Job Category': 'Job Category:\nFluid Service',
        'Labor': 'Labor:\nPerform Modern Premium Transmission Fluid Exchange',
        'Parts Used': 'Parts Used:\nTransmission Cleaner Solution and Additive Kit\nSynthetic Automatic Transmission Fluid'
    }
    
    # --- what we are going to input
    soup_lst = []
    job_ex_lst = []
    
    # --- loop through
    for i in tmp_soup_dct:
        if tmp_soup_dct[i] != '' and str(tmp_soup_dct[i]) != 'nan':
            job_ex_lst.append(default_soup_dct[i])
            soup_lst.append(tmp_soup_dct[i])
    
    # --- set example and input string
    ex_str_system = '\n'.join(job_ex_lst)
    ex_str_user = '\n'.join(soup_lst)
    
    # --- setup prompt
    tmp_system_input = f'''
    You are an automotive service adviser that reads past services and can 'normalize' the services to a set of standard service names.
    
    You will return a comma separated list of jobs from the following choices
    
    {OutputPossibleList}
    
    Here is an example input:
    
    {ex_str_system}
    
    And the expected result:
    ['Transmission Fluid']'''
    
    # --- setup dictionary output
    dct_out = {
        'job_id' : JobIdInput,
        'system_input' : tmp_system_input,
        'user_input' : ex_str_user,
        'response' : ''
    }
    
    # --- return final dct
    return dct_out

# =============================================================================
# this is where we start
# =============================================================================
# --- sample input repair order
RepairOrderInput = RepairOrderDF['id'].iloc()[0]

# --- get list of job_id(s)
job_id_lst = list(JobsDF[JobsDF['repair_order_id'] == RepairOrderInput]['id'])

# --- list to append to
job_dct_lst = []

# --- loop through and append to list
for job_id in job_id_lst:
    try:
        job_dct_lst.append(dct_prepare_for_ai(job_id, LaborSoupDF, PartsSoupDF, JobsSoupDF, output_list))
    except:
        print(f'couldnt parse job_id {job_id}')

# =============================================================================
# get ai response
# =============================================================================
# --- function to get data from ai
def get_ai_response(DctInput):
    if len(DctInput['user_input']) > 14:
        # -- define api key
        openai.api_key = cred_dict['GPT_KEY']
         # -- api response product
        response_content = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": DctInput['system_input']
                },
                {
                    "role": "user",
                    "content": DctInput['user_input']
                }
            ],
            temperature=0.14,
            max_tokens=256,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        # -- get content product
        product_content = response_content['choices'][0]['message']['content']
        DctInput['response'] = product_content
    else:
        DctInput['response'] = '''['insufficient data']'''
    return DctInput

# --- final list to append to
final_job_dct_lst = []

# --- loop through
for dct in job_dct_lst:
    try:
        final_job_dct_lst.append(get_ai_response(dct))
    except:
        dct['response'] = '''['unable to get response']'''
        final_job_dct_lst.append(dct)

# --- final response df
RawResponseDF = pd.DataFrame(final_job_dct_lst)
ParsedResponseDF = RawResponseDF.copy()[['job_id', 'response']]

# --- parse to list
ParsedResponseDF['service_response'] = ParsedResponseDF['response'].apply(lambda x: ast.literal_eval(x))
ParsedResponseDF = ParsedResponseDF[['job_id', 'service_response']].explode('service_response').reset_index(drop = True)

# --- for joining
job_df_join = JobsDF[['repair_order_id', 'id']]
job_df_join.columns = ['repair_order_id', 'job_id']
ParsedResponseDF = ParsedResponseDF.merge(job_df_join, on = ['job_id'])[['repair_order_id', 'service_response']].drop_duplicates()

# --- join final data
ParsedResponseDF = ParsedResponseDF.merge(
    RepairOrderDF[['id', 'completed_date', 'miles_in', 'miles_out', 'customer_id', 'vehicle_id']],
    left_on = 'repair_order_id', 
    right_on = 'id'
)[['repair_order_id', 'completed_date', 'miles_in', 'miles_out', 'customer_id', 'vehicle_id', 'service_response']]

# --- final schema dct
schema_dct = {
    'repair_orders': RepairOrderDF,
    'jobs': JobsDF,
    'labor': LaborDF,
    'parts': PartsDF,
    'parts_soup': PartsSoupDF,
    'labor_soup': LaborSoupDF,
    'jobs_soup': JobsSoupDF,
    'raw_response': RawResponseDF,
    'parsed_response': ParsedResponseDF
}

# --- output
for i in schema_dct:
    name_tmp = f'./data/{i}.csv'
    df_tmp = schema_dct[i]
    df_tmp.to_csv(name_tmp, index=False)
