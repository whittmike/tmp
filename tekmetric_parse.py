# =============================================================================
# import libraries
# =============================================================================
# --- lib imports
import numpy as np
import pandas as pd
import re
import json

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
os.chdir('C:/Users/mwhittlesey/Desktop/TEK_METRIC')

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
JobDF = remove_none_id(pd.concat(job_lst))
LaborDF = remove_none_id(pd.concat(labor_lst))
PartsDF = remove_none_id(pd.concat(parts_lst))

# --- get rid of rows we dont need
print(RepairOrderDF)
print(JobDF)
print(LaborDF)
print(PartsDF)







