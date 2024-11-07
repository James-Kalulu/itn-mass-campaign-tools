

import pandas as pd
from pathlib import Path
import pydash as py_

# Searches for recent csv file in directory
file_directory = Path(__file__).parent
print(file_directory)
filepath = py_.max_by(collection=file_directory.glob("*.csv"), iteratee=lambda x : x.stat().st_mtime_ns)
print(filepath)

# %%
data = pd.read_csv(filepath_or_buffer=filepath)
data.head()

# %%
column_name = lambda x : x.split("-")[-1].strip()

# %%
column_names = data.columns.map(column_name)
data_new = data.copy()
data_new.columns = column_names
data_new.head()

# %%
fill_mapper = {'Household System ID' : "",'Registration type' : "",
               "Household head name" : "",
               "Household head identifier":"",
               "Village Name" : "", "Number of household members" : 0,
               "Number of ITNs required" : 0}

# %%
data_grouped = data_new.groupby(by=['Last updated by','Organisation unit name'])
data_regrouped = data_new.groupby(by=['Last updated by'])

# %%
from datetime import datetime

# %%
data_grouped.groups

# %%
import re

# %%
import math

# %%
def re_allocate(reg_type, pop, alloc) -> int:
    if reg_type == 'Household' or reg_type != "":
        if pop > 2:
            return min(math.ceil(pop / 2),3)
        else:
            return 2
    else:
        return 2 if alloc==3 else min(math.ceil(pop / 2), 3)

# %%
p = 5
5 <= p < 9

# %%
def re_allocatex(reg_type,pop, alloc) -> int:
    if alloc > 2:
        return min(alloc - 1, 3)
    else:
        return min(alloc,3)

# %%
def ration(data : pd.DataFrame) -> pd.DataFrame:
    new_data  = data.copy()
    new_data['Number of ITNs to be received'] = new_data.apply(func=lambda x : re_allocatex(x['Registration type'],x['Number of household members'],
                                                                                            x['Number of ITNs required']), 
                                                               axis='columns')
    return new_data

# %%
column_remapper = {"Organisation unit name" : "Catchment Area"}
drop_columns = ['Village Name','Last updated by']

# %%
# %%script echo --false
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
output_filepath = filepath.parent / '{}_segregated_{}.xlsx'.format(filepath.stem, timestamp)
summaries = []
with pd.ExcelWriter(path=output_filepath) as writer:
    for group in data_regrouped.groups:
        group_data = ration(pd.DataFrame(data_regrouped.get_group((group,)).rename(columns=column_remapper
                                                                        ).fillna(fill_mapper
                                                                            ).drop(columns=drop_columns
                                                                               ).sort_values(by=['Date of household registration',
                                                                                                 'Date of household registration into ITN campaign',
                                                                                                 'Household System ID']
                                                                                             ).drop_duplicates(subset=['Household System ID'],
                                                                                                               keep='last')))
        
        username = " ".join([el.strip() 
                             for el in reversed(re.sub(pattern=r"\(\w+\)",string=group,repl="").strip().split(","))])
        print(username)
        # org_unit_name = group[1]
        total_households = group_data['Household System ID'].nunique()
        # print( group_data['Number of household members'].astype(int).value_counts())
        total_null_households = group_data['Number of household members'].astype(int).value_counts().to_dict().get(0,0)
        total_pop = int(group_data['Number of household members'].sum())
        total_itns_required = group_data['Number of ITNs required'].sum()  
        total_itns_to_receive = group_data['Number of ITNs to be received'].sum()
        org_unit_name = ",".join(group_data['Catchment Area'].unique())  
        sheetname = re.sub(pattern=r"[\[\]:*?/\]]",string="{} - {}".format(username, org_unit_name)[:31],repl="")                                                              
        group_summary_data = pd.DataFrame({'Catchment Area': [org_unit_name],
                                           'Assigned HSA' : [username],
                                           'Number of households' : [total_households], "Total household members" : [total_pop] ,
                                           "Number of households unallocated (missing pop/invalid)" : total_null_households,
                                           "Total ITNs allocated (initial)" : [total_itns_required], "Total ITNs re-allocated (final)" : [total_itns_to_receive] })
        summaries.append(group_summary_data)
        group_data.to_excel(sheet_name=sheetname,excel_writer=writer, index=False)
    summary_data = pd.concat(summaries)
    summary_data.to_excel(sheet_name="A Summarized Logistics Plan", excel_writer=writer, index=False)

# %%
