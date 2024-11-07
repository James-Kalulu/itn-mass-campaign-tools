#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module aids in preparing a summarized pre-distribution logistics draft plan
that can be used for creating a transport plan. This also aids in enforcing rationing of ITNs on a household-level
for each household recorded in the provided line listing-derived export dataset. 
The dataset must contain a listing of households including the following information:
 - Household head name
 - Household System ID
 - Household head identifier
 - Registration type
 - Village Name
 - Number of household members
 - Number of ITNs required
 - Last updated by
 - Organisation unit name

## Usage:
 - python itn-data-segregation.py
Help : The script fetches the latest modified csv file and uses it as input. 
"""


from pathlib import Path
import re
from datetime import datetime
import typing

import pandas as pd
import pydash as py_


def re_allocate(reg_type,pop, alloc) -> int:
    if reg_type == 'Household' or reg_type != "":
        if alloc > 2:
            return min(alloc - 1, 3)
        else:
            return min(alloc,3)
    else:
        return alloc

def ration(data : pd.DataFrame) -> pd.DataFrame:
    new_data  = data.copy()
    new_data['Number of ITNs to be received'] = new_data.apply(func=lambda x : re_allocate(x['Registration type'],x['Number of household members'],
                                                                                            x['Number of ITNs required']), 
                                                               axis='columns')
    return new_data


if __name__ == "__main__":
    # Searches for recent csv file in directory path
    file_directory = Path(__file__).parent
    
    filepath = py_.max_by(collection=file_directory.glob("*.csv"), iteratee=lambda x : x.stat().st_mtime_ns)

    # Reads csv file as dataframe
    data : pd.DateFrame = pd.read_csv(filepath_or_buffer=filepath)


    rename_cols : typing.Callable = lambda x : x.split("-")[-1].strip()

    column_names = data.columns.map(rename_cols)
    data_new = data.copy()
    data_new.columns = column_names


    fill_mapper = {'Household System ID' : "",'Registration type' : "",
                "Household head name" : "",
                "Household head identifier":"",
                "Village Name" : "", "Number of household members" : 0,
                "Number of ITNs required" : 0}

    # Groups data by either org-user combination or just user
    data_grouped  = data_new.groupby(by=['Last updated by','Organisation unit name'])
    data_regrouped = data_new.groupby(by=['Last updated by'])
    
    column_remapper = {"Organisation unit name" : "Catchment Area"}
    drop_columns = ['Village Name','Last updated by']


    timestamp : str = datetime.now().strftime("%Y%m%d_%H%M")
    output_filepath : Path = filepath.parent / '{}_segregated_{}.xlsx'.format(filepath.stem, timestamp)
    summaries : typing.List = []
    catchment_area_data : typing.List = []
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
            catchment_area_data.append({"key" : sheetname, "data" : group_data})    
        summary_data = pd.concat(summaries).sort_values(by=["Assigned HSA"])
        summary_data.to_excel(sheet_name="Summarized Logistics Plan", excel_writer=writer, index=False)
        sorted_catchment_area_data : typing.List[typing.Dict[str : typing.Any]] = sorted(catchment_area_data, lambda x : x['key'])
        for data in sorted_catchment_area_data:
            data['data'].to_excel(sheetname=data['key'], writer=writer, index=False)

