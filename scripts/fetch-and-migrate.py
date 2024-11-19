#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This modules aids in retrieving user-segregated Household register records that
have ITN registration details. Firstly, it filters tracked entities based on organisation unit (or set of )
specified. In addition, the it will initiate the transfer of ownership from the specified origin organisation unit(s)
to the destination organisation units.
After executing the ownership transfer, a payload of the tracked entities transferred will be exported to the
filesystem.
"""

import requests
from requests.auth import HTTPBasicAuth
from getpass import getpass
import typing
from datetime import datetime
import json
from pathlib import Path
import os
import re
from concurrent.futures import ThreadPoolExecutor
import pydash as py_
from dotenv import load_dotenv, find_dotenv

def main():
    origin_org_unit = input("Enter origin organisation unit ID : ").strip()
    destination_org_unit = input("Enter destination organisation unit ID : ").strip()
    filter_username = input("Enter target username: ")

    org_unit_params = re.sub(pattern=r"\s+",string=origin_org_unit,repl=";")
    # print(org_unit_params)


    with requests.Session() as session:
        session.auth = basic
        new_data = retrieve_and_transfer(session=session,url=URL,filter_=filter_username,ous=org_unit_params)
        # print(new_data)
        with ThreadPoolExecutor(max_workers=8) as tpe:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_data_dir = BASE_OUTPUT_DATA_DIR / f"{filter_username}" / f"{timestamp}"
            output_data_dir.mkdir(exist_ok=True, parents=True)
            with (output_data_dir /  f"raw_tracked_entities_{org_unit_params.replace(';','_')}_{timestamp}.json").open(mode="w") as outf:
                with (output_data_dir /  f"updated_tracked_entities_{destination_org_unit}_{timestamp}.json").open(mode="w") as outfpath:
                    futures = [tpe.submit(transfer, session, payload, TRANSFER_API_BASE_URL, org_unit_params, destination_org_unit) for payload in new_data]
                    full_data = [future.result() for future in futures if future.result()]
                    # print(full_data)
                    json.dump(obj={"trackedEntities" : new_data}, fp=outf)
                    data_string = re.sub(string=re.sub(string=json.dumps(obj=full_data), 
                                         pattern=org_unit_params.replace(";","|"), 
                                         repl=destination_org_unit),
                                         pattern='"orgUnitName":\s*".*?"\s*,?',
                                         repl="")
                    
                    # Handle trailing commas and orphaned braces {}
                    data_string = re.sub(string=re.sub(pattern=r',\s*}',
                                         repl='}',string=data_string),
                                         pattern=r'{\s*,',repl='{')
                    
                    # print(data_string)
                    new_payload = {"trackedEntities" : json.loads(data_string)}
                    print(new_payload)
                    json.dump(obj=new_payload, fp=outfpath)

def filter_data(payload : typing.List[typing.Dict], filter_username : str) -> typing.Iterable:
    data = py_.collections.filter_(payload, lambda x : x['attributes'][0]['storedBy'] == filter_username)
    return data

def get_org_unit_name(session : requests.Session,
                      url : str,
                      org_uid : str) -> typing.Union[str|None]:
    if (org_unit_req := session.get(url.format(org_uid))).status_code == 200:
        org_unit_name = org_unit_req['name']
    return org_unit_name 


def retrieve_and_transfer(session: requests.Session,
                        filter_ : str,
                        url : str, 
                        ous : str
                        # payload : typing.Optional[typing.Dict], 
                        # origin_org : typing.Optional[str],
                        # dest_org:typing.Optional[str]                        
                          ) -> typing.Any:
    filtered_data = []
    if (tei_data_req := session.get(url=url.format(ous))).status_code == 200:
        tei_data = tei_data_req.json()['instances']
        data = filter_data(payload=tei_data, filter_username=filter_)
        filtered_data = data
    return filtered_data


def transfer(session : requests.Session, 
             payload : dict, 
             url : str,
             org_params,
             dest_ou) -> typing.Any:
    trackedEntity = payload['trackedEntity']
    new_payload = {}
    program = payload['programOwners'][0]['program']
    full_url = url.format(trackedEntity, program, dest_ou)
    if (transfer_rec := session.put(full_url)).status_code == 200:
        data_str = json.dumps(obj=payload)
        
        payload_str = re.sub(string=data_str, pattern=org_params.replace(";","|"), repl=dest_ou)
        new_payload.update(json.loads(payload_str))
    return new_payload
        

        
if __name__ == "__main__":
    load_dotenv(find_dotenv())
    print(find_dotenv())
    
    dhis2_api_base = os.environ.get("DHIS2_API_ROOT_URL")

    BASE_OUTPUT_DATA_DIR = Path(__file__).parents[1] / 'data'
    URL = f"{dhis2_api_base}/42/tracker/trackedEntities?program=sXzdrtXMink"+"&orgUnit={}&totalPages=true&fields=*&skipPaging=true"
    TRANSFER_API_BASE_URL = f"{dhis2_api_base}/40/tracker/ownership/transfer?" + "trackedEntityInstance={}&program={}&ou={}"
    ORG_UNIT_QUERY_URL = f"{dhis2_api_base}/42/organisationUnits/"+"{}.json?fields=name"

    # print(BASE_OUTPUT_DATA_DIR)
    username = os.environ.get("DHIS2_USERNAME","") or input("Enter username: ").strip()
    password = os.environ.get("DHIS2_PASSWORD","") or getpass("Enter DHIS2 password! : ")

    basic = HTTPBasicAuth(username=username, password=password)

    while True:
        main()
        print("======== END ========")

