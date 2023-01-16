#!/usr/bin/env python

# Inspired by https://github.com/derhuerst/fetch-bvg-occupancy/blob/1ebb864b1ff7130f9d2f0ab031c6d78bcabdd633/lib/query-power-bi.js

import requests
from datetime import datetime, timezone
import csv


API_URL = 'https://wabi-canada-central-api.analysis.windows.net/public/reports/querydata?synchronous=true'
DATASET_ID = '150dbede-6908-465b-ad2c-bece9770f16b'
REPORT_ID = '895f6037-52fd-4925-a8a6-e5c7af44d2b0'

UTC = timezone.utc


def query_powerbi_endpoint(q: list):
    body = {
        "version": "1.0.0",
        "queries": q,
        "modelId": 2742974,
        "cancelQueries": [],
    }

    r = requests.post(
        url=API_URL,
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            # TODO: Should these be generated dynamically?
            'ActivityId': '6a4cc7ac-c884-4dff-8473-b608c8abeeae',
            'RequestId': '17261023-7538-cf2a-eff1-df0e72f178a1',
            'X-PowerBI-ResourceKey': 'c4d6a855-6b44-44f3-b414-385b461ddd8e',
        },
        json=body,
    )

    if r.status_code != 200:
        raise requests.RequestException()
    return r.json()


def compose_query_for_wastewater_site_list():
    # Constant query
    return {
        'ApplicationContext':
            {'DatasetId': DATASET_ID,
             'Sources': [{'ReportId': REPORT_ID, 'VisualId': '8197f414e41e1d03702d'}]},
        'Query': { 'Commands': [
            {'SemanticQueryDataShapeCommand':
                {'Binding':
                    {'DataReduction': {'DataVolume': 3, 'Primary': {'Window': {}}},
                     'IncludeEmptyGroups': True,
                     'Primary': {'Groupings': [{'Projections': [0]}]},
                     'Version': 1},
                 'ExecutionMetricsKind': 1,
                 'Query': {
                    'From': [{'Entity': 'Dim: Wastewater Sites', 'Name': 'd', 'Type': 0}],
                    'Select': [{'Column': {'Expression': {'SourceRef': {'Source': 'd'}}, 'Property': 'Site'}, 'Name': 'Dim: Wastewater Sites.Site'}], 'Version': 2
                 }
                }
            }
        ]}}
 

def parse_wastewater_site_list(a):
    # Custom parser for list of constants
    data = a['results'][0]['result']['data']
    descriptor = data['descriptor']
    dsr = data['dsr']
    version = dsr['Version']

    assert version == 2

    result = []

    dataset = dsr['DS']

    for ds in dataset:
        ph = ds['PH']
        for dm in ph:
            field_name = None
            if 'DM0' in ph[0]:
                for row in dm['DM0']:
                    if 'S' in row:
                        # header info
                        field_name = row['S'][0]['N']
                        assert row['S'][0]['T'] == 1  # String type?
                        break

            if field_name:
                for row in dm['DM0']:
                    result.append(row[field_name])

    return result


def parse_metric_result(a):
    assert len(a['jobIds']) == 1
    first_job_id = a['jobIds'][0]
    first_result = a['results'][0]
    assert first_result['jobId'] == first_job_id


    data = first_result['result']['data']
    descriptor = data['descriptor']
    dsr = data['dsr']
    version = dsr['Version']

    assert version == 2

    result = []

    first_dataset = dsr['DS'][0]

    #print("First dataset", first_dataset)    
    #schema = first_dataset['S']
    ph = first_dataset['PH'][0]

    dm0 = ph['DM0']
    subschema = dm0[0]['S']

    #print('Descriptor', descriptor)
    #print('Schema', schema)
    #print('Subschema', subschema)

    # each row in dm0 parsed with the subschema
    for row in dm0:
        parsed_row = list(parse_c_values_by_schema(row['C'], subschema))
        yield parsed_row


def parse_c_values_by_schema(c_values, subschema):
    assert len(c_values) == len(subschema)

    for value, schema in zip(c_values, subschema):
        yield load_typed_value(value, schema['T'])


def load_typed_value(v, t):
    if t == 1:
        return str(v)
    if t == 7:
        return datetime.fromtimestamp(v / 1000.0, tz=UTC).date()  # v in milliseconds utc
    if t == 3:
        return float(v)
    raise ValueError("Unknown type t for value", t, v)


def compose_query_for_wastewater_history_for_site(site_name):
    q = {
        "Query": {
            "Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [{
                            "Name": "c",
                            "Entity": "Calendar",
                            "Type": 0
                        }, {
                            "Name": "f",
                            "Entity": "Fact: Waste Water Over Time",
                            "Type": 0
                        }, {
                            "Name": "d",
                            "Entity": "Dim: Wastewater Sites",
                            "Type": 0
                        }],
                        "Select": [{
                            "Column": {
                                "Expression": {
                                    "SourceRef": {
                                        "Source": "c"
                                    }
                                },
                                "Property": "Date"
                            },
                            "Name": "Calendar.Date"
                        }, {
                            "Measure": {
                                "Expression": {
                                    "SourceRef": {
                                        "Source": "f"
                                    }
                                },
                                "Property": "Norm N1N2"
                            },
                            "Name": "Fact: Waste Water Over Time.Norm N1N2"
                        }, {
                            "Measure": {
                                "Expression": {
                                    "SourceRef": {
                                        "Source": "f"
                                    }
                                },
                                "Property": "WW Trends Title"
                            },
                            "Name": "Fact: Waste Water Over Time.WW Trends Title"
                        }],
                        "Where": [{
                            "Condition": {
                                "In": {
                                    "Expressions": [{
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "d"
                                                }
                                            },
                                            "Property": "Site"
                                        }
                                    }],
                                    "Values": [
                                        [{
                                            "Literal": {
                                                "Value": "'{}'".format(site_name)
                                            }
                                        }]
                                    ]
                                }
                            }
                        }]
                    },
                    "Binding": {
                        "Primary": {
                            "Groupings": [{
                                "Projections": [0, 1]
                            }]
                        },
                        "Projections": [2],
                        "DataReduction": {
                            "DataVolume": 4,
                            "Primary": {
                                "BinnedLineSample": {}
                            }
                        },
                        "Version": 1
                    },
                    "ExecutionMetricsKind": 1
                }
            }]
        },
        "ApplicationContext": {
            "DatasetId": DATASET_ID,
            "Sources": [{
                "ReportId": REPORT_ID,
                "VisualId": "434994cd0348d4b50756"
            }]
        }
    }
    return q


def fetch_wastewater_site_list():
    q = compose_query_for_wastewater_site_list()
    result = query_powerbi_endpoint([q])
    sites = parse_wastewater_site_list(result)
    return sites


def fetch_wastewater_history_for_site(site_name):
    q = compose_query_for_wastewater_history_for_site(site_name)
    result = query_powerbi_endpoint([q])
    return parse_metric_result(result)


if __name__ == '__main__':   
    sites = fetch_wastewater_site_list()
    
    with open('sites.txt', 'w') as sitefile:
        writer = csv.writer(sitefile)
        for s in sites:
            writer.writerow((s,))

    result_for_site = {}

    for site_name in sites:
        result_for_site[site_name] = list(fetch_wastewater_history_for_site(site_name))

        with open('result-{}.csv'.format(site_name), 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["DateUTC", "Value"])
            writer.writerows(result_for_site[site_name])

    # TODO: combined output
