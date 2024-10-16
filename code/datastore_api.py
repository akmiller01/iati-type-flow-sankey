import os
from dotenv import load_dotenv
import json
import requests
import pandas as pd
import progressbar

load_dotenv()
API_KEY = os.getenv('API_KEY')


def find_all_indices_of(value, list_to_search):
    results = list()
    for i, list_value in enumerate(list_to_search):
        if type(value) is list:
            if list_value in value:
                results.append(i)
        else:
            if list_value == value:
                results.append(i)
    return results


def multi_index(list_to_index, indices):
    return [element for i, element in enumerate(list_to_index) if i in indices]


def main():
    # Use the IATI Datastore API to fetch all
    rows = 1000
    next_cursor_mark = '*'
    current_cursor_mark = ''
    results = []
    with progressbar.ProgressBar(max_value=1) as bar:
        while next_cursor_mark != current_cursor_mark:
            url = (
                'https://api.iatistandard.org/datastore/activity/select'
                '?q=(!reporting_org_secondary_reporter:true AND '
                'transaction_transaction_date_iso_date:[2015-12-31T00:00:00Z TO 2024-01-01T00:00:00Z] AND '
                '(default_currency:[* TO *] OR transaction_value_currency:[* TO *]) AND '
                '((transaction_provider_org_type:[* TO *] AND transaction_receiver_org_type:[* TO *]) OR participating_org_type:[* TO *]) AND'
                '(transaction_transaction_type_code:3 OR transaction_transaction_type_code:4))'
                '&sort=id asc'
                '&wt=json&fl=iati_identifier,reporting_org_type,'
                'participating_org_type,participating_org_role,'
                'transaction_provider_org_type,'
                'transaction_receiver_org_type,'
                'transaction_transaction_type_code,transaction_transaction_date_iso_date,transaction_value,default_currency,transaction_value_currency'
                '&rows={}&cursorMark={}'
            ).format(rows, next_cursor_mark)
            api_json_str = requests.get(url, headers={'Ocp-Apim-Subscription-Key': API_KEY}).content
            api_content = json.loads(api_json_str)
            if bar.max_value == 1:
                bar.max_value = api_content['response']['numFound']
            activities = api_content['response']['docs']
            len_results = len(activities)
            current_cursor_mark = next_cursor_mark
            next_cursor_mark = api_content['nextCursorMark']
            for activity in activities:
                transaction_len = len(activity['transaction_transaction_date_iso_date'])
                for transaction_index in range(0, transaction_len):
                    results_dict = dict()
                    results_dict['iati_identifier'] = activity['iati_identifier']
                    transaction_year = int(activity['transaction_transaction_date_iso_date'][transaction_index][:4])
                    if transaction_year < 2016 or transaction_year > 2023:
                        continue
                    if activity['transaction_transaction_type_code'][transaction_index] not in ['3', '4']:
                        continue
                    results_dict['transaction_year'] = transaction_year
                    results_dict['value'] = 0
                    if 'transaction_value' in activity:
                        results_dict['value'] = activity['transaction_value'][transaction_index]
                    results_dict['currency'] = ''
                    if 'default_currency' in activity:
                        results_dict['currency'] = activity['default_currency']
                    if 'transaction_value_currency' in activity and len(activity['transaction_value_currency']) == transaction_len:
                        results_dict['currency'] = activity['transaction_value_currency'][transaction_index]
                    if 'participating_org_role' in activity and 'participating_org_type' in activity:
                        funding_org_indices = find_all_indices_of('1', activity['participating_org_role'])
                        implementing_org_indices = find_all_indices_of('4', activity['participating_org_role'])
                        funding_org_types = multi_index(activity['participating_org_type'], funding_org_indices)
                        implementing_org_types = multi_index(activity['participating_org_type'], implementing_org_indices)
                        if len(funding_org_types) > 0:
                            results_dict['donor_org_type_code'] = funding_org_types[0]
                        if len(implementing_org_types) > 0:
                            results_dict['recipient_org_type_code'] = implementing_org_types[0]
                    if 'reporting_org_type' in activity:
                        results_dict['donor_org_type_code'] = activity['reporting_org_type']
                    if 'transaction_provider_org_type' in activity and len(activity['transaction_provider_org_type']) == transaction_len:
                        results_dict['donor_org_type_code'] = activity['transaction_provider_org_type'][transaction_index]
                    if 'transaction_receiver_org_type' in activity and len(activity['transaction_receiver_org_type']) == transaction_len:
                        results_dict['recipient_org_type_code'] = activity['transaction_receiver_org_type'][transaction_index]
                    results.append(results_dict)

                    
            if bar.value + len_results <= bar.max_value:
                bar.update(bar.value + len_results)
    
    # Collate into Pandas dataframe
    df = pd.DataFrame.from_records(results)

    # Write to disk
    df.to_csv(
        os.path.join('input', 'api_results.csv'),
        index=False,
    )


if __name__ == '__main__':
    main()