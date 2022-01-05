import requests, uuid, argparse, os
import pandas as pd

credentials_path = 'credentials.json'
parser = argparse.ArgumentParser()

parser.add_argument("-f", "--full", help="If passed, the full data from all content will downloaded. If omitted just id, xapi and title of course and asset will populate", action='store_true')
parser.add_argument("-m", "--maxPage", help="The max page number to paginate through",  type=int)

if not os.path.exists(credentials_path):
    print('No credentials in place.')
    
    def validate_uuid():
        value = input('Enter orgId UUID:')
        try: 
            uuid.UUID(value)
            return value
        except ValueError:
            print('Not a valid UUID. Try again.')
            validate_uuid()
    
    def validate_token():
        value = input('Enter bearer token (exclude "Bearer " when you paste):')
        if len(value) == 0:
            print('Bearer token cannot be empty. Try again')
            validate_token()
        return value    
    org_id = validate_uuid()
    bearer_token = validate_token()
    
    df = pd.DataFrame( columns=['orgId', 'token'])#.to_json('test.json',orient='records')
    df.at[0,'orgId'] = org_id
    df.at[0,'token'] = bearer_token
    df.iloc[0].to_json('test.json')#,orient='records')

else: 
    df = pd.read_json(credentials_path, typ='series')
    org_id = df.orgId
    token = df.token


args = parser.parse_args()
rand_uuid = uuid.uuid4()

max_page = int(args.maxPage * 1000) if args.maxPage else 1000000
mt_arr = [] 


def main():
         
    
    headers = {"Authorization": "Bearer {}".format(token)}

    for page in range(0,max_page,1000):

        data = requests.get('https://api.percipio.com/content-discovery/v2/organizations/{}/catalog-content?offset={}&max=1000&pagingRequestId={}&updatedSince=2002-10-26T00:00:00.11Z'.format(org_id, page, rand_uuid), 
        headers=headers)

        for i in data.json():
            try:
                flat = pd.json_normalize(i)
            except NotImplementedError:
                print('Problem with bearer token. Please check format and try again.')
                return
            flat['localizedMetadata.title'] = flat['localizedMetadata'][0][0]['title']
            try:
                flat['technologies.title'] = flat['technologies'][0][0]['title']
                flat['technologies.version'] = flat['technologies'][0][0]['version']
            except IndexError:
                flat['technologies.title'] = None
                flat['technologies.version'] = None

            mt_arr.append(flat)

        print('Page',int(page / 1000 + 1),'done')

    fname = 'full_results_'+org_id+'.csv' if args.full else 'lite_results_'+org_id+'.csv'
    final = pd.concat(mt_arr)
    important_columns = ['id', 'xapiActivityId','localizedMetadata.title','technologies.title','technologies.version']
    columns_order = important_columns + [col for col in final.columns.tolist() if col not in important_columns]
    final[columns_order].to_csv(fname, index=False) if args.full else final[important_columns].to_csv(fname, index=False)
    return
main()