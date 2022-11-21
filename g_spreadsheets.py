# Файл, полученный в Google Developer Console
import json
import os
import httplib2
import googleapiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

CREDENTIALS_FILE = None


def g_service_init():
    global CREDENTIALS_FILE
    CREDENTIALS_FILE = os.environ.get('GOOGLE_CREDENTIALS_FILE')


def get_service():
    """ Авторизуемся и получаем service — экземпляр доступа к API. """
    service = None
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            CREDENTIALS_FILE,
            ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive'])
        http_auth = credentials.authorize(httplib2.Http())
        service = googleapiclient.discovery.build('sheets', 'v4', http=http_auth)
    except Exception as e:
        print(f'unable to get google api service client, CREDENTIALS_FILE: {CREDENTIALS_FILE}, ', str(e))
    return service


def check_spreadsheet(url):
    service = get_service()
    spreadsheet_id = get_spreadsheet_id(url)
    range_ = 'A1:B2'
    return get_data_from_sheet(service, spreadsheet_id, range_)


def get_data_from_sheet(service, spreadsheet_id, range_=None, major_dimension='ROWS'):
    if service is None:
        print(f'invalid google api service client: {service}')
        return None
    if spreadsheet_id is None:
        print(f'invalid spreadsheet_id: {spreadsheet_id}')
        return None
    values = None
    try:
        values = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_,
            majorDimension=major_dimension
        ).execute()
    except Exception as e:
        print(f'unable to get spreadsheet data, spreadsheet id: {spreadsheet_id}, ', str(e))
    return values


def get_spreadsheet_id(url):
    """returns the id of the Google Sheets document"""
    sh_id = None
    try:
        sh_id = url.split('/edit')[0].split('/')[-1]
    except Exception as e:
        print('unable to get spreadsheet id, ', str(e))
    return sh_id


def get_credentials_email():
    """return email link for spreadsheet edit access"""
    email = None
    try:
        with open(CREDENTIALS_FILE) as f:
            email = json.loads(f.read())['client_email']
    except Exception as e:
        print('unable to get client email, ', str(e))
    return email


def get_range(from_, to_):
    """ Return string in format [letter][number]:[letter][number] for Google Sheets range setup.
    Input data with count from 1, if < 1 - set to 1. """
    from_[0] = 1 if from_[0] < 1 else from_[0]
    from_[1] = 1 if from_[1] < 1 else from_[1]
    to_[0] = 1 if to_[0] < 1 else to_[0]
    to_[1] = 1 if to_[1] < 1 else to_[1]
    r_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
              'U', 'V', 'W', 'X', 'Y', 'Z']
    range_ = f"{r_list[from_[0] - 1]}{from_[1]}:{r_list[to_[0] - 1]}{to_[1]}"
    print(range_)
    return range_


def add_text_to_sheet(service, spreadsheet_id, data, range_, major_dimension='ROWS'):
    if service is None:
        print(f'invalid google api service client: {service}')
        return
    if spreadsheet_id is None:
        print(f'invalid spreadsheet_id: {spreadsheet_id}')
        return
    try:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {"range": range_,
                     "majorDimension": major_dimension,
                     "values": data},
                ]
            }
        ).execute()
    except Exception as e:
        print(f'unable to add data to spreadsheet, spreadsheet id: {spreadsheet_id}, ', str(e))


def insert_rows_or_columns(service, spreadsheet_id, data, start_index, major_dimension='ROWS', sheet_id='0'):
    if service is None:
        print(f'invalid google api service client: {service}')
        return
    if spreadsheet_id is None:
        print(f'invalid spreadsheet_id: {spreadsheet_id}')
        return
    if len(data) == 0:
        print(f'invalid incoming data: {data}')
        return
    try:
        end_index = start_index + len(data)
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                'requests': [
                    {
                        'insertDimension': {
                            'range': {
                                'sheetId': sheet_id,
                                'dimension': major_dimension,
                                'startIndex': start_index,
                                'endIndex': end_index
                            }
                        }
                    }
                ]
            }
        ).execute()
        if major_dimension in ['ROWS', 'COLUMNS']:
            length = 0
            for d in data:
                if len(d) > length:
                    length = len(d)
            if major_dimension == 'ROWS':
                range_ = get_range([1, start_index + 1], [length + 1, start_index + len(data)])
            else:
                range_ = get_range([start_index + 1, 1], [start_index + len(data), length + 1])
            add_text_to_sheet(service, spreadsheet_id, data, range_, major_dimension)
    except Exception as e:
        print(f'unable to add data to spreadsheet, spreadsheet id: {spreadsheet_id}, ', str(e))
