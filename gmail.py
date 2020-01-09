import code
import hashlib
import json
import os
import pickle
import time
import os.path
from collections import namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor as Pool, as_completed
from queue import Queue
import sqlite3
from typing import Dict, List, Union, Optional
import threading

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient import errors

import PySimpleGUI as sg

# If modifying these scopes, delete the file token.pickle.
SCOPES = [
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.compose'
]
METADATA_HEADERS = ['From', 'Subject', 'Date']
EmailMessage = namedtuple('EmailMessage', ['message_id', 'thread_id'])

TRASHED = 1
TRASHING_FAILED = 2

def main():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    label_ids = [
        ['CATEGORY_PROMOTIONS'],
        ['CATEGORY_FORUMS'],
        ['CATEGORY_SOCIAL'],
        ['CATEGORY_UPDATES']
    ]

    user_id = 'me'
    workers = 8

    db = Database('email.db')
    db.init_connection()
    db.create_db_if_not_exists()

    preload = False

    get_service_object()
    if preload:
        service = get_service_object()
        preload_emails(label_ids, service, db, user_id, workers)
    else:
        start_window(db, workers=workers, sleep=1)


def preload_emails(label_ids, service, db, user_id='me', workers=5):

    for labels in label_ids:
        messages = get_messages_info(labels, service, user_id)

        print(f"Total Messages Found: {len(messages)}")

        metadata = get_emails_metadata(labels, service, messages, user_id, workers)

        db.populate_emails_metadata(labels[0], metadata)


class Database(object):

    def __init__(self, db_path):
        self._db_path = db_path
        self.connection: sqlite3.Connection = None

    def init_connection(self):
        self.connection = sqlite3.connect(self._db_path)

    def create_instance(self):
        db = Database(self._db_path)
        db.init_connection()
        return db

    def create_db_if_not_exists(self):

        table_schema = '''
            CREATE TABLE IF NOT EXISTS email_metadata (
                message_id string NOT NULL,
                label string NOT NULL,
                labels string,
                snippet string,
                subject string,
                date string,
                sender_name string,
                sender_email string,
                status integer,
                PRIMARY KEY (message_id, label)
            );
        '''

        email_schema = '''
            CREATE TABLE IF NOT EXISTS email_message (
                message_id string NOT NULL,
                label string NOT NULL,
                labels string,
                snippet string,
                subject string,
                date string,
                sender_name string,
                sender_email string,
                status integer,
                data text,
                PRIMARY KEY (message_id, label)
            );
        '''

        try:
            self.connection.execute(table_schema)
            self.connection.commit()
        except sqlite3.Error as e:
            print(e)
            raise

    def populate_emails_metadata(self, label, emails_metadata: List[Dict]):

        sql = '''
            INSERT INTO email_metadata(message_id, label, labels, snippet, subject, date, sender_name, sender_email, status)
            VALUES(?,?,?,?,?,?,?,?,?)
        '''

        print("Adding Rows into DB")
        for item in emails_metadata:

            parts = item['sender_email'].split('<')
            if len(parts) > 1:
                parts = parts[0].split('>')
            email = parts[0]

            row = (
                item['message_id'], label, ','.join(item['label_ids']), item['snippet'],
                item['subject'], item['date'], item['sender_name'], email, 0
            )
            try:
                self.connection.execute(sql, row)
            except sqlite3.IntegrityError as e:
                print(e)

        print("Commiting to Database")
        self.connection.commit()
        print("Data has saved successfully")

    def update_status(self, status, message_id):

        try:
            query = 'update email_metadata set status=? where message_id=?'
            self.connection.execute(query, (status, message_id,))
            self.connection.commit()
        except sqlite3.Error as e:
            print(e)
            return False

        return True

    def unique_emails_count(self) -> List:

        query = f'select sender_email, label, count(sender_email), status from email_metadata where status != {TRASHED} group by label, sender_email'
        cursor = self.connection.cursor()

        cursor.execute(query)
        rows = cursor.fetchall()
        rows = [list(row) for row in rows]
        rows.sort(key=lambda x: x[2], reverse=True)

        return rows

    def get_records(self, sender_emails):
        print(f"Got Emails: {','.join(sender_emails)}")
        emails = "'" + "','".join(sender_emails) + "'"
        print(emails)
        query = f'select * from email_metadata where sender_email in ({emails}) and status != {TRASHED}'
        factory = self.connection.row_factory
        self.connection.row_factory = sqlite3.Row
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        self.connection.row_factory = factory
        print(f"get_records: found: {len(rows)}")
        return rows


done_so_far = 0
error = 0


def start_window(db, user_id='me', workers=10, sleep=1.0, timeout=0):

    sg.theme('Dark Blue 3')  # please make your windows colorful

    headers = ['Email Address', 'Label', 'Count', 'Status']
    data = db.unique_emails_count()

    layout = [
        [
            sg.Text('Work progress'),
            sg.ProgressBar(10, size=(20,20), orientation='h', key='_progress_'),
            sg.Text('N/A', key='_progress_detail_', size=(25,1)),
        ],
        [sg.Table(values=data,
                  headings=headers,
                  display_row_numbers=True,
                  max_col_width=25,
                  auto_size_columns=True,
                  justification='right',
                  key='_table_',
                  enable_events=True,
                  right_click_menu=['&Right', ['Trash All']],
                  num_rows=min(len(data), 20))]
    ]

    window = sg.Window('Unique Emails Count', layout)

    in_progress = False
    total = 0

    def trash_all(rows, total_rows, user_id, workers=5, sleep=1.0, timeout=None):
        global done_so_far, error
        new_db = db.create_instance()
        emails = new_db.get_records([row[0] for row in rows])
        service_pool = create_service_pool(workers)

        worker_pool = Pool(workers)

        with Pool(workers) as worker_pool:
            futures = {
                worker_pool.submit(trash_email, service_pool, user_id, email['message_id'], sleep): email['message_id']
                for email in emails
            }

            emails_metadata = []
            count = 0
            for future in as_completed(futures, timeout=timeout):
                result = future.result()
                message_id = futures[future]
                print(f"[{count}] Got response for one message. {result}")
                done_so_far += 1
                status = TRASHED
                if not result:
                    error += 1
                    status = TRASHING_FAILED

                new_db.update_status(status, message_id)

    def handle_trash_all(rows):
        rows_to_trash = sum([row[2] for row in rows])

        print(f'Rows to trash: {rows_to_trash}')
        th = threading.Thread(target=trash_all, args=(rows, rows_to_trash, user_id, workers, sleep,), daemon=True)
        th.start()
        print('Thread started to process in background')
        return rows_to_trash

    while True:
        event, values = window.read(timeout=1000)
        if event != '__TIMEOUT__':
            print(event, values)

        if event in (None, 'Exit'):
            break

        # table row selected
        if event == '_table_':
            table : sg.Table = window.Element('_table_')
            print(table.SelectedRows)
            print(table.Values[table.SelectedRows[0]])

        if event == 'Trash All':
            table: sg.Table = window.Element('_table_')
            print(table.SelectedRows)
            rows = [table.Values[row] for row in table.SelectedRows]
            total = handle_trash_all(rows)
            window['_progress_'].update_bar(done_so_far, max=total)
            in_progress = True

        if in_progress:
            print(f'Updating progress bar. Done so far: {done_so_far}')
            window['_progress_'].update_bar(done_so_far, max=total)
            window['_progress_detail_'].update(f'{done_so_far}/{total} (Error: {error}')
            in_progress = done_so_far < total
            if not in_progress:
                window['_progress_'].update_bar(total, max=total)

    window.close()


def get_service_object(pickled_path='token.pickle', credentials_path='credentials.json'):

    creds = None
    if os.path.exists(pickled_path):
        with open(pickled_path, 'rb') as token:
            creds = pickle.load(token)
            print("Pickled file loaded")

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(pickled_path, 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)

    print("Service object is created")
    return service


def message_decoder(dct):
    return EmailMessage(dct['id'], dct['threadId'])


def get_unique_id(label_ids):
    md5 = hashlib.md5(','.join(label_ids).encode('utf-8'))
    return f'{md5.hexdigest()}'


def get_filename(label_ids):
    return f'{get_unique_id(label_ids)}.json'


def deserialize_data(filepath, object_hook=None):
    with open(filepath, 'r') as fh:
        return json.load(fh, object_hook=object_hook)


def serialize_data(filepath, data):

    with open(filepath, 'w') as fh:
        json.dump(data, fh)


def trash_email(service_pool: Queue, user_id: str, message_id: str, sleep=0.75):

    print(f"UserId: {user_id} Trash Email: {message_id}")
    is_pool = isinstance(service_pool, Queue)

    if is_pool:
        print("Waiting for free connection object")
        service = service_pool.get()
        print("Service Connection is available and received")
    else:
        service = service_pool

    try:

        response = service.users().messages().trash(userId=user_id, id=message_id).execute()

    except errors.HttpError as error:
        print(f'Error Happened while Trashing messge: {message_id}')
        print(error)
        return False
    finally:
        if is_pool:
            print("Connection is free now, putting it back")
            service_pool.put(service)
            time.sleep(sleep)
    return True


def get_email_metadata(service_pool: Queue, msg: EmailMessage, dump_dirpath, user_id='me', sleep=0):

    is_pool = isinstance(service_pool, Queue)

    if is_pool:
        print("Waiting for free connection object")
        service = service_pool.get()
        print("Service Connection is available and received")
    else:
        service = service_pool

    try:

        email_path = os.path.join(dump_dirpath, f'{msg.message_id}.json')

        if os.path.exists(email_path):
            response = deserialize_data(email_path)
        else:
            response = service.users().messages().get(
                userId=user_id, id=msg.message_id,
                format='metadata', metadataHeaders=METADATA_HEADERS
            ).execute()

            serialize_data(email_path, response)

        metadata = {
            'message_id': response['id'], 'label_ids': response['labelIds'],
            'snippet': response['snippet'],
        }

        for header in response['payload']['headers']:
            name = header['name'].lower()
            value = header['value']
            if name == 'from':
                # e.g. value = '"CyberCoders JobAlert" <JobAlerts@CyberCoders.com>'
                parts = value.split(' <')
                metadata['sender_name'] = parts[0]
                if len(parts) >= 2:
                    metadata['sender_email'] = parts[1][:-1]
                else:
                    metadata['sender_email'] = value
            else:
                metadata[name] = value

        return metadata
    except errors.HttpError as error:
        print(error)
    finally:
        if is_pool:
            print("Connection is free now, putting it back")
            service_pool.put(service)
            time.sleep(sleep)
    return None


def create_service_pool(workers=5):

    print("Initializing service pool")
    service_pool = Queue(maxsize=workers)
    for _ in range(workers):
        service_pool.put(get_service_object())

    print("Done creating service pool, returning it now")
    return service_pool


def get_emails_metadata(label_ids, service, messages, user_id='me', workers=5, timeout=None):

    dump_dir = get_unique_id(label_ids)
    os.makedirs(dump_dir, exist_ok=True)

    email_metadata_path = f"{dump_dir}-metadata.json"
    if os.path.exists(email_metadata_path):
        return deserialize_data(email_metadata_path)

    use_pool = True
    if use_pool:
        service_pool = create_service_pool(workers+2)
    else:
        service_pool = service

    print(f"Service Pool is created with {workers} connection")
    worker_pool = Pool(workers)

    sleep = 0.75

    futures = [
        worker_pool.submit(get_email_metadata, service_pool, msg, dump_dir, user_id, sleep)
        for msg in messages
    ]

    emails_metadata = []
    count = 0
    for future in as_completed(futures, timeout=timeout):
        result = future.result()
        print(f"[{count}] Got response for one message. {result}")
        count += 1
        if result:
            emails_metadata.append(result)

    serialize_data(email_metadata_path, emails_metadata)

    return emails_metadata


def get_messages_info(label_ids, service, user_id='me'):

    email_path = get_filename(label_ids)
    if os.path.exists(email_path):
        return deserialize_data(email_path, object_hook=message_decoder)

    try:
        response = service.users().messages().list(userId=user_id,
                                                   labelIds=label_ids).execute()
        messages = []
        if 'messages' in response:
            messages.extend(response['messages'])

        print(f'Total Result Size Estimate: {response.get("resultSizeEstimate", "N/A")}')
        batch = 1
        while 'nextPageToken' in response:
            batch += 1
            print(f"Loading Batch: {batch}")
            page_token = response['nextPageToken']
            response = service.users().messages().list(userId=user_id,
                                                       labelIds=label_ids,
                                                       pageToken=page_token).execute()
            messages.extend(response['messages'])

        print("All data has been loaded. Saving it to json")
        serialize_data(email_path, messages)

        return deserialize_data(email_path, object_hook=message_decoder)
    except errors.HttpError as error:
        print(error)


if __name__ == '__main__':
    main()
