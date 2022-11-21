import os
from datetime import datetime

from telethon.sync import TelegramClient

from dotenv import load_dotenv
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.patched import Message

from MessagePack import print_info_msg, print_exception_msg
from g_spreadsheets import get_data_from_sheet, get_service, add_text_to_sheet, get_range, insert_rows_or_columns, \
    g_service_init
from pytz import timezone

load_dotenv()
g_service_init()

CHANNEL = "https://t.me/zhumys_almaty"
spreadsheet_id = '1a0gJgJ4VrfEGo5Q2OMqBrmQd0AXF3l56KPX9Z8PfKK8'
api_id = os.environ.get('TG_API_ID')
api_hash = os.environ.get('TG_API_HASH')
name = 'test'
client = TelegramClient(name, api_id, api_hash)


async def get_messages(channel, from_id: int, limit: int):
    offset_msg = from_id  # номер записи, с которой начинается считывание
    limit_msg = limit  # максимальное число записей, передаваемых за один раз
    history = await client(GetHistoryRequest(
        peer=channel,
        offset_id=offset_msg,
        offset_date=None, add_offset=0,
        limit=limit_msg, max_id=0, min_id=0,
        hash=0))
    if not history.messages:
        return [], from_id
    messages = history.messages
    last_msg = messages[len(messages) - 1].id
    total_messages = len(messages)
    print(f'count: {total_messages}')
    print(f'last id: {last_msg}')
    return messages, last_msg


async def main():
    data = []
    channel = await client.get_entity(CHANNEL)
    print('channel:', channel)
    i = 0
    from_id = 0
    time = datetime.now()
    while True:
        print(f'step: {i}')
        msg_data = await get_messages(channel, from_id, 100)
        if msg_data[1] == from_id:
            break
        from_id = msg_data[1]
        for msg in msg_data[0]:
            row = ParsTelegramMsg(msg).get_msg_data()
            print(row)
            data.append(row)
        add_data_to_spreadsheet(data)
        i += 1
        # if i == 2:
        #     break
    time = datetime.now() - time
    print(time)


def add_data_to_spreadsheet(data):
    data_, row_count = get_table_data()
    header = ParsTelegramMsg.header
    if row_count == 0:
        range_ = get_range([1, 1], [len(header) + 1, 2])
        add_text_to_sheet(get_service(), spreadsheet_id, [header], range_, 'ROWS')
    range_ = get_range([1, 2], [len(header) + 1, len(data) + 2])
    add_text_to_sheet(get_service(), spreadsheet_id, data, range_, 'ROWS')


def get_table_data():
    data = get_data_from_sheet(get_service(), spreadsheet_id, range_='A1:B', major_dimension='ROWS')
    rows = data.get('values')
    row_count = len(rows) if rows else 0
    print('table row count: ', row_count)
    return data, row_count


class ParsTelegramMsg:
    header = ['ID сообщения', 'ID пользователя', 'Дата публикации', 'Сообщение', 'Fwd_from_user_id',
              'Fwd_from_channel_id', 'Fwd_from_date', 'Теги', 'Контакты', 'Инстаграм', 'Телефон']

    def __init__(self, msg_data: Message):
        super(ParsTelegramMsg, self).__init__()
        print(msg_data)
        self._msg_data = msg_data

    def get_msg_data(self):
        if self._msg_data.fwd_from:
            print(self._msg_data.fwd_from)
        tz = timezone('Europe/Moscow')
        row = [
            self._msg_data.id,  # ID сообщения
            self._msg_data.from_id.user_id if self._msg_data.from_id else "",  # ID пользователя
            self._msg_data.date.astimezone(tz).strftime("%d.%m.%Y, %H:%M:%S"),  # Дата публикации
            self._msg_data.message,  # Сообщение
            self._get_fwd_from_user(),  # Fwd_from_user_id
            self._get_fwd_from_channel(),  # Fwd_from_channel_id
            self._msg_data.fwd_from.date.astimezone(tz).strftime(
                "%d.%m.%Y, %H:%M:%S") if self._msg_data.fwd_from else None,  # Fwd_from_date
            self._get_tags(),  # Теги
            self._get_contacts(),  # Контакты
            self._get_instagram(),  # Инстаграм
            self._get_phone_number(),  # Телефон
        ]
        return row

    def _get_fwd_from_user(self):
        user_id = None
        try:
            user_id = self._msg_data.fwd_from.from_id.user_id
        except Exception as e:
            print_exception_msg('_get_fwd_from_user', f'{str(e)}')
        return user_id

    def _get_fwd_from_channel(self):
        channel_id = None
        try:
            channel_id = self._msg_data.fwd_from.from_id.channel_id
        except Exception as e:
            print_exception_msg('_get_fwd_from_user', f'{str(e)}')
        return channel_id

    def _get_tags(self):
        tags = None
        try:
            parts = self._msg_data.message.split('#')
            parts = parts[1:]
            data_ = ''
            for i, tag in enumerate(parts):
                tag = tag.replace('\n', ' ')
                if i == len(parts) - 1:
                    tag = tag.split(' ')[0]
                data_ += '#' + tag + ', '
            tags = data_[:-2]
        except Exception as e:
            print_exception_msg('_get_tags', f'{str(e)}')
        return tags

    def _get_contacts(self):
        check = ['@yandex.ru', '@gmail.com', '@outlook.com', '@hotmail.com', '@mail.ru']
        contacts = None
        try:
            parts = self._msg_data.message.split('@')
            parts = parts[1:]
            data_ = ''
            for i, part in enumerate(parts):
                part = part.replace('\n', ' ')
                part = part.split(' ')[0]
                if part != '' and '@' + part not in check and '@' + part + ',' not in data_:
                    data_ += '@' + part + ', '
            contacts = data_[:-2]
        except Exception as e:
            print_exception_msg('_get_contacts', f'{str(e)}')
        return contacts

    def _get_instagram(self):
        inst = None
        try:
            msg = self._msg_data.message
            split = 'https://www.instagram.com' if 'https://www.instagram' in msg else 'https://instagram.com'
            inst = msg.split(split)[1].split('\n')[0].split(' ')[0]
            inst = 'https://instagram.com' + inst
        except Exception as e:
            print_exception_msg('_get_instagram', f'{str(e)}')
        return inst

    def _get_phone_number(self):
        check = [" ", "-", "(", ")"]
        phone = ''
        try:
            msg = self._msg_data.message

            split = '+7' if '+7' in msg else ''
            if split == '':
                split = '8' if '8' in msg else ''

            if split != '':
                parts_ = msg.split(split)
                parts = [parts_[0]]
                for part in parts_[1:]:
                    if split == '8' and (part[0].isdigit() or part[0] in check) \
                            and (len(parts) > 1 and (parts[-1][-1].isdigit() or parts[-1][-1] in check)):
                        parts[-1] += split + part
                    else:
                        parts.append(part)
                for part in parts[1:]:
                    phone_ = split
                    for char in part:
                        if char.isdigit() or char in check:
                            if char.isdigit():
                                phone_ += char
                        else:
                            break

                    print('phone_', phone_)
                    if len(phone_) < 11 or phone_ in phone:
                        phone_ = ''
                    elif phone_[0] == '+' and len(phone) == 0:
                        phone_ = "'" + phone_
                    if len(phone) > 0 and len(phone_) > 0:
                        phone += ', '
                    phone += phone_
        except Exception as e:
            print_exception_msg('_get_phone_number', f'{str(e)}')
        return phone


if __name__ == '__main__':
    with client:
        client.loop.run_until_complete(main())
    # insert_rows_or_columns(get_service(), spreadsheet_id, [['--', '-'], ['--', '-']], 3, major_dimension='ROWS')
