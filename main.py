import os, logging, requests, gspread, smtplib, ssl
from datetime import datetime, timedelta
from glob import glob
from email.message import EmailMessage
from oauth2client.service_account import ServiceAccountCredentials
from load_params import LoadParams
from database_connection import DatabaseConnection
from data_processing import DataProcessing


# Функция отправки сообщения по электронной почте.
def send_email(message):
    # Создание контекста SSL по умолчанию
    context = ssl.create_default_context()
    msg = EmailMessage()
    subject = f'Loading data on {datetime.strftime(START, '%Y-%m-%d')}.'
    # Формирование сообщения.
    msg.set_content(message)
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    # Подключение к SMTP серверу.
    server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context)
    server.login(EMAIL_FROM, EMAIL_PASSWORD)
    # Отправка сообщения.
    server.send_message(msg=msg)
    # Отключение сервера.
    server.quit()
# _________________________________________________________________________________

# Текущая дата в строковом формате.
DATA = datetime.now().strftime('%Y-%m-%d')

# Настройка конструктора логирования.
logging.basicConfig(
    format='%(asctime)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO,
    filename=f'project214_log_{DATA}.log',
    filemode='a'
)

try:
    # Запись лога о начале работы программы.
    logging.info('Getting started with the program.')
    message = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n\nGetting started with the program.'

    # Удаление лог-файлов старше двух дней.
    # Считывание названий лог-файлов из текущей директории.
    dir_path = os.getcwd()
    files_log = list(glob(os.path.join(dir_path, '*.log')))
    # Определение текущей даты.
    data_now = datetime.now().date()
    # Поиск и удаление лог-файлов старше двух дней от текущей даты.
    for file in files_log:
        data_file = datetime.strptime(file[-14:-4], '%Y-%m-%d').date()
        if (data_now - data_file) >= timedelta(days=3):
            os.remove(file)
    logging.info('Obsolete log files have been removed.')
    message += '\nObsolete log files have been removed.'


    # Загрузка из файла необходимых для подключений параметров.
    lp = LoadParams('params.txt')
    # Проверка загруженных параметров на корректность.
    flags = lp.check_params()
    if not flags[0]:
        raise AttributeError(f'Incorrect value of the {flags[1]} parameter.')

    # Вводные данные для подключения к API обучающей системы.
    API_URL = lp.configs['api_url']
    CLIENT = lp.configs['client']
    CLIENT_KEY = lp.configs['client_key']
    START = datetime.strptime(lp.configs['start'], '%Y-%m-%d %H:%M:%S.%f')
    END = datetime.strptime(lp.configs['end'], '%Y-%m-%d %H:%M:%S.%f')
    if START > END:
        raise ValueError(
            f'The start value of the period is later than the end value of the period: start = {START}, end = {END}.')

    # Вводные данные для подключения к базе данных.
    DATABASE = lp.configs['database']
    USER = lp.configs['user']
    PASSWORD = lp.configs['password']
    HOST = lp.configs['host']
    PORT = lp.configs['port']

    # Вводные данные для подключения к почтовому сервису.
    SMTP_SERVER = lp.configs['smtp_server']
    SMTP_PORT = lp.configs['smtp_port']
    EMAIL_FROM = lp.configs['email_from']
    EMAIL_PASSWORD =lp.configs['email_password']
    EMAIL_TO = lp.configs['email_to']
    # Запись об успешной загрузке необходимых данных для подключений.
    logging.info('Data for connection are loaded')
    message += '\nData for connection are loaded'

    # Подключение к базе данных.
    dt = DatabaseConnection(HOST, PORT, DATABASE, USER, PASSWORD)
    logging.info('Database connection is completed successfully.')
    message += '\nDatabase connection is completed successfully.'

    # Создание экземпляра класса для извлечения, агрегирования данных и записи их в базу.
    process = DataProcessing()
    # Вычисление длительности выбранного временного интервала.
    n_hours = (END - START).total_seconds() / 3600. # Длительность выбранного интервала в часах.

    # Разбиение выбранного временного интервала на часовые промежутки для уменьшения нагрузки.
    start_time = START
    while start_time < END:
        # Укорачивание длины промежутка, если она выходит за пределы выбранного интервала.
        end_time = END if END <= start_time + timedelta(hours=1) else start_time + timedelta(hours=1)

        # Выполнение запроса к API.
        logging.info(f'Sending a request for data in the range from {start_time} to {end_time}.')
        params = {'client':CLIENT,
                  'client_key':CLIENT_KEY,
                  'start':datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S.%f'),
                  'end':datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S.%f')}
        responce = requests.get(API_URL, params=params)
        logging.info('The request was completed successfully.')

        # Обработка полученных данных.
        for row in responce.json():
            process.processing(row)#, dt)
            query = '''
                insert into grades(
                    user_id,
                    oauth_consumer_key,
                    lis_result_sourcedid,
                    lis_outcome_service_url,
                    is_correct,
                    attempt_type,
                    created_at
                            )
                            values(%s, %s, %s, %s, %s, %s, %s)
                    '''
            # Запись данных в базу в соответствии с запросом.
            dt.post(query, process.values)

        # Запись об успешной загрузке данных за временной интервал в базу.
        logging.info('Data is successfully loaded into the database.')
        # Переход к новому временному интервалу.
        start_time = end_time

    # Отключение базы данных.
    dt.exit()
    logging.info('All data for the selected period has been loaded into the database.'
                 ' The connection to the database was interrupted.')
    message += '\nAll data for the selected period has been loaded into the database.'
    message += '\nThe connection to the database was interrupted.'

    # Расчеты на основании агрегированных данных.
    process.calculate(n_hours)
    logging.info('Data has been aggregated successfully.')
    message += '\nData has been aggregated successfully.'

    # Подключение к Google Таблицам.
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name("gs_credentials.json", scope)
    client = gspread.authorize(credentials)
    sheet = client.open("project214_result").sheet1
    logging.info('Connection to Google Sheets is complete.')
    message += '\nConnection to Google Sheets is complete.'

    # Запись агрегированных данных в гугл таблицу.
    sheet.append_row([datetime.strftime(START, '%Y-%m-%d %H:%M:%S.%f'),
                            datetime.strftime(END, '%Y-%m-%d %H:%M:%S.%f'),
                            process.n_id_uniq, process.n_all, process.n_submit,
                            process.n_correct, process.success_rate,
                            process.n_all_per_hour, process.n_submit_per_hour,
                            process.n_correct_per_hour, process.n_all_per_id_uniq,
                            process.n_submit_per_id_uniq, process.n_correct_per_id_uniq])
    logging.info('Aggregated data written to Google Sheets successfully.')
    message += '\nAggregated data written to Google Sheets successfully.'

    # Отправка информационного сообщения по электронной почте.
    message += f'''
        \nData loading for the period from {datetime.strftime(START, '%Y-%m-%d %H:%M:%S.%f')}\
        to {datetime.strftime(END, '%Y-%m-%d %H:%M:%S.%f')} completed successfully.
        '''
    send_email(message)
    logging.info('Email message sent.')


except AttributeError as err:
    logging.error(err, exc_info=True)

except requests.exceptions.HTTPError as err:
    logging.error(err, exc_info=True)
    # Отправка информационного сообщения по электронной почте.
    message += f'''
            \nProblems arose when connecting to the training system via API: \n\n{err}.
                '''
    send_email(message)
    logging.info('Email message sent.')

except Exception as err:
    logging.error(err, exc_info=True)
    # Отправка информационного сообщения по электронной почте.
    message += f'''
            \nAn error occurred while running the program: \n\n{err}.
            '''
    send_email(message)
    logging.info('Email message sent.')