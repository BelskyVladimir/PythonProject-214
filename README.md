### Название проекта
ETL обработка данных об обучении студентов компании на сторонней образовательной платформе.

### Описание проекта
Приложение предназначено для:
- получения по API данных о действиях, выполняемых студентами на образовательной платформе, за произвольно выбранный промежуток времени;
- загрузки полученных первичных данных в развернутую на локальном компьютере базу данных;
- вывода агрегированных за выбранный промежуток времени данных в виде строки таблицы в файл Google Sheets, доступный для просмотра по ссылке заинтересованным лицам;
- записи информации об основных этапах выполнения обработки данных в лог-файл с указанием даты и времени;
- удаления лог-файлов старше трех дней;
- отправки на электронную почту сообщения о результатах выполнения загрузки.

### Инструкции и примеры
Для выполнения работ по проекту использовались:
- Python 3.13
- PostgreSQL 16

Исходные параметры задаются в виде словаря в файле params.txt, находящемся в одной директории с проектом:
- параметры для подключения к API обучающей системы: api_url, client, client_key
- начало (start) и конец (end) временного интервала
- параметры для подключения к базе данных: user, password, host, port
- параметры для подключения к SMTP серверу: smtp_server, smtp_port, email_from,
 email_password, email_to.
Образец оформления параметров приведен в файле  params_example.txt.

Учетные данные для подключения к API Google Диска и Google Таблиц содержатся в файле gs_credentials.json, находящемся в одной директории с проектом.

На локальном компьютере создана база данных PostgreSQL, содержащая одну таблицу  с полями:
- id serial 			primary key		уникальный номер записи
- user_id 			varchar(40)		уникальный номер студента
- oauth_consumer_key	 varchar(30) 		уникальный токен компании
- lis_result_sourcedid 	varchar(200)		ссылка на блок, в котором находится задача в LMS
- lis_outcome_service_url 	varchar(250)		URL адрес LMS компании
- is_correct 			smallint		была ли попытка решения верной (null если run)
- attempt_type		varchar(10)		тип решения (run или submit)
- created_at 			varchar(30)		дата и время попытки
Код для создания таблицы приведен в файле create_table.sql.

В приложении использованы стандартные библиотеки: os, logging, requests, gspread, smtplib, ssl, datetime, glob, ast, json, psycopg2, re, email.message, oauth2client.service_account.

Для улучшения читаемости кода были созданы и вынесены в отдельные файлы 
- Класс LoadParams для загрузки исходных параметров и проверки их на корректность.
- Класс DatabaseConnection для работы с базой данных.
- Класс DataProcessing для обработки и агрегирования загруженной строки данных.

### Результат загрузки агрегированных данных в Google таблицу
https://docs.google.com/spreadsheets/d/1dtEQpmxO_N10mXofr-WPKDEaXbASyxHao4R0BiIjUwI/edit?usp=drive_link

### Пример сообщения по электронной почте
![2025-03-03_22-25-57](https://github.com/user-attachments/assets/8357ceb6-0a10-4881-96a0-5b59fae01450)




