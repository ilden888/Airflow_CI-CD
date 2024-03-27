from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from sqlalchemy import create_engine, Column, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import numpy as np
import random
import json
import requests
from io import StringIO
import asyncio
from requests.exceptions import ConnectionError
from time import sleep

default_args = {
    'owner': 'deniks',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 27),
}

dag = DAG(
    'ya_dir_up_Haval',
    default_args=default_args,
    description='DAG для загрузки данных из Yandex Direct каждые 4 часа. ТАНК',
    schedule_interval='0 */4 * * *',  # запуск каждые 4 часа, в 0 минут
    catchup=False
)

class Yandex_Direct_API:
    """
    Класс, представляющий API Yandex Direct, с методами для выполнения запросов и обработки ответов.
    """

    def get_headers(self, token,clientLogin):
        """
        Получение заголовков для API запроса.

        Аргументы:
        token (str): OAuth-токен.
        clientLogin (str): Логин клиента рекламного агентства.

        Возвращает:
        dict: Заголовки для запроса к API.
        """
        headers_direct = {
            # OAuth-токен.
            "Authorization": "Bearer " + token,
            # Логин клиента рекламного агентства
            "Client-Login": clientLogin,
            # Язык ответных сообщений
            "Accept-Language": "ru",
            # Режим формирования отчета
            "processingMode": "auto",
            # Формат денежных значений в отчете
            "returnMoneyInMicros": "false",
            #Не выводить в отчете строку с названием отчета и диапазоном дат
            "skipReportHeader": "true",
            # Не выводить в отчете строку с названиями полей
            # "skipColumnHeader": "true",
            # Не выводить в отчете строку с количеством строк статистики
            "skipReportSummary": "true"
        }
        return headers_direct

    #новая функция с лимитами
    def get_body(self,date1,date2,goalID):
        """
        Создание тела запроса.

        Аргументы:
        date1 (str): Дата начала отчетного периода.
        date2 (str): Дата окончания отчетного периода.
        fields_direct (list): Список полей для отчета.
        goalID (int): ID цели.

        Возвращает:
        str: Тело запроса в формате JSON.
        """
        # боди для директа
        reportNumber = random.randrange(1, 200000)
        # Создание тела запроса
        body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": date1,
                    "DateTo": date2
                },
                "Goals": goalID,
                "FieldNames": ['CampaignName', 'CampaignId', 'AdGroupName','Device','Date','Gender','TargetingLocationName','Impressions','Clicks','Cost','Conversions'],
                "ReportName": f'Отчет №{reportNumber}',
                "ReportType": "CUSTOM_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }

        # Кодирование тела запроса в JSON
        body = json.dumps(body, indent=4)
        return body


    def getReport(self,headers,body):
        """
        Получение отчета из API.

        Аргументы:
        headers (dict): Заголовки для запроса.
        body (str): Тело запроса в формате JSON.

        Возвращает:
        DataFrame: DataFrame pandas с данными отчета или None, если возникла ошибка.
        """
        URL = 'https://api.direct.yandex.com/json/v5/reports'
        retryIn = int(3)
        while True:
            try:
                req = requests.post(URL, body, headers=headers)
                req.encoding = 'utf-8'

                if req.status_code == 400:
                    print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди.\n")
                    print(f"JSON-код ответа сервера: \n{req.json()['error']['error_detail']}\n")
                    break

                elif req.status_code == 200:
                    print("Данные выгружены.\n")
                    df = pd.read_csv(StringIO(req.text), sep='\t')
                    return df

                elif req.status_code == 201:
                    print("Отчет успешно поставлен в очередь в режиме офлайн. \nЗагрузка может занять до 3-х минут.\n")
                    sleep(retryIn)

                elif req.status_code == 202:
                    sleep(retryIn)

                elif req.status_code == 500:
                    print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее.\n")
                    print(f"JSON-код ответа сервера: \n{req.json()['error']['error_detail']}\n")
                    break

                elif req.status_code == 502:
                    print("Время формирования отчета превысило серверное ограничение.")
                    print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.\n")
                    print(f"JSON-код ответа сервера: \n{req.json()['error']['error_detail']}\n")
                    break

                else:
                    break
            except ConnectionError:
                print('Ошибка соединения')
                break
            except Exception as e:
                print(e)
                break

    def normalize_Data(self, df):
        """
        Нормализация и очистка данных отчета.

        Аргументы:
        df (DataFrame): DataFrame pandas с данными отчета.

        Возвращает:
        DataFrame: Очищенный и нормализированный DataFrame pandas.
        """

        # получаем список колонок с конверсиями
        conversionsCols = []

        # меняем -- на нули и переводим в инт
        for col in df.columns:
          if 'Conversions' in col:
              df[col] = df[col].replace('--', '0').astype(int)
              conversionsCols.append(col)

        df.fillna('--', inplace=True)

        # создаем столбец, который будет суммировать все столбцы с conversions в названии
        df['Conversions'] = df[conversionsCols].sum(axis=1)

        # удаляем все столбцы с conversions в названии, кроме 'Conversions'
        for col in conversionsCols:
          if col != 'Conversions':
              df = df.drop(columns=[col])

        # переименовываем столбцы на русский язык
        df = df.rename(columns={
        'CampaignName': 'Название кампании',
        'CampaignId': 'ID',
        'AdGroupName': 'Название группы объявлений',
        'Device': 'Устройство',
        'Date': 'Дата',
        'Gender': 'Пол',
        'TargetingLocationName': 'ГЕО',
        'Impressions': 'Показы',
        'Clicks': 'Клики',
        'Cost': 'Расход',
        'Conversions': 'Конверсии'
        })

        print("Данные обработаны.")
        return df



def calculate_metrics(df):
    """
    Обработка данных и добавление столбцов CPC, CPA и CR.

    Аргументы:
    df (DataFrame): DataFrame pandas с данными отчета.

    Возвращает:
    DataFrame: DataFrame pandas с добавленными столбцами CPC, CPA и CR.
    """
    # Вычисление CPC
    df['CPC (руб.)'] = round(df['Расход'] / df['Клики'],2)

    # Вычисление CPA
    df['CPA (руб.)'] = round(df['Расход'] / df['Конверсии'])

    # Вычисление CR
    df['CR (%)'] = round((df['Конверсии'] / df['Клики']) * 100,2)

    # Заполнение бесконечных значений в результате деления на ноль
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df

def param_agg(df, param):
    # Расчет агрегированных значений по интересующим параметрам
    agg_values = df.groupby(param, as_index=False).agg({'Показы': 'sum', 'Клики': 'sum', 'Расход': 'sum', 'Конверсии': 'sum'})

    if param == 'Дата':
        agg_values = agg_values.sort_values('Дата')
    else:
        agg_values = agg_values.sort_values('Расход', ascending=False)

    agg_values = calculate_metrics(agg_values)  # Обновление агрегированного DataFrame с помощью функции calculate_metrics

    return agg_values

def main():
    token = 'y0_AgAAAAByc-21AAtswwAAAAD-TjezAAB3kd_REtVHeqDqPek69heLSWTEqA'
    clientLogin = 'ad-borishoftank'

    # Определение ID цели из раздела "Цели" Яндекс.Метрики
    goalID = [308966440, 308966441, 308966442, 308966443, 309176715, 317801667, 320651459, 321699286, 321706298]

    # Определение дат начала и окончания отчетного периода
    date1 = '2024-01-01'
    date2 = '2024-05-31'

    yandex_direct_api = Yandex_Direct_API()
    headers = yandex_direct_api.get_headers(token, clientLogin)
    body = yandex_direct_api.get_body(date1, date2, goalID)
    df = yandex_direct_api.getReport(headers, body)

    params = ['Название кампании', 'Название группы объявлений', 'Устройство', 'Дата', 'Пол', 'ГЕО']

    arrayDfs = []

    if df is not None:
        df = yandex_direct_api.normalize_Data(df)

        for param in params:
            arrayDfs.append(param_agg(df,param))

        # Вывод данных перед загрузкой в базу данных PostgreSQL
        print("Данные, готовые к загрузке в базу данных PostgreSQL:")
        print(arrayDfs)

        return arrayDfs

def load_data_to_postgresql(df):
    connection_params = {
        'user': 'test-ad',
        'password': 'cE4uT8aL9a',
        'host': '185.200.240.131',
        'database': 'test-ad',
        'port': '5432'
    }

    engine = create_engine(f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}")
    
    # Создание таблицы в базе данных PostgreSQL
    Base = declarative_base()

    class St2(Base):
        __tablename__ = 'Tank_Direct_day'
        Day = Column(Date, primary_key=True)
        Impressions = Column(Integer)
        Clicks = Column(Integer)
        Cost = Column(Float)

    Base.metadata.create_all(engine)

    # Вывод данных перед загрузкой в PostgreSQL
    print("Данные для загрузки в PostgreSQL:")
    print(df)

    # Загрузка данных в PostgreSQL
    df.to_sql(
        'Tank_Direct_day',
        engine,
        index=False,
        if_exists='replace',
        dtype={
            'Дата': Date,
            'Показы': Integer,
            'Клики': Integer,
            'Расход': Float,
            'Конверсии': Float,
            'CPC (руб.)': Float,
            'CPA (руб.)': Float,
            'CR (%)': Float
        }
    )

    print("Данные успешно загружены в таблицу Tank_Direct_day.")

def run_yandex_direct_update():
    main()

def run_yandex_direct_update():
    arrayDfs = main()
    if arrayDfs:
        # Выбираем нужный DataFrame для загрузки в PostgreSQL
        df_to_load = arrayDfs[3]  # Измените индекс, если требуется другой DataFrame
        load_data_to_postgresql(df_to_load)

run_yandex_direct_update_task = PythonOperator(
    task_id='ya_dir_up_Tank',
    python_callable=run_yandex_direct_update,
    dag=dag,
)

run_yandex_direct_update_task
