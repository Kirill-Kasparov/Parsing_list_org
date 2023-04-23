import requests    # by Kirill Kasparov, 2023
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

# из-за отсутствия прокси для https, сайт блокирует работу после 50 ИНН

def main_info(tables, inn):
    dict_main = {'ИНН': inn}
    for table in tables:
        tr_tags = table.find_all('tr')
        for tr in tr_tags:  # дробим таблицу на строки
            tr_list = []  # наши строки
            td_tags = tr.find_all('td')
            for td in td_tags:
                tr_list.append(td.text)
            # структурируем и собираем списки, если есть пары
            if len(tr_list) == 2:  # собираем в таблицй
                dict_main[tr_list[0]] = tr_list[1]
    main_info_df = pd.DataFrame(dict_main, index=[inn])
    return main_info_df
def other_info(main_info_df, tables_2):
    full_tr_list = []
    vd_mark = 0
    for table in tables_2:
        tr_tags = table.find_all('tr')
        for tr in tr_tags:  # дробим таблицу на строки
            tr_list = []  # наши строки
            td_tags = tr.find_all('td')
            for td in td_tags:
                tr_list.append(td.text)
            full_tr_list.append(tr_list)
            if len(tr_list) == 2 and len(tr_list[0]) < 9 and vd_mark == 0:
                main_info_df['ВД по ЕГРЮЛ'] = tr_list[1]
                vd_mark += 1    # костыль
                break
            if tr_list[0] == 'Ф1.1150' and len(tr_list) == 4:
                main_info_df['Основные средства, тыс.руб.'] = tr_list[2]
            if tr_list[0] == 'Ф1.1210' and len(tr_list) == 4:
                main_info_df['Запасы, тыс.руб.'] = tr_list[2]
            if tr_list[0] == 'Ф1.1200' and len(tr_list) == 4:
                main_info_df['Оборотные активы, тыс.руб.'] = tr_list[2]
            if tr_list[0] == 'Ф2.2110' and len(tr_list) == 4:
                main_info_df['Выручка, тыс.руб.'] = tr_list[2]
            if tr_list[0] == 'Ф2.2120' and len(tr_list) == 4:
                main_info_df['Себестоимость продаж, тыс.руб.'] = tr_list[2]
            if tr_list[0] == 'Ф2.2100' and len(tr_list) == 4:
                main_info_df['Валовая прибыль, тыс.руб.'] = tr_list[2]
            if tr_list[0] == 'Ф2.2200' and len(tr_list) == 4:
                main_info_df['Прибыль от продаж, тыс.руб.'] = tr_list[2]
            if tr_list[0] == 'Ф2.2400' and len(tr_list) == 4:
                main_info_df['Чистая прибыль, тыс.руб.'] = tr_list[2]

    return main_info_df
def get_free_proxies():
    url = "https://free-proxy-list.net/"
    soup = BeautifulSoup(requests.get(url).content, "html.parser").text
    proxy = soup.split('\n')[3:-1]
    return proxy[:50]
def get_proxies_https_csv():
    # http://free-proxy.cz/en/proxylist/country/all/https/ping/all
    proxies = pd.read_csv('proxy_ip.csv', sep=';', encoding='windows-1251', dtype='unicode', nrows=1000)
    return list(proxies['ip'])
def get_random_user_agent():
    #url = 'https://user-agents.net/random'
    url = 'https://useragents.io/random'
    soup = BeautifulSoup(requests.get(url).content, "html.parser").text
    useragent = soup.split('\n')[56:-17]
    #print(useragent)
    #print(len(useragent))
    return useragent

start = time.time()    # для таймера выполнения
#head = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
#       ' Chrome/86.0.4240.185 YaBrowser/20.11.2.78 Yowser/2.5 Safari/537.36', 'accept': '*/*'}

inn_list_df = pd.read_csv('inn_list.csv', sep=';', encoding='windows-1251', dtype='unicode', nrows=50)
inn_list = list(inn_list_df['ИНН'])
#inn_list = ['1644040195', '5835094907']    # 1644040195   5835094907
inn_df = pd.DataFrame()

count = 0
bad_count = 0
proxy = ''

for inn in inn_list:
    url = 'https://www.list-org.com/search?val=' + str(inn)

    # меняем Прокси каждые 10 запросов
    if count % 10 == 0 or count == 0:
        proxy = {"http": random.choice(get_free_proxies())}

    # меняет маску user-agent каждый запрос
    head = {'user-agent': random.choice(get_random_user_agent())}
    r = requests.get(url, headers=head, proxies=proxy)
    time.sleep(3)

    # ищем id страницы
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        inn_id = soup.findAll('input', class_='form-check-input')
        id = ''
        if len(inn_id) > 1:
            for i in str(inn_id[0]):
                if i.isdigit():
                    id += str(i)
        else:
            bad_count += 1
            print(inn, 'не найден')

        # получили id, теперь парсим саму страницу компании
        url = 'https://www.list-org.com/company/' + str(id)
        r = requests.get(url, headers=head, proxies=proxy)
        time.sleep(5)

        if r.status_code == 200:
            soup = BeautifulSoup(r.content, 'html.parser')

            # собираем основные данные по ИНН

            tables = soup.findAll('table', class_='table table-sm')
            main_info_df = main_info(tables, inn)

            tables_2 = soup.findAll('div', class_='card w-100 p-1 p-lg-3 mt-2')
            main_info_df = other_info(main_info_df, tables_2)

            inn_df = pd.concat([inn_df, main_info_df])    # рабочая, есть дубли
            count += 1
            print(count)
            if count % 50 == 0:
                end = time.time()
                while True:  # проверка, если файл открыт
                    try:
                        inn_df.to_csv('export.csv', sep=';', encoding='windows-1251', index=False, header=True)
                        end = time.time()
                        print('Загружено строк:', count, "|  Время выполнения:", end - start,
                              '|  Итоговые данные сохранены в файл export.csv')
                        break
                    except IOError:  # PermissionError
                        input('Необходимо закрыть файл inn_list.csv перед сохранением данных')

full_df = inn_list_df.merge(inn_df, how='outer', on='ИНН')
#full_df = inn_df

while True:  # проверка, если файл открыт
    try:
        full_df.to_csv('export.csv', sep=';', encoding='windows-1251', index=False, header=True)
        end = time.time()
        print('Загружено строк:', count, '|  Ошибок', bad_count,"|  Время выполнения:", end-start, '|  Итоговые данные сохранены в файл export.csv')
        break
    except IOError:    # PermissionError
        input('Необходимо закрыть файл inn_list.csv перед сохранением данных')