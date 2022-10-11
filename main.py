import psycopg2
import config
import csv

list_ads = []
dict_author = {}
dict_address = {}
connection = psycopg2.connect(host=config.host, user=config.user, password=config.password, database=config.db_name)
connection.autocommit = True

def parsing_csv():
    with open('ads.csv', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',', skipinitialspace=True)
        for row in reader:
            list_ads.append(row)
        list_author = []
        list_address = []
        for ads in list_ads:
            list_author.append(ads['author'])
            list_address.append(ads['address'])

        list_author = list(set(list_author))
        list_address = list(set(list_address))

        for i in range(len(list_author)):
            dict_author[list_author[i]] = i + 1
        for i in range(len(list_address)):
            dict_address[list_address[i]] = i + 1

def INSERT_INTO_ads_author():
    try:


        with connection.cursor() as cursor:
            for aut in dict_author:
                cursor.execute(f"""
                INSERT INTO ads_author (author) VALUES ('{aut}')
                """)
            print(cursor.fetchone())


    except Exception as ex:
        print(ex)
    finally:
        if connection:
            connection.close()

def INSERT_INTO_ads_address():
    try:


        with connection.cursor() as cursor:
            for aut in dict_address:
                cursor.execute(f"""
                INSERT INTO ads_address (address) VALUES ('{aut}')
                """)
            print(cursor.fetchone())


    except Exception as ex:
        print(ex)
    finally:
        if connection:
            connection.close()

def INSERT_INTO_ads():
    try:


        with connection.cursor() as cursor:
            for ads in list_ads:
                cursor.execute(f"""
                INSERT INTO ads (name, fk_author, price, description, fk_address, is_published)
                 VALUES ('{ads['name']}','{dict_author[ads['author']]}','{ads['price']}','{ads['description']}','{dict_address[ads['address']]}','{ads['is_published']}')
                """)

            print(cursor.fetchone())

    except Exception as ex:
        print(ex)
    finally:
        if connection:
            connection.close()

parsing_csv()
INSERT_INTO_ads_author()
INSERT_INTO_ads_address()
INSERT_INTO_ads()