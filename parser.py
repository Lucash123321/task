import pymysql
import requests
import datetime

db = pymysql.connect(host='localhost', port=3306, user='root', password='root', database='legpromtest')
cursor = db.cursor()
session = requests.Session()


def parse_data_by_email():
    url = 'https://suggestions.dadata.ru/suggestions/api/4_1/rs/findByEmail/company'

    params = {}

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        "Authorization": "Token eb4289fd1a4ce046be01b420ebf2d407c38dddf4",
        "X-Secret": "8cc6d47b7ae8c04af691d1161d27e29615e38d1e"
    }

    is_source_exist = cursor.execute("SELECT name FROM `spr_istochnik` WHERE name='DaData'")

    if not is_source_exist:
        cursor.execute("INSERT INTO `spr_istochnik` (name) VALUES ('DaData')")
        cursor.execute("SELECT id FROM `spr_istochnik` WHERE name='DaData'")
        source_id = cursor.fetchone()[0]
    else:
        source_id = cursor.fetchone()[0]

    query = "SELECT key_company, email FROM `c_email`"
    cursor.execute(query)

    for key_company, email in cursor:
        params['query'] = email
        response = requests.get(url, params=params, headers=headers)
        response_data = response.json()
        if response_data['suggestions']:
            company_data = response_data['suggestions'][0]['data']['company']
        else:
            continue

        query = "SELECT id FROM `c_company` WHERE id=%s"
        is_company_exist_in_database = cursor.execute(query, (key_company,))

        if is_company_exist_in_database:
            query = "UPDATE c_company SET inn=%s, ogrn=%s, id_istochnik=%s WHERE id=%s"
            c_company_data = tuple(map(str, (company_data['inn'], company_data['ogrn'], source_id, key_company)))
        else:
            query = "INSERT INTO `c_company` (inn, ogrn, id, id_member, date_create, id_istochnik ) " \
                    "VALUES (%s, %s, %s, %s, %s, %s)"
            c_company_data = tuple(map(str, (company_data['inn'], company_data['ogrn'],
                                             key_company, 0, datetime.datetime.now(), source_id)))
        cursor.execute(query, c_company_data)

        query = "SELECT kode_region, kode_town FROM `spr_town` WHERE name=%s"
        is_town_exist_in_database = cursor.execute(query, (company_data['city'],))

        if is_town_exist_in_database:
            location_data = cursor.fetchone()

        else:
            cursor.execute("SELECT MAX(kode_town) AS max_kode_town FROM spr_town")
            max_kode_town = cursor.fetchone()[0]
            new_kode_town = str(int(max_kode_town) + 1)
            query = "INSERT INTO `spr_town` (kode_town, name) VALUES (%s, %s)"
            spr_town_data = tuple(map(str, (new_kode_town, company_data['city'])))
            cursor.execute(query, spr_town_data)
            cursor.execute("SELECT kode_region, kode_town FROM `spr_town` ORDER BY id DESC LIMIT 1")
            location_data = cursor.fetchone()

        kode_town = location_data[1]
        kode_region = location_data[0]

        query = "INSERT INTO `c_address` (id_town, id_gerion, key_company, data_create, id_istochnik) " \
                "VALUES (%s, %s %s, %s, %s)"
        c_address_data = tuple(map(str, (kode_town, kode_region, key_company,
                                             datetime.datetime.now(), source_id)))

        cursor.execute(query, c_address_data)

        query = "SELECT id FROM `spr_okved` WHERE kode_okved=%s"
        is_spr_okved_exist = cursor.execute(query, (company_data['okved'],))
        if is_spr_okved_exist:
            okved_id = cursor.fetchone()[0]


        else:
            query = "INSERT INTO `spr_okved` (kode_okved, name_okved) VALUES (%s, %s)"
            spr_okved_data = (company_data['okved'], company_data['okved_name'])
            cursor.execute(query, spr_okved_data)
            query = "SELECT id FROM `spr_okved` WHERE kode_okved=%s"
            okved_id = cursor.execute(query, (company_data['okved'], ))

        query = "SELECT id FROM `c_okved` WHERE key_company=%s"
        is_c_okved_exist = cursor.execute(query, (key_company,))

        if is_c_okved_exist:
            query = "UPDATE `c_okved` SET id_okved=%s, id_istochnik=%s"
            c_okved_data = (okved_id, source_id)
        else:
            query = "INSERT INTO `c_okved` (id_okved, id_istochnik, data_create) VALUES (%s, %s, %s)"
            c_okved_data = (okved_id, source_id, datetime.datetime.now())

        cursor.execute(query, c_okved_data)

        query = "SELECT id FROM `company` WHERE company.key=%s"
        is_company_exist = cursor.execute(query, (key_company,))

        if is_company_exist:
            query = "UPDATE `company` SET name=%s WHERE company.key=%s"
            c_company_data = (company_data['name'], key_company)
        else:
            query = "INSERT INTO `company` (company.key, name, create_date) VALUES (%s, %s, %s)"
            c_company_data = (key_company, company_data['name'], datetime.datetime.now())
        cursor.execute(query, c_company_data)

        if company_data.get('income'):
            query = "SELECT id FROM `c_finka` WHERE key_company=%s"
            is_finka_exist = cursor.execute(query, (key_company,))
            if is_finka_exist:
                query = "UPDATE `c_finka` SET fin_pribil=%s, id_istochnik WHERE key_company=%s"
                c_finka_data = map(str, (company_data['income'], source_id, key_company))
            else:
                query = "INSERT INTO `c_finka` (key_company, id_istochnik, data_create, fin_pribil) " \
                        "VALUES (%s, %s, %s, %s)"
                c_finka_data = tuple(map(str, (key_company, source_id,
                                               datetime.datetime.now(), company_data['income'])))
            cursor.execute(query, c_finka_data)

        cursor.commit()


if __name__ == '__main__':
    parse_data_by_email()
