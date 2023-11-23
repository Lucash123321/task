from typing import List
from fastapi import FastAPI
import pymysql
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

db = pymysql.connect(host='localhost', port=3306, user='root', password='root', database='legpromtest')
cursor = db.cursor()


app = FastAPI()

origins = [
    "http://localhost:3000",
    "localhost:3000"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)



@app.get("/companies-by-email/")
async def create_item():
    query = "SELECT c_email.email, c_company.inn, c_company.ogrn, spr_okved.kode_okved, " \
            "spr_okved.name_okved, c_finka.fin_pribil, spr_town.name, company.name FROM `c_email` " \
            "LEFT JOIN `c_company` ON c_email.key_company = c_company.id " \
            "LEFT JOIN `c_okved` ON c_email.key_company = c_okved.key_company " \
            "JOIN `spr_okved` ON c_okved.id_okved = spr_okved.id " \
            "LEFT JOIN `c_finka` ON c_email.key_company = c_finka.key_company " \
            "LEFT JOIN `c_address` ON c_email.key_company = c_address.key_company " \
            "JOIN `spr_town` ON c_address.id_town = spr_town.kode_town " \
            "LEFT JOIN `company` ON c_email.key_company = company.key"
    cursor.execute(query)
    companies_data_by_email = cursor.fetchall()
    response_data = []
    for company_data in companies_data_by_email:
        response_data.append(
            {
                "email": company_data[0],
                "inn": company_data[1],
                "ogrn": company_data[2],
                "okved_kode": company_data[3],
                "okved_name": company_data[4],
                "fin_pribil": company_data[5],
                "city": company_data[6],
                "name": company_data[7]
            }
        )
    return response_data

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=3333)
