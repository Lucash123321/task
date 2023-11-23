import "./App.css";
import React, {useState, useEffect} from "react";

function App() {
    const url = "http://127.0.0.1:3333/companies-by-email/";
    const [companiesDataByEmail, setData] = useState([]);

    const fetchCompaniesDataByEmail = () => {
        return fetch(url)
            .then((response) => response.json())
            .then((data) => setData(data))
    }


    useEffect(() => {
        fetchCompaniesDataByEmail();
    }, []);

    return (
        <div>
            {companiesDataByEmail.map((companyDataByEmail, index) => {
                return (
                    <tr>
                        <td>{companyDataByEmail.email}</td>
                        <td>{companyDataByEmail.inn}</td>
                        <td>{companyDataByEmail.ogrn}</td>
                        <td>{companyDataByEmail.okved_kode}</td>
                        <td>{companyDataByEmail.okved_name}</td>
                        <td>{companyDataByEmail.fin_pribil}</td>
                        <td>{companyDataByEmail.city}</td>
                        <td>{companyDataByEmail.name}</td>
                    </tr>
                );
            })}
        </div>
    );
}

export default App;