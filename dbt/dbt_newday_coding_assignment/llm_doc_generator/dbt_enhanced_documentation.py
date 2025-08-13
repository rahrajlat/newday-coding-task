import requests
import json


def generate_model_documentation(sql_content: str):
    """

    :param sql_content:
    :return:
    """

    # URL for the Ollama server
    url = "http://localhost:11434/api/generate"
    # Input data (e.g., a text prompt)
    data = {
        "model": "llama3.1",
        "stream": False,
        "prompt": f"""
        
        Print your understanding of the following sql code:\n{sql_content}\n" which is written in DBT.The text must be
         simple to understand the overall flow of the code.Do not add anything else in the output. Also do not print any JINJA templates in the
         output."
        
        
        """,
    }

    # Make a POST request to the server
    response = requests.post(url, json=data)

    # Check if the request was successful
    if response.status_code == 200:
        # Process the response
        response_text = response.text

        # Convert each line to json
        response_lines = response_text.splitlines()
        response_json = [json.loads(line) for line in response_lines]
        for line in response_json:
            # Print the response. No line break
            return line["response"]
    else:
        print("Error:", response.status_code)


if __name__ == "__main__":
    sql_text = open(
        '/Users/rlek/PycharmProjects/dlg-repos/datp-dap-claims-airflow/dags/claims/dbt/dbt_claims_mart_home_cc10/model/staging/stg_gw_cc10_cc_activity.sql',
        'r').read()

    print(sql_text)

    print(generate_model_documentation(sql_content=sql_text))
