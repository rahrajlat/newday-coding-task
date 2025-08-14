import json
import sys
import yaml
import json
from pathlib import Path
import yaml
import requests


def generate_model_documentation(sql_content: str):
    """

    :param sql_content: DBT SQL
    :return:
    """

    # URL for the Ollama server
    url = "http://localhost:11434/api/generate"
    # Input data (e.g., a text prompt)
    data = {
        "model": "llama3:latest",
        "stream": False,
        "prompt": f"""

        
                    You are given SQL code from a dbt model.
            Explain its overall purpose and logic using the following fixed format, every time.

            Format:
            Purpose:
            [One concise sentence summarizing what the SQL is doing]

            Logic Flow:
            [2–4 short bullet points explaining the main steps in order]

            Rules:
            - Use plain, simple language.
            - Do not include any Jinja templates or braces.
            - Do not include or render any Jinja templates in the explanation.
            - Do not add anything outside the Purpose and Logic Flow sections.
            - Keep the tone and style the same for every answer.

            SQL Code:
            {sql_content}
        
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


def llm_docs_generator(model_name: str, manifest_file: str, project: str, out_dir: str) -> str:
    """_summary_

    Args:
        model_name (str): Name of the DBT Model
        manifest_file (str): DBT Manifest File
        project (str): DBT Project Name
        out_dir (str): Output Dir

    Returns:
        str: AI Documentation
    """
    manifest_path = Path(manifest_file)
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest not found: {manifest_path}")

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    nodes = manifest.get("nodes", {})
    target_node_id = f"model.{project}.{model_name}"

    if target_node_id not in nodes:
        raise ValueError(f"Model node not found in manifest: {target_node_id}")

    node = nodes[target_node_id]
    m_name = target_node_id.split(".")[-1]          # model name
    # not used, but kept if needed
    schema = node.get("schema")
    sql = node.get("compiled_code", "")

    # Keep asking until the user accepts
    while True:
        llm_description = generate_model_documentation(sql_content=sql)

        print("\nGenerated Description for", m_name)
        print("----------------------------------")
        print(llm_description)
        print("----------------------------------")

        choice = input(
            "Are you happy with this description? (y/n): ").strip().lower()
        if choice == "y":
            break
        elif choice == "n":
            print("\nRegenerating...\n")
            continue
        else:
            print("Please type 'y' or 'n'.\n")

    # Prepare doc markdown and YAML (only after acceptance)
    doc_md = (
        "{% docs " + m_name + " %}\n\n"
        + llm_description + "\n"
        + "{% enddocs %}\n\n"
    )

    out_dir = Path(
        f"/Users/rahulrajasekharan/vscode_proj/newday-dbt/newday-coding-task/dbt/"
        f"dbt_newday_coding_assignment/models/{out_dir}/docs_mds"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    md_path = out_dir / f"{model_name}.md"
    yml_path = out_dir / f"{model_name}.yml"

    # Write docs markdown (strip accidental backticks or stray Jinja braces if you want)
    with open(md_path, "w", encoding="utf-8") as f:
        # keep Jinja docs tags; remove backticks only
        f.write(doc_md.replace("`", ""))

    # Write YAML pointing the model description to the doc
    yaml_content = {
        "version": 2,
        "models": [
            {
                "name": m_name,
                "description": f'{{{{ doc("{m_name}") }}}}'
            }
        ]
    }
    with open(yml_path, "w", encoding="utf-8") as f:
        yaml.dump(yaml_content, f, sort_keys=False)

    print(f"\n✅ Docs written:\n- {md_path}\n- {yml_path}")
    return llm_description


if __name__ == "__main__":
    print("Running")
    model_name = sys.argv[1]
    out_dir = sys.argv[2]
    dbt_project = 'dbt_newday_coding_assignment'
    dbt_manifest = "/Users/rahulrajasekharan/vscode_proj/newday-dbt/newday-coding-task/dbt/dbt_newday_coding_assignment/target/manifest.json"

    llm_docs_generator(model_name=model_name,
                       manifest_file=dbt_manifest, project=dbt_project, out_dir=out_dir)
