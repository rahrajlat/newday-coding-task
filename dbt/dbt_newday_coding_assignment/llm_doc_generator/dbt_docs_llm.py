import json
import sys
import yaml
import collections
from dbt_enhanced_documentation import generate_model_documentation

with open(
        '/Users/rahulrajasekharan/vscode_proj/newday-dbt/newday-coding-task/dbt/dbt_newday_coding_assignment/target/manifest.json',
        "r") as f:
    manifest = json.load(f)
nodes = manifest['nodes']


# print(dict_file)


# with open(r'data.yml', 'w') as file:
# documents = yaml.dump(dict_file, file,sort_keys=False)
# sys.exit()
model_docs_list = []
for node in nodes:
    if 'seed' not in node:
        m_name = node[::-1].split(".")[0][::-1]
        print(m_name)
        schema = manifest['nodes'][node]['schema']
        sql = manifest['nodes'][node]['raw_sql']
        # model_docs_list.append({'name':m_name,'description':'{{ doc("'+m_name+'") }}'})
        llm_description = generate_model_documentation(sql_content=sql)
        # model_docs_list.append({'name':m_name,'description':f'{llm_description}'})

        doc_md = '{% docs ' + m_name + ' %}' + '\n' + '\n' + \
            llm_description + '\n' + '{% enddocs %}' + '\n' + '\n'

        with open(
                f'/Users/rahulrajasekharan/vscode_proj/newday-dbt/newday-coding-task/dbt/dbt_newday_coding_assignment/dbt_docs/docs_md/dbt_docs.md',
                'a', encoding='utf-8') as f:
            f.write(doc_md.replace(
                "}}", "").replace("{{", '').replace("`", ""))

    # with open('result.yml', 'w') as yaml_file:
        # yaml.dump(article_info, yaml_file, default_flow_style=False)

    # sys.exit()

dict_file = {
    "version": 2,
    "models": model_docs_list

}
with open(r'data.yml', 'w') as file:
    documents = yaml.dump(dict_file, file, sort_keys=False)
