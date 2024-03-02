import pandas as pd

import time


value = '{"Open_time":1709281394000,"Open":61372.02,"High":61372.02,"Low":61372.01,"Close":61372.02,"Volume":0.02984,"quote_av":1831.3410668,"trades":5,"tb_base_av":0.02884,"tb_quote_av":1769.9690568,"Symbol":"BTCUSDT"}'
data_dict = eval(value)

df = pd.DataFrame([data_dict])

print(df)


import yaml
import great_expectations as ge

# YAML 파일에서 검증 규칙 불러오기
with open("/opt/flink/main/src/job/test_alram_gx/rule.yaml", 'r') as file:
    expectations_config = yaml.safe_load(file)


columns = []
for i in expectations_config["expectations"]:
    print("columns : ", i["kwargs"]["column"] )
    columns.append(i["kwargs"]["column"])

print("pd_columns: ", df.columns)    




if all(column in df.columns for column in columns):
# pandas DataFrame을 Great Expectations DataFrame으로 변환
    ge_df = ge.dataset.PandasDataset(df)

    # YAML 파일에서 불러온 검증 규칙을 적용
    for expectation in expectations_config["expectations"]:
        expectation_method = getattr(ge_df, expectation["expectation_type"])
        expectation_method(**expectation["kwargs"])


    s = time.time()
    # 검증 실행
    results = ge_df.validate()

    
    print(results)
    # # 결과 출력
    for i in results["results"]:
        print(i["result"])