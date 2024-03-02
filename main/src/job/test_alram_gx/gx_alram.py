import pandas as pd
import yaml
import json
import great_expectations as ge

# quote_av
# '{"Open_time":1709281394000,"Open":61372.02,"High":61372.02,"Low":61372.01,"Close":61372.02,"Volume":0.02984,"quote_av":1831.3410668,"trades":5,"tb_base_av":0.02884,"tb_quote_av":1769.9690568,"Symbol":"BTCUSDT"}'

def validate(value):
    with open("/opt/flink/main/src/job/test_alram_gx/rule.yaml", 'r') as file:
        expectations_config = yaml.safe_load(file)
    
    data_dict = eval(value)
    df = pd.DataFrame([data_dict])



    columns = []
    for col_list in expectations_config["expectations"]:
        print("columns : ", col_list["kwargs"]["column"] )
        columns.append(col_list["kwargs"]["column"])
    
    if all(column in df.columns for column in columns):
        ge_df = ge.dataset.PandasDataset(df)
        
        for expectation in expectations_config["expectations"]:
            expectation_method = getattr(ge_df, expectation["expectation_type"])
            expectation_method(**expectation["kwargs"])

        results = ge_df.validate()

        for i in results["results"]:
            print(i["result"])

        # {'element_count': 1, 'missing_count': 0, 'missing_percent': 0.0, 'unexpected_count': 1, 'unexpected_percent': 100.0, 'unexpected_percent_total': 100.0, 'unexpected_percent_nonmissing': 100.0, 'partial_unexpected_list': [1831.3410668]}
        # {'element_count': 1, 'missing_count': 0, 'missing_percent': 0.0, 'unexpected_count': 1, 'unexpected_percent': 100.0, 'unexpected_percent_total': 100.0, 'unexpected_percent_nonmissing': 100.0, 'partial_unexpected_list': [61372.02]}

        return results