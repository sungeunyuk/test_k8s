import re
import networkx as nx
import matplotlib.pyplot as plt


def parse_sql_query_with_create_table(sql_query):
    graph_data = {'nodes': set(), 'edges': set()}
    
    queries = sql_query.strip().split(";")
    
    for query in queries:
        if not query.strip():
            continue  # Skip if the query is empty
    
    for sql_query in queries:
        create_table_match = re.search(r"create table\s+([\w\.]+)", sql_query, re.IGNORECASE)
        target_table = None
        if create_table_match:
            target_table = create_table_match.group(1).strip()
            graph_data['nodes'].add(target_table)
        
        
        with_clauses = re.findall(r"with\s+([\w\W]+?)select", sql_query, re.IGNORECASE)
        if with_clauses:
            for with_clause in with_clauses[0].split("),"):
                with_name = with_clause.strip().split(" as ")[0]
                graph_data['nodes'].add(with_name.strip())
                if target_table:
                    graph_data['edges'].add((with_name.strip(), target_table))

        
        main_query_tables = re.findall(r"from\s+([\w\.]+)|join\s+([\w\.]+)", sql_query, re.IGNORECASE)
        for tables in main_query_tables:
            for table in tables:
                if table:
                    graph_data['nodes'].add(table.strip())
                    if target_table:
                        graph_data['edges'].add((table.strip(), target_table))

        
        with_references = re.findall(r"(?<=\s)(\w+)(?=\.)", sql_query, re.IGNORECASE)
        for ref in with_references:
            if ref.strip() in graph_data['nodes']:
                graph_data['edges'].add((ref.strip(), target_table if target_table else "Main Select"))

    return graph_data





# 쿼리 문자열 예시
sql_query_example = """
create table source1 stored as parquet as
select * from cdp.dep_tx_l as tx
left join mncd on (tx.cstno = mncd.cstno)
left join cust on (tx.cstno = cust.cstno)
left join chcr on (tx.chcr_card_no = chcr.card_no);
create table target stored as parquet as
select * 
from source1 z
left join sub_table1 on (z.cstno = sub_table1.cstno)
left join sub_table2 on (z.cstno = sub_table2.cstno)
left join sub_table3 on (z.chcr_card_no = sub_table3.card_no)
"""

sql_query_example2 = """
create table source1 stored as parquet as
select * from cdp.dep_tx_l as tx
left join mncd on (tx.cstno = mncd.cstno)
left join cust on (tx.cstno = cust.cstno)
left join chcr on (tx.chcr_card_no = chcr.card_no);
create table target stored as parquet as
select * 
from source1 z
left join (select * from mk_test) as sub_table1 on (z.cstno = sub_table1.cstno)
left join sub_table2 on (z.cstno = sub_table2.cstno)
left join sub_table3 on (z.chcr_card_no = sub_table3.card_no)
"""


# sql_file_path = 'test.sql'
# with open(sql_file_path, 'r') as file:
#     sql_content = file.read()
#     print(sql_content)


all_graph_data = parse_sql_query_with_create_table(sql_query_example2)

G_with_create = nx.DiGraph()
G_with_create.add_nodes_from(all_graph_data['nodes'])
G_with_create.add_edges_from(all_graph_data['edges'])


plt.figure(figsize=(24, 16))
pos = nx.spring_layout(G_with_create, seed=42)  # Calculate positions for all nodes

# # Draw nodes with specific color and size
# nx.draw_networkx_nodes(G_with_create, pos, node_size=1500, node_color="#A0CBE2")

# # Draw edges with specific style
# nx.draw_networkx_edges(G_with_create, pos, arrows=True, arrowstyle="->", arrowsize=20, edge_color="gray")

# # Draw node labels with specific font size
# nx.draw_networkx_labels(G_with_create, pos, font_size=10)


# plt.figure(figsize=(20, 18))
# pos_with_create = nx.spring_layout(G_with_create)
nx.draw(G_with_create, pos, with_labels=True, node_size=2000, node_color="lightgreen", font_size=12, arrows=True)
plt.title("SQL Query Structure Visualization with Target Table")
plt.savefig('graph.png')
#plt.show()
