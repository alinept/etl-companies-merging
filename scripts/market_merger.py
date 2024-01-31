from data_processing import Data

#starting to read data
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

#Extract
companyA_data = Data(path_json, 'json')
print(companyA_data.columns_name)
print(companyA_data.num_lines)

companyB_data = Data(path_csv, 'csv')
print(companyB_data.columns_name)
print(companyB_data.num_lines)

#Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

companyB_data.rename_columns(key_mapping)
print(companyB_data.columns_name)

data_merger = Data.join(companyA_data, companyB_data)
print(data_merger.columns_name)

#Load

combined_data_path = 'data_processed/combined_data.csv'
data_merger.saving_data(combined_data_path)






