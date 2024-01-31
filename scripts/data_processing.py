import csv
import json

class Data:

    def __init__(self, path, data_type):
        self.path = path
        self.data_type = data_type
        self.data = self.read_data()
        self.columns_name = self.get_columns()
        self.num_lines = self.size_data()

    def read_json(self):
        json_data = []
        with open(self.path, 'r') as file:
            json_data = json.load(file)
        return json_data

    def read_csv(self):
        csv_data = []
        with open(self.path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                csv_data.append(row)
        return csv_data

    def read_data(self):
        data = []
        if self.data_type == 'csv':
            data = self.read_csv()
        elif self.data_type == 'json':
            data = self.read_json()
        elif self.data_type == 'list':
            data = self.path
            self.path = 'List in memory'
        
        return data
    
    def get_columns(self):
        return list(self.data[-1].keys())
    
    def rename_columns(self, key_mapping):
        new_data = []

        for old_dict in self.data:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_data.append(dict_temp)
        
        self.data =  new_data
        self.columns_name = self.get_columns()

    def size_data(self):
        return len(self.data)
    
    def join(dataA, dataB):

        combined_list = []
        combined_list.extend(dataA.data)
        combined_list.extend(dataB.data)
        return Data(combined_list, 'list')
    
    def transforming_data_table(self):
        combined_data_table = [self.columns_name]

        for row in self.data:
            line = []
            for column in self.columns_name:
                line.append(row.get(column, 'Unavailable'))
            combined_data_table.append(line)
        
        return combined_data_table
    
    def saving_data(self, path):

        combined_data_table = self.transforming_data_table()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(combined_data_table)