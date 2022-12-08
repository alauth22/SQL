
#python 3 already has sqlite3 buitl in so we should use this. 
from sqlite3 import connect

#connection will be used to connect to a database. 
connection = connect("store_transactions_test.db")

#cursor will allow us to interact with our data within our database. 
cursor = connection.cursor()

#create a stores table 
command_01 = '''

CREATE TABLE IF NOT EXISTS stores(
    store_id INTEGER PRIMARY KEY, 
    location TEXT NOT NULL
);
'''
cursor.execute(command_01)

#create a purchases table 
command_02 = '''
CREATE TABLE IF NOT EXISTS purchases(
    purchase_id INTEGER PRIMARY KEY, 
    store_id INTEGER, 
    total_cost FLOAT, 
    FOREIGN KEY (store_id) REFERENCES stores (store_id)
);
'''
cursor.execute(command_02)

#add three rows to our store table. 
cursor.execute("INSERT INTO stores VALUES (21, 'Green Bay, WI')")
cursor.execute("INSERT INTO stores VALUES (39, 'Chicago, IL')")
cursor.execute("INSERT INTO stores VALUES (65, 'Iowa City, IA')")

#add to rows to our purchase table. 
cursor.execute("INSERT INTO purchases VALUES (52, 19, 56.99)")
cursor.execute("INSERT INTO purchases VALUES (12, 77, 34.75)")

#get results 
cursor.execute("SELECT * FROM stores")
results = cursor.fetchall()
print(results)

#Sparse your data and feed into your database. 
with open("worm_genome.gff3") as worm_genome_file:
    #going through each line in the file. 
    for line in worm_genome_file:
        #disregard the lines beginning with #'s.
        if not line.startswith("#"):
            #split the lines by tab. Not sure about strip
            #so maybe strip is not needed/extra.
            line_elems = line.strip().split("\t")
            #assign each column in my features table to the respective input from the file.
            seq_id = line_elems[0]
            source = line_elems[1]
            type_id = line_elems[2]
            start = line_elems[3]
#             start = int(line_elems[3])
#             if line_elems[3] == ".":
#                 start = -1
#             else: 
#                 start = int(line_elems[3])
            end = line_elems[4]
            score = line_elems[5]
#             if line_elems[5] == ".":
#                 score = "NULL"
#             else:
#                 score = float(line_elems[5])
            strand = line_elems[6]
            phase = line_elems[7]
            #setting all these variables equal to one variable. 
            features_data = (seq_id, source, type_id, start, end, score, strand, phase)
            #SQL to insert features_data into features table. Insert command above.
            cursor.execute(insert_features, features_data)
            
            #Using lastrowid to return the value generated from the autoincrement 
            #column in the features table. Needed because we need feature_id to be included in the attributes table. 
            feature_id = cursor.lastrowid
            #take the last element in the line_elems list. That will contain the data for attributes. 
            attr_str = line_elems[-1]
            #go through each pair of attributes and split by ";".
            for pair in attr_str.split(";"):
                #splitting the pair by name and value_name
                name, values = pair.split("=")
                #go through each element in the value list and split by ",".
                for value_name in values.split(","):
                    attr_name = name 
                    value = value_name
                    attributes_data = (feature_id, attr_name, value)
                    cursor.execute(insert_attributes, attributes_data)
                    
                    # break 

#print out what you see
command_03 = "SELECT * FROM purchases LIMIT 15"
cursor.execute(command_03)
cursor.fetchall()

#do the rest of the queries here. 



