# ROHINI KUKKA - 12618604
#PAVAN SAI TIRUMALASETTI - 12616379

import pandas as pd
import csv
import re
from itertools import combinations

# Function to check if the column is of INT Datatype
def is_integer(attr):
    try:
        int(attr)
        return True # Attribute of INT type
    except ValueError:
        return False # Not an INT Type

# Function to check if the column is of VARCHAR Datatype
def is_alphanumeric(attr):
    return bool(re.match("^[.a-zA-Z0-9 {},]*$", attr)) # True if Attribute of a VARCHAR type else False

# Function to check if the column is of Date Datatype
def is_date(attr):
    try:
        pd.to_datetime(attr)
        # DATE type attribute
        return True 
    except ValueError:
        # Not a DATE type attribute
        return False 

# Function determining the datatypes of each column
def check_datatypes(csv_filePath):
    import csv
    with open(f'{csv_filePath}', mode ='r')as file:
        csvFile = csv.reader(file)
        header = next(csvFile)
        row1 = next(csvFile)
        f=0
        data_types = {} # Dictionary type variable to store the datatype of each column
        for i,j in zip(header, row1):
            # if(f==0):
            #     i = i[3:]
            #     f = 1
            if(is_integer(j)):
                data_types[i] = "INT"
            elif(is_alphanumeric(j)):
                data_types[i] = "VARCHAR(100)"
            elif(is_date(j)):
                data_types[i] = "DATE"
            else:
                data_types[i] = "VARCHAR(50)"
    cleaned_data = {key.replace('\ufeff', '', 1): value for key, value in data_types.items()}
    return cleaned_data

def convert_to_1NF(csv_filePath):
    # global newRelations
    count = 1

    # Read the CSV file
    df = pd.read_csv(csv_filePath)

    # Prepare to store the new relations
    newRelations = {}

    # Identify non-atomic columns (those containing lists of values)
    non_atomic_columns = []
    for column in df.columns:
        if df[column].apply(lambda x: isinstance(x, str) and (',' in x or '{' in x)).any():
            non_atomic_columns.append(column)

    # Create the base relation for atomic values
    atomic_relation = df.drop(columns=non_atomic_columns)
    newRelations['Candidate'] = atomic_relation.columns.tolist()
    
    # Print and write the atomic relation
    with open(r'/Users/rohinik/Desktop/output.txt', 'a') as file:
        file.write(f"\nNew Relation: {'Candidate'}\n{atomic_relation.to_string(index=False)}\n")
    count += 1

    # Create tables for each non-atomic column
    for column in non_atomic_columns:
        # Explode the non-atomic column to atomic values
        exploded_relation = df.explode(column)[[*primaryKey, column]].dropna().reset_index(drop=True)
        # Ensure atomic values (removing any set-like structures)
        exploded_relation[column] = exploded_relation[column].apply(lambda x: x.strip('{}').split(',') if isinstance(x, str) else x)

        # Flatten the exploded values into separate rows
        if exploded_relation[column].dtype == 'object':
            exploded_relation = exploded_relation.explode(column)
        
        newRelations[column] = [*primaryKey, column]

        # Print and write the new relation
        with open(r'/Users/rohinik/Desktop/output.txt', 'a') as file:
            file.write(f"\nNew Relation: {count}\n{exploded_relation.to_string(index=False)}\n")
        
        count += 1
    
    return newRelations

def convert_to_2NF(tables, FD, primaryKey):
    newTables = {}
    count = 1
    for fd in FD:
        x = fd.split("-->")
        lhs_fd = [i.strip() for i in x[0].split(',')]
        is_subset = set(lhs_fd).issubset(set(primaryKey))    
        if(is_subset and len(lhs_fd)!=len(primaryKey)):
            rhs_fd = [i.strip() for i in x[1].split(',')]
            t = lhs_fd + rhs_fd
            newTables[count] = t
            count+=1
    
    for tname,v in tables.items():
        value = []
        all_rhs = []
        for fd in FD:
            x = fd.split("-->")
            lhs_fd = [i.strip() for i in x[0].split(',')]
            rhs_fd = [i.strip() for i in x[1].split(',')]
            is_subset = set(lhs_fd).issubset(set(primaryKey))    
            if(is_subset and len(lhs_fd)!=len(primaryKey)):
                for i in rhs_fd:
                    all_rhs.append(i)
        for i in v:
            if i not in all_rhs:
                value.append(i)
        # value.sort()
        # tables_set.add(value)
        newTables[count] = value
        count+=1

    return newTables

def convert_to_3NF(tables, FD, primaryKey):
    newTables = {}
    count = 1
    for fd in FD:
        x = fd.split("-->")
        lhs_fd = [i.strip() for i in x[0].split(',')]
        is_subset = set(lhs_fd).issubset(set(primaryKey))    
        if(not is_subset):
            rhs_fd = [i.strip() for i in x[1].split(',')]
            t = lhs_fd + rhs_fd
            newTables[count] = t
            count+=1
    
    for tname,v in tables.items():
        value = []
        all_rhs = []
        for fd in FD:
            x = fd.split("-->")
            lhs_fd = [i.strip() for i in x[0].split(',')]
            rhs_fd = [i.strip() for i in x[1].split(',')]
            is_subset1 = set(lhs_fd).issubset(set(primaryKey))  
            is_subset2 = set(lhs_fd).issubset(set(v))
            if(not is_subset1 and is_subset2):
                for i in rhs_fd:
                    all_rhs.append(i)
        for i in v:
            if i not in all_rhs:
                value.append(i)
        # value.sort()
        # tables_set.add(value)
        newTables[count] = value
        count+=1

    return newTables

def convert_to_BCNF(tables, FD, candidateKey):
    newTables = {}
    count = 1
    for fd in FD:
        x = fd.split("-->>")
        lhs_fd = [i.strip() for i in x[0].split(',')]
        is_subset = set(lhs_fd).issubset(set(candidateKey))    
        if(is_subset and len(lhs_fd)!=len(candidateKey)):
            rhs_fd = [i.strip() for i in x[1].split(',')]
            t = lhs_fd + rhs_fd
            newTables[count] = t
            count+=1
    
    for tname,v in tables.items():
        value = []
        all_rhs = []
        for fd in FD:
            x = fd.split("-->>")
            lhs_fd = [i.strip() for i in x[0].split(',')]
            rhs_fd = [i.strip() for i in x[1].split(',')]
            is_subset = set(lhs_fd).issubset(set(candidateKey))
            if(is_subset and len(lhs_fd)!=len(candidateKey)):
                for i in rhs_fd:
                    all_rhs.append(i)
        for i in v:
            if i not in all_rhs:
                value.append(i)
        # value.sort()
        # tables_set.add(value)
        newTables[count] = value
        count+=1
        
    return newTables

def convert_to_4NF(tables, MVD, primaryKey):
    newTables = {}
    count = 1
    output = "4NF Decomposition:\n"
    for fd in MVD:
        x = fd.split("-->>")
        lhs_fd = [i.strip() for i in x[0].split(',')]
        is_subset = set(lhs_fd).issubset(set(primaryKey))    
        if(is_subset and len(lhs_fd)!=len(primaryKey)):
            rhs_fd = [i.strip() for i in x[1].split(',')]
            t = lhs_fd + rhs_fd
            newTables[count] = t
            output += f"Table {count}: {', '.join(t)}\n"
            count+=1
    
    for tname,v in tables.items():
        value = []
        all_rhs = []
        for fd in MVD:
            x = fd.split("-->>")
            lhs_fd = [i.strip() for i in x[0].split(',')]
            rhs_fd = [i.strip() for i in x[1].split(',')]
            is_subset = set(lhs_fd).issubset(set(primaryKey))
            if(is_subset and len(lhs_fd)!=len(primaryKey)):
                for i in rhs_fd:
                    all_rhs.append(i)
        for i in v:
            if i not in all_rhs:
                value.append(i)
        # value.sort()
        # tables_set.add(value)
        if value:
            newTables[count] = value
            output += f"Table {count}: {', '.join(value)}\n"
            count+=1
        
    return newTables, output


def is_lossless_join(table, projections, primary_key):
    # Check if the join of all projections is lossless
    all_attributes = set().union(*projections)
    if all_attributes != set(table):
        return False
    
    # Check if any projection contains the primary key
    if any(set(primary_key).issubset(proj) for proj in projections):
        return True
    
    # Check if the intersection of all projections is empty
    intersection = set.intersection(*projections)
    return len(intersection) == 0

def convert_to_5NF(tables, JD, primaryKey):
    newTables = {}
    count = 1

    for jd in JD:
        projections = [set(proj.strip().split(',')) for proj in jd.split('*')]
        
        # Check if the join dependency is trivial
        if any(set(primaryKey).issubset(proj) for proj in projections):
            continue  # Skip trivial JDs

        # Create new tables for non-trivial JDs
        for proj in projections:
            table_name = f"Table_{count}"
            newTables[table_name] = list(proj)
            count += 1

    # If no new tables were created, keep the original table
    if not newTables:
        for tname, cols in tables.items():
            newTables[tname] = cols
    else:
        # Add any remaining attributes to a new table
        all_attrs = set()
        for cols in tables.values():
            all_attrs.update(cols)
        
        remaining_attrs = all_attrs - set.union(*[set(t) for t in newTables.values()])
        if remaining_attrs:
            newTables[f"Table_{count}"] = list(remaining_attrs)

    return newTables

def generate_sql_queries(FD, Key, tables, data_types):
    queries = []
    flag_values = {}
    for tname, col in tables.items():
        s = f"CREATE TABLE {tname} ("
        fkey = ""
        column_definitions = []
        primary_key_cols = []

        for c in col:
            if c not in data_types:
                print(f"Warning: No data type found for column '{c}'. Assuming VARCHAR(100).")
                data_types[c] = "VARCHAR(100)"

            if data_types[c] == "INT":
                if c in flag_values:
                    fkey += f', FOREIGN KEY ({c}) REFERENCES {flag_values[c]}({c})'
                else:
                    flag_values[c] = tname
                column_definitions.append(f'{c} INT')
                if c in Key:
                    primary_key_cols.append(c)
            else:
                column_definitions.append(f'{c} {data_types[c]}')
                if c in Key:
                    primary_key_cols.append(c)

        # Add primary key constraint
        if primary_key_cols:
            column_definitions.append(f'PRIMARY KEY ({", ".join(primary_key_cols)})')

        s += ", ".join(column_definitions) + f"{fkey})"
        queries.append(s)

    return queries

# path for .csv file that has table details in it
filePath = input("Enter the .csv file path")

# Taking input for Key
print("Enter Primary Key - seperated by comma, if more than 1 key")
pk = input().split(",")
primaryKey = []
for p in pk:
    primaryKey.append(p.strip(' '))

print("Enter Candidate Key - seperated by comma, if more than 1 key")
ck = input().split(",")
candidateKey = []
for c in ck:
    candidateKey.append(c.strip(' '))
allKey = primaryKey + candidateKey

# Taking input for functional dependencies and storing in FD
FuncD = []
print("Enter Functional Dependencies:   Eg(A-->B, A,B-->C, A->{B,C})")
print("Enter 'Done' if completed")
i = 1
while(i):
    x = str(input())
    if(x=="DONE" or x=="done" or x == "Done"):
        i=0
    else:
        FuncD.append(x)
FD = [re.sub(r'[{}]', '', s) for s in FuncD]


# Taking input for multi valued dependencies and storing in MVD
MVDs = []
print("Enter Multi Valued Dependencies:   Eg(A->>B, A->>C)")
print("Enter 'Done' if completed")
i = 1
while(i):
    x = str(input())
    if(x=="DONE" or x=="done" or x == "Done"):
        i=0
    else:
        MVDs.append(x)
MVD = [re.sub(r'[{}]', '', s) for s in MVDs]
# After your existing input sections, add:
JDs = []
print("Enter Join Dependencies: Eg(A,B * B,C * A,C)")
print("Enter 'Done' if completed")
while True:
    x = input().strip()
    if x.lower() == "done":
        break
    JDs.append(x)

# Taking input from user to get the Choice of the highest normal form to reach (1: 1NF, 2: 2NF, 3: 3NF, B: BCNF, 4: 4NF, 5: 5NF):
print("Choice of the highest normal form to reach (1: 1NF, 2: 2NF, 3: 3NF, B: BCNF, 4: 4NF, 5: 5NF):")
print("Enter 1/2/3/B/4/5")
k = input()
user_choice = 0
# For further usage of the variable if the user inputs to convert the table to BCNF the input B is converted to "3.5"
if(k == '1'):
    user_choice = 1
if(k == '2'):
    user_choice = 2
if(k == '3'):
    user_choice = 3
if(k == 'B' or k == 'b'):
    user_choice = 3.5
if(k == '4'):
    user_choice = 4
if(k == '5'):
    user_choice = 5

# Storing the data types of each attribute from the table

data_types = check_datatypes(filePath)
#print(data_types)

tables = {}

if(user_choice>=1):
    tables = convert_to_1NF(filePath)
    # print(tables)
    SQL_queries = generate_sql_queries(FD, primaryKey, tables, data_types)
    print("1NF SCHEMA")
    for query in SQL_queries:
        print(query)
        print()
if(user_choice>=2):
    tables = convert_to_2NF(tables, FD, primaryKey)
    print("-----")
    print()
    # print(tables)
    SQL_queries = generate_sql_queries(FD, primaryKey, tables, data_types)
    print("2NF SCHEMA")
    for query in SQL_queries:
        print(query)
        print()
if(user_choice>=3):
    tables = convert_to_3NF(tables, FD, primaryKey)
    print("-----")
    print()
    # print(tables)
    SQL_queries = generate_sql_queries(FD, primaryKey, tables, data_types)
    print("3NF SCHEMA")
    for query in SQL_queries:
        print(query)
        print()
if(user_choice>=3.5):
    tables = convert_to_BCNF(tables, MVD, candidateKey)
    print("-----")
    print()
    # print(tables)
    SQL_queries = generate_sql_queries(FD, primaryKey, tables, data_types)
    print("BCNF SCHEMA")
    for query in SQL_queries:
        print(query)
        print()
if(user_choice>=4):
    tables,output_4nf = convert_to_4NF(tables, MVD, primaryKey)
    with open('/Users/rohinik/Desktop/output.txt', 'w') as f:
        f.write(output_4nf)
    print("-----")
    print()
    # print(tables)
    SQL_queries = generate_sql_queries(FD, primaryKey, tables, data_types)
    print("4NF")
    for query in SQL_queries:
        print(query)
        print()
if user_choice >= 5:
    # tables = test_5NF_join_dependencies
    tables = convert_to_5NF(tables, JDs, primaryKey)
    # test_5NF_join_dependencies(R1, R2, original_df)

# To store unique value lists
unique_tables = {}
seen_combinations = set()

for key, values in tables.items():
    # Convert the list to a tuple to make it hashable
    value_tuple = tuple(sorted(values))  # Sort to ensure consistency in representation
    if value_tuple not in seen_combinations:
        seen_combinations.add(value_tuple)
        unique_tables[len(unique_tables) + 1] = list(value_tuple)  # Use a new key

# Output the final unique dictionary
print("All unique tables after normalization")
print(unique_tables)

print()
print("SQL Queries")
SQL_queries = generate_sql_queries(FD, primaryKey, unique_tables, data_types)
for query in SQL_queries:
    print(query)
    print("  ")
