### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    connection_norm = create_connection(normalized_database_filename)
    table_sql1 = """CREATE TABLE Region 
                   (
                        RegionID INTEGER NOT NULL PRIMARY KEY,
                        Region TEXT NOT NULL
                   )"""
    stat_sql_ins = """ INSERT INTO Region (RegionID, Region) 
                       VALUES (?, ?) """
    con_nor_cur=connection_norm.cursor()
    l_line_count = None
    p_line_count = 0
    set1 = set()
    with open(data_filename, 'r') as file:
        while l_line_count is None or p_line_count < l_line_count:
            line = file.readline()
            if not line:
                break
            p_line_count += 1
            if p_line_count >= 2:
                set1.add(line.split('\t')[4])
    set1 = sorted(set1)
    with connection_norm:
        create_table(connection_norm,table_sql1, "Region")
        i = 1
        amount = 0
        while amount < len(set1):
            tab_reg = set1[amount]
            con_nor_cur.execute(stat_sql_ins, (i, tab_reg))
            i += 1
            amount += 1
    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    connection_norm = create_connection(normalized_database_filename)
    sql_statement="""SELECT * FROM Region"""
    rows=execute_sql_statement(sql_statement, connection_norm)
    output = {}
    for row in rows:
        key = None
        value = None
        for i, item in enumerate(row):
            if i == 1:
                key = item
            elif i == 0:
                value = item
        output[key] = value
    return output
    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    tab3_data = step2_create_region_to_regionid_dictionary(normalized_database_filename)
    connection_norm = create_connection(normalized_database_filename)
    table_sql2 = """CREATE TABLE Country
                   (
                        CountryID INTEGER NOT NULL PRIMARY KEY,
                        Country TEXT NOT NULL,
                        RegionID INTEGER NOT NULL,
                        FOREIGN KEY(RegionID) REFERENCES Region(RegionID)
                    )"""
    create_table(connection_norm, table_sql2, "Country")
    def insert_country_table(connection_norm, values):
        ins_sql_stat3 = """INSERT INTO Country(CountryID, Country, RegionID)
                           VALUES(?, ?, ?)"""
        with connection_norm:
                con_nor_cur = connection_norm.cursor()
                con_nor_cur.execute(ins_sql_stat3, values)
                return con_nor_cur.lastrowid
    l_line_count = None
    p_line_count = 0
    output = set()
    with open(data_filename, 'r') as file:
        while l_line_count is None or p_line_count < l_line_count:
            line = file.readline()
            if not line:
                break
            p_line_count += 1
            if p_line_count >= 2:
               output.add((line.split('\t')[3], line.split('\t')[4]))
    result = sorted(list(output))
    i = 1
    count = iter(result)
    while True:
        try:
            country = next(count)
            info=(i, country[0], tab3_data[country[1]])
            insert_country_table(connection_norm,info)
            i += 1
        except StopIteration:
            break
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    with create_connection(normalized_database_filename) as connection:
        sql_stat="""SELECT CountryID, Country FROM Country"""
        rows=execute_sql_statement(sql_stat, connection)
        output = {}
        for row in rows:
            key = None
            value = None
            for i, item in enumerate(row):
                if i == 1:
                    key = item
                elif i == 0:
                    value = item
            output[key] = value
        return output
    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    tab4_data = step4_create_country_to_countryid_dictionary(normalized_database_filename)
    connection_norm = create_connection(normalized_database_filename)
    table_sql5 = """CREATE TABLE Customer
                    (
                        CustomerID INTEGER NOT NULL PRIMARY KEY,
                        FirstName TEXT NOT NULL,
                        LastName TEXT NOT NULL,
                        Address TEXT NOT NULL,
                        City TEXT NOT NULL,
                        CountryID INTEGER NOT NULL,
                        FOREIGN KEY(CountryID) REFERENCES Country(CountryID)
                    )"""
    create_table(connection_norm, table_sql5, "Customer")
    def insert_customer_table(connection_norm, values):
        ins_sql_stat5 = """INSERT INTO Customer(CustomerID, FirstName, LastName, Address, City, CountryID)
                 VALUES (?, ?, ?, ?, ?, ?)"""
        with connection_norm:
                con_nor_cur = connection_norm.cursor()
                con_nor_cur.execute(ins_sql_stat5, values)
                return con_nor_cur.lastrowid
    with open(data_filename, 'r') as file:
        lines = file.readlines()
        n_lines = []
        for i in range(1, len(lines)):
            n_lines.append(lines[i])
        lines = n_lines
        output = []
        i = 0
        while i < len(lines):
            line = lines[i]
            strip_line = line.strip()
            spt_line = strip_line.split('\t')
            l_tuple = tuple(spt_line)
            output.append(l_tuple)
            i += 1
    i = 0
    while i < len(output) - 1:
        j = 0
        while j < len(output) - 1 - i:
            if output[j][0] > output[j+1][0]:
                output[j], output[j+1] = output[j+1], output[j]
            j += 1
        i += 1
    with connection_norm:
        with open(data_filename) as file:
            lines = [tuple(line.strip().split('\t')) for line in file.readlines()[1:]]
            s_lines = []
            for line in lines:
                for i in range(len(s_lines)):
                    if line[0] < s_lines[i][0]:
                        s_lines.insert(i, line)
                        break
                else:
                    s_lines.append(line)
            output = []
            n = 0
            while n < len(s_lines):
                row = s_lines[n]
                n_p = row[0].split(' ')
                cust_info = (
                    n + 1,
                    n_p[0],
                    ' '.join(n_p[1:]),
                    row[1],
                    row[2],
                    tab4_data[row[3]]
                )
                insert_customer_table(connection_norm, cust_info)
                output.append(cust_info)
                n += 1
    ### END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    with create_connection(normalized_database_filename) as connection:
        sql_stat="SELECT * FROM Customer"
        rows = execute_sql_statement(sql_stat, connection)
        result = {}
        i = 0
        while i < len(rows):
            row=rows[i]
            key = f"{row[1]} {row[2]}"
            value = int(row[0])
            result[key] = value
            i += 1
        return result
    ### END SOLUTION
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    table_sql7 = """CREATE TABLE ProductCategory
                    (
                        ProductCategoryID INTEGER NOT NULL PRIMARY KEY,
                        ProductCategory TEXT NOT NULL,
                        ProductCategoryDescription TEXT NOT NULL
                    )"""
    ins_sql_stat7="""INSERT INTO ProductCategory(ProductCategoryID, ProductCategory, ProductCategoryDescription) 
                     VALUES (?, ?, ?)"""
    with create_connection(normalized_database_filename) as connection:
        create_table(connection, table_sql7, "ProductCategory")
    with open(data_filename, 'r') as file:
        line = file.readline()
        line = line.strip()
        title = line.split('\t')
        info_data1 = -1
        info_data2 = -1
        for i, item in enumerate(title):
            if item == 'ProductCategory':
                info_data1 = i
            elif item == 'ProductCategoryDescription':
                info_data2 = i
        output= {}
        line = file.readline().strip()
        count = iter(file)
        while True:
            try:
                line = next(count)
                l_l = line.split('\t')[info_data1].split(';')
                n_l = line.split('\t')[info_data2].split(';')
                for i in range(len(l_l)):
                    division = l_l[i]
                    des_ion = n_l[i]
                    output[division] = des_ion
            except StopIteration:
                break
            line = file.readline().strip()
    with create_connection(normalized_database_filename) as connection_norm:
        con_nor_cur = connection_norm.cursor()
        i = 0
        while i < len(sorted(output)):
            division = sorted(output)[i]
            resultant_data=(i+1, division, output[division])
            con_nor_cur.execute(ins_sql_stat7,resultant_data)
            i+=1
    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
        connection_norm = create_connection(normalized_database_filename)
        sql_stat = """SELECT * FROM ProductCategory"""
        con_nor_cur = connection_norm.cursor()
        con_nor_cur.execute(sql_stat)
        rows = con_nor_cur.fetchall()
        output = {}
        for row in rows:
            key = None
            value = None
            for i, item in enumerate(row):
                if i == 1:
                    key = item
                elif i == 0:
                    value = item
            output[key] = value
        return output
    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    data_tab9= step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    connection_norm= create_connection(normalized_database_filename)
    table_sql9 = """CREATE TABLE Product
                    (
                        ProductID INTEGER NOT NULL PRIMARY KEY,
                        ProductName TEXT NOT NULL,
                        ProductUnitPrice REAL NOT NULL,
                        ProductCategoryID INTEGER NOT NULL,
                        FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
                    )"""
    create_table(connection_norm,table_sql9, "Product")
    with open(data_filename, 'r') as file:
        info = [line for line in file.readlines()[1:]]
        data_dict1 = {}
        data_dict2 = {}
        count = iter(info)
        while True:
            try:
                line = next(count)
                data_1 = line.strip().split('\t')[5].split(';')
                data_2 = line.strip().split('\t')[8].split(';')
                data_3 = line.strip().split('\t')[6].split(';')
                np_tab = zip((x.replace('\n', '').replace('\r', '').replace('\t', '') for x in data_1), (x.replace('\n', '').replace('\r', '').replace('\t', '') for x in data_2), (x.replace('\n', '').replace('\r', '').replace('\t', '') for x in data_3))
                for n_tab9, p_tab9, c_tab9 in np_tab:
                    data_dict1[(n_tab9, c_tab9)] = float(p_tab9)
                info_1= line.strip().split('\t')[5].split(';')
                info_2 = line.strip().split('\t')[6].split(';')
                nc_tab = zip((x.replace('\n', '').replace('\r', '').replace('\t', '') for x in info_1), (x.replace('\n', '').replace('\r', '').replace('\t', '') for x in info_2))
                for n_tab9, c_tab9 in nc_tab:
                    data_dict2[n_tab9] = c_tab9
            except StopIteration:
                break
    data_dict1 = {key : value for key, value in sorted(data_dict1.items())}
    data_dict2 = {key : value for key, value in sorted(data_dict2.items())}
    with connection_norm:
        ins_sql_stat9 = """INSERT INTO Product(ProductID,ProductName,ProductUnitPrice,ProductCategoryID)
                          VALUES (?, ?, ?, ?)"""
        con_nor_cur = connection_norm.cursor()
        num = 1
        results = []
        iterator = iter(data_dict1.items())
        i = 0
        while True:
            try:
                item = next(iterator)
                ((n_tab9, c_tab9), p_tab9) = item
                results.append((num + i, n_tab9, p_tab9, data_tab9[data_dict2[n_tab9]]))
                i += 1
            except StopIteration:
                break
        con_nor_cur.executemany(ins_sql_stat9, results)
        num += len(results)
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    connection_norm = create_connection(normalized_database_filename)
    sql_statement="""SELECT ProductID, ProductName FROM Product"""
    rows=execute_sql_statement(sql_statement, connection_norm)
    output = {}
    for row in rows:
        key = None
        value = None
        for i, item in enumerate(row):
            if i == 1:
                key = item
            elif i == 0:
                value = item
        output[key] = value
    return output

    ### END SOLUTION
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    data1_tab11 = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    data2_tab11= step10_create_product_to_productid_dictionary(normalized_database_filename)
    connection_norm = create_connection(normalized_database_filename)
    table_sql11 = """CREATE TABLE OrderDetail
                     (
                        OrderID INTEGER NOT NULL PRIMARY KEY,
                        CustomerID INTEGER NOT NULL,
                        ProductID INTEGER NOT NULL,
                        OrderDate INTEGER NOT NULL,
                        QuantityOrdered INTEGER NOT NULL,
                        FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID),
                        FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
                    )"""
    create_table(connection_norm, table_sql11, "OrderDetail")
    def insert_OrderDetail(connection_norm, values):
        ins_sql11 = """INSERT INTO OrderDetail(OrderID,CustomerID,ProductID,OrderDate,QuantityOrdered)
                       VALUES(?,?,?,?,?)"""
        con_nor_cur = connection_norm.cursor()
        con_nor_cur.execute(ins_sql11, values)
        return con_nor_cur.lastrowid
    output = []
    with open(data_filename, 'r') as file:
        lines = [line for i, line in enumerate(file.readlines()) if i >= 1]
        for line in lines:
            l_tab11 = []
            for n in line.split('\t'):
                l_tab11.append(n.strip())
            res1_tab11 = []
            for x in l_tab11[10].split(';'):
                res1_tab11.append(x.strip())
            res2_tab11 = []
            for x in l_tab11[9].split(';'):
                res2_tab11.append(x.strip())
            res3_tab11 = []
            for x in l_tab11[5].split(';'):
                res3_tab11.append(x.strip())
            i = 0
            while i < len(res3_tab11):
                date_tab11 = res1_tab11[i]
                fdate_tab11 = '-'.join([date_tab11[:4], date_tab11[4:6], date_tab11[6:]])
                output.append([l_tab11[0]] + [res3_tab11[i]] + [fdate_tab11] + [res2_tab11[i]])
                i +=1
    with connection_norm:
        num = 1
        amount = 0
        while amount < len(output):
            results = output[amount]
            r_1 = data1_tab11[results[0]]
            r_2 = data2_tab11[results[1]]
            r_3=(num + amount, r_1, r_2,results[2], results[3])
            r_4 = insert_OrderDetail(connection_norm, r_3)
            amount += 1
    ### END SOLUTION


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    customer_dict = step6_create_customer_to_customerid_dictionary('normalized.db')
    try:
        name_customer_tab6 = customer_dict[CustomerName]
    except KeyError:
        name_customer_tab6 = None
    sql_statement = f"""
                        SELECT Tab6_Cust.FirstName || ' ' || Tab6_Cust.LastName AS Name,
                            Tab6_Prod.ProductName,
                            Tab6_OrdDet.OrderDate,
                            Tab6_Prod.ProductUnitPrice,
                            Tab6_OrdDet.QuantityOrdered,
                            ROUND(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered, 2) AS Total
                        FROM OrderDetail AS Tab6_OrdDet
                        INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID
                        INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
                        WHERE Tab6_Cust.CustomerID = '{name_customer_tab6}'
                    """
    ### END SOLUTIONsql_statement = """
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    customer_dict = step6_create_customer_to_customerid_dictionary('normalized.db')
    try:
        name_customer_tab6 = customer_dict[CustomerName]
    except KeyError:
        name_customer_tab6 = None
    sql_statement = f"""
                        SELECT Tab6_Cust.FirstName || ' ' || Tab6_Cust.LastName AS Name,
                            ROUND(SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered), 2) AS Total
                        FROM OrderDetail AS Tab6_OrdDet
                        INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID
                        INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
                        WHERE Tab6_Cust.CustomerID = '{name_customer_tab6}'
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
    sql_statement = """
                        SELECT Tab6_Cust.FirstName ||' ' || Tab6_Cust.LastName AS Name,
                            ROUND(SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered), 2)AS Total
                        FROM OrderDetail AS Tab6_OrdDet
                        INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID=Tab6_Prod.ProductID
                        INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID=Tab6_Cust.CustomerID
                        GROUP BY Name
                        ORDER BY -Total 
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
                        SELECT Tab6_Reg.Region, 
                            ROUND(SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered), 2) AS Total
                        FROM OrderDetail AS Tab6_OrdDet
                        INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
                        INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID
                        INNER JOIN Country AS Tab6_County ON Tab6_Cust.CountryID = Tab6_County.CountryID
                        INNER JOIN Region AS Tab6_Reg ON Tab6_County.RegionID = Tab6_Reg.RegionID
                        GROUP BY Tab6_Reg.Region
                        ORDER BY -Total;
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
                        SELECT Tab6_County.Country, 
                            ROUND(SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered)) AS CountryTotal
                        FROM OrderDetail AS Tab6_OrdDet 
                        INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
                        INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID
                        INNER JOIN Country AS Tab6_County ON Tab6_Cust.CountryID = Tab6_County.CountryID
                        GROUP BY Tab6_County.Country
                        ORDER BY -CountryTotal
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """
                        SELECT Region, Country, 
                            ROUND(SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered)) AS CountryTotal,
                        RANK() OVER (PARTITION BY Region ORDER BY -SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered)) AS CountryRegionalRank
                        FROM OrderDetail AS Tab6_OrdDet
                        INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
                        INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID
                        INNER JOIN Country AS Tab6_County ON Tab6_Cust.CountryID = Tab6_County.CountryID
                        INNER JOIN Region AS Tab6_Reg ON Tab6_County.RegionID = Tab6_Reg.RegionID
                        GROUP BY Region, Country
                        ORDER BY Region, CountryRegionalRank;
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """
                        SELECT 
                            Tab6_top.Region, 
                            Tab6_top.Country, 
                            ROUND(Tab6_top.CountryTotal) AS CountryTotal,
                            Tab6_top.CountryRegionalRank
                        FROM 
                            (SELECT 
                                Tab6_Reg.Region, 
                                Tab6_County.Country, 
                                SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered) AS CountryTotal,
                                RANK() OVER (PARTITION BY Tab6_Reg.Region ORDER BY -SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered)) AS CountryRegionalRank
                            FROM OrderDetail AS Tab6_OrdDet
                            INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
                            INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID
                            INNER JOIN Country AS Tab6_County ON Tab6_Cust.CountryID = Tab6_County.CountryID
                            INNER JOIN Region AS Tab6_Reg ON Tab6_County.RegionID = Tab6_Reg.RegionID
                            GROUP BY Tab6_Reg.Region, Tab6_County.Country
                            ) AS Tab6_top
                        WHERE Tab6_top.CountryRegionalRank = 1
                        ORDER BY Tab6_top.Region
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """
                        WITH S_Cust_Sal_By_Quar AS 
                        (
                            SELECT CAST(STRFTIME('%Y', Tab6_OrdDet.OrderDate) AS INTEGER) AS Year,
                                CASE 
                                    WHEN strftime('%m', OrderDate) BETWEEN '01' AND '03' THEN 'Q1'
                                    WHEN strftime('%m', OrderDate) BETWEEN '04' AND '06' THEN 'Q2'
                                    WHEN strftime('%m', OrderDate) BETWEEN '07' AND '09' THEN 'Q3'
                                    ELSE 'Q4'
                                END AS Quarter,
                                Tab6_OrdDet.CustomerID,
                                ROUND(SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered)) AS Total
                            FROM OrderDetail AS Tab6_OrdDet
                            INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID
                            INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
                            GROUP BY Quarter, Year, Tab6_OrdDet.CustomerID
                            ORDER BY Year
                        )
                        SELECT Quarter, Year, CustomerID, Total
                        FROM S_Cust_Sal_By_Quar  
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """
                        WITH Cust_sales AS
                        (
                            SELECT CAST(STRFTIME('%Y', OrderDate) AS INTEGER) AS Year,
                                CASE 
                                    WHEN strftime('%m', OrderDate) BETWEEN '01' AND '03' THEN 'Q1'
                                    WHEN strftime('%m', OrderDate) BETWEEN '04' AND '06' THEN 'Q2'
                                    WHEN strftime('%m', OrderDate) BETWEEN '07' AND '09' THEN 'Q3'
                                    ELSE 'Q4'
                                END AS Quarter,
                            Tab6_OrdDet.CustomerID,
                            ROUND(SUM(Tab6_Prod.ProductUnitPrice * Tab6_OrdDet.QuantityOrdered)) AS Total
                            FROM OrderDetail AS Tab6_OrdDet
                            INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID
                            INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
                            GROUP BY Quarter, Year, Tab6_OrdDet.CustomerID
                        ),
                        Quar_y_Cust_sal AS (
                            SELECT *, RANK() OVER (PARTITION BY Quarter, Year ORDER BY -Total) AS CustomerRank
                            FROM Cust_sales
                        )
                        SELECT Quarter, Year, CustomerID, Total, CustomerRank
                        FROM Quar_y_Cust_sal
                        WHERE CustomerRank <=5
                        ORDER BY Year,quarter, CustomerRank
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = f"""
    WITH Ran_Mon_Sal AS (
            SELECT 
                CAST(strftime('%Y', OrderDate) AS INTEGER) AS Year,
                CASE strftime('%m', OrderDate)
                    WHEN '01' THEN 'January'
                    WHEN '02' THEN 'February'
                    WHEN '03' THEN 'March'
                    WHEN '04' THEN 'April'
                    WHEN '05' THEN 'May'
                    WHEN '06' THEN 'June'
                    WHEN '07' THEN 'July'
                    WHEN '08' THEN 'August'
                    WHEN '09' THEN 'September'
                    WHEN '10' THEN 'October'
                    WHEN '11' THEN 'November'
                    WHEN '12' THEN 'December'
                END AS Month,
                SUM(ROUND(Tab6_Prod.ProductUnitPrice * QuantityOrdered)) AS Total
            FROM OrderDetail AS Tab6_OrdDet
            INNER JOIN Product AS Tab6_Prod ON Tab6_OrdDet.ProductID = Tab6_Prod.ProductID
            INNER JOIN Customer AS Tab6_Cust ON Tab6_OrdDet.CustomerID = Tab6_Cust.CustomerID 
            GROUP BY Month
        )
        SELECT Month,Total,
            RANK() OVER (ORDER BY -Total) AS TotalRank
        FROM Ran_Mon_Sal
        ORDER BY TotalRank
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """
     
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement