import pyodbc
import datetime
import xml.etree.ElementTree as ET


class View():
    f = ""
    
    def __init__(self, filename):
        self.f = open(filename, "wt");

    def __del__(self):
        self.f.close()
            
    def print(self, msg):
        print(msg)
        self.f.write(msg+"\n")


class SQL():
    con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=EBRR;"
                      "Database=SWE_Central_EM;"
                      "Trusted_Connection=yes;")
    cursor = con.cursor()

    def update(self):
        try:
            self.con.commit()
        except Exception as ex:
            view.print(ex)
            view.print("data NOT commited")
        else:
            view.print("data commited")
            

    def close(self):
        self.cursor.close()
        self.con.close()


class category_table():
    data = {}

    def read(self, xml_file):
        tree = ET.ElementTree(file=xml_file)
        root = tree.getroot()
        appointments = root.getchildren()
        for appointment in appointments:
            appt_children = appointment.getchildren()
            for appt_child in appt_children:
                self.data[appointment.items()[0][1]] = appt_child.text

    def search(self, cursor, key):
        res = cursor.execute("SELECT ProdCategoryName FROM dbo.tblProductCategory WHERE ProdCategoryCode = %s" % (key,))
        return cursor.fetchall()

    def add(self, cursor, category_code, category_name):
        #need try-construction here
        cursor.execute("""
        insert into dbo.tblProductCategory(ProdCategoryName,
                                           ProdCategoryShortName,
                                           SortOrder,
                                           DLM,
                                           Status,
                                           ULM,
                                           IsConcurrent,
                                           ProdCategoryCode)
                    values (?,?,?,?,?,?,?,?)""",
                    (category_name
                     ,category_name
                     ,0
                     ,datetime.datetime.now()
                     ,2
                     ,1
                     ,0
                     ,category_code,))

    def add_all(self, cursor):
        _a = 0
        _p = 0
        for k in self.data.keys():
            if not self.search(cursor, k):
                self.add(cursor, k,  self.data[k])
                view.print("  ***  Category:%s (%s) was added" % (k, self.data[k],))
                _a += 1
            else:
                _p += 1
        view.print("""categories:
                      total: %s
                      added: %s
                      passed: %s
                      """% (_a+_p, _a, _p))

class groups_table():
    data = {}

    def read(self, xml_file):
        tree = ET.ElementTree(file=xml_file)
        root = tree.getroot()
        appointments = root.getchildren()
        for appointment in appointments:
            appt_children = appointment.getchildren()
            self.data[appointment.items()[0][1]] = []
            for appt_child in appt_children:
                self.data[appointment.items()[0][1]].append(appt_child.text)
    def search(self, cursor, key):
        res = cursor.execute("""SELECT ProdGroupName
                                  FROM dbo.tblProductGroups
                                  WHERE ProdGroupCode = %s""" %(key,))
        return cursor.fetchall()

    def get_id(self, cursor, category_code):
        cursor.execute("""SELECT ProdCategory_ID
                            FROM dbo.tblProductCategory
                            WHERE ProdCategoryCode = %s""" %(category_code,))
        res = cursor.fetchall()
        if res:
            return res[0][0]
        else:
            return res
 
    def add(self, cursor, group_code, group_name, category_code):
        category_id = self.get_id(cursor, category_code)
        if not category_id:
            view.print("%s not found" %(category_code,))
            return
        cursor.execute("""insert
                            into dbo.tblProductGroups 
                            (ProdGroupName,
                             ProdGroupShortName,
                             SortOrder,
                             DLM,
                             Status,
                             ULM,
                             IsConcurrent,
                             ProdCategory_ID,
                             ProdGroupCode)
                             values (?,?,?,?,?,?,?,?,?)""",
                             (group_name,
                             group_name,
                             0,
                             datetime.datetime.now(),
                             2,
                             1,
                             0,
                             category_id,
                             group_code))


    def add_all(self, cursor):
        _a = 0
        _p = 0
        for k in self.data.keys():
            if not self.search(cursor, k):
                self.add(cursor, k, self.data[k][0], self.data[k][1])
                view.print("  ***  Groups:%s (%s) was added" % (k, self.data[k][0],))
                _a += 1
            else:
                _p += 1
        view.print("""groups:
                      total: %s
                      added: %s
                      passed: %s
                      """% (_a+_p, _a, _p))


class types_table():
    data = {}

    def read(self, xml_file):
        tree = ET.ElementTree(file=xml_file)
        root = tree.getroot()
        appointments = root.getchildren()
        for appointment in appointments:
            appt_children = appointment.getchildren()
            self.data[appointment.items()[0][1]] = []
            for appt_child in appt_children:
                self.data[appointment.items()[0][1]].append(appt_child.text)
                
    def search(self, cursor, key):
        res = cursor.execute("""SELECT ProdTypeName
                                  FROM dbo.tblProductTypes
                                  WHERE ProductTypeCode = %s"""
                                  % (key,))
        return cursor.fetchall()
    

    def get_id(self, cursor, group_code):
        cursor.execute("""SELECT ProdGroup_ID
                            FROM dbo.tblProductGroups
                            WHERE ProdGroupCode = %s"""
                            %(group_code,))
        res = cursor.fetchall()
        if res:
            return res[0][0]
        else:
            return res
 
    def add(self, cursor, type_code, type_name, group_code):
        group_id = self.get_id(cursor, group_code)
        if not group_id:
            view.print("%s not found" %(group_code,))
            return
        cursor.execute("""
        insert into dbo.tblProductTypes(ProdTypeName,
                                        ProductTypeShortName,
                                        SortOrder,
                                        DLM,
                                        Status,
                                        ULM,
                                        IsConcurrent,
                                        ProdGroup_Id,
                                        ProductTypeCode)
                    values (?,?,?,?,?,?,?,?,?)""",
                    (type_name
                     ,type_name
                     ,0
                     ,datetime.datetime.now()
                     ,2
                     ,1
                     ,0
                     ,group_id
                     ,type_code))


    def add_all(self, cursor):
        _a = 0
        _p = 0
        for k in self.data.keys():
            if not self.search(cursor, k):
                self.add(cursor, k, self.data[k][0], self.data[k][1])
                view.print("Add new type:%s (%s (group:%s)" % (k,
                                                               self.data[k][0],
                                                               self.data[k][1],
                                                               ))
                _a += 1
            else:
                _p += 1
        view.print("""types:
                      total: %s
                      added: %s
                      passed: %s
                      """% (_a+_p, _a, _p))
    

class brands_table():
    data = {}

    def read(self, xml_file):
        tree = ET.ElementTree(file=xml_file)
        root = tree.getroot()
        appointments = root.getchildren()
        for appointment in appointments:
            appt_children = appointment.getchildren()
            self.data[appointment.items()[0][1]] = []
            for appt_child in appt_children:
                self.data[appointment.items()[0][1]].append(appt_child.text)
                
    def search(self, cursor, key):
        res = cursor.execute("""SELECT ProductBrand_Name
                                  FROM dbo.tblProductBrands
                                  WHERE ProductBrand_ID = %s""" %(key,))
        return cursor.fetchall()
    

  
    def add(self, cursor, brand_code, brand_name):
        cursor.execute("""insert
                        into dbo.tblProductBrands 
                        (ProductBrand_Name,
                        Status,
                        DLM)
                        values (?,?,?)""",
                        (brand_name, 
                        2,
                        datetime.datetime.now()))

    def add_all(self, cursor):
        _a = 0
        _p = 0
        for k in self.data.keys():
            if not self.search(cursor, k):
                self.add(cursor, k, self.data[k][0])
                view.print("Add new brand %s (%s )" % (k, self.data[k][0],))
                _a += 1
            else:
                _p += 1
        view.print("""brands:
                      total: %s
                      added: %s
                      passed: %s
                      """% (_a+_p, _a, _p))


class products_table():
    data = {}

    def read(self, xml_file):
        tree = ET.ElementTree(file=xml_file)
        root = tree.getroot()
        appointments = root.getchildren()
        for appointment in appointments:
            appt_children = appointment.getchildren()
            self.data[appointment.items()[0][1]] = []
            for appt_child in appt_children:
                self.data[appointment.items()[0][1]].append(appt_child.text)
                
    def search(self, cursor, key):
        res = cursor.execute("""SELECT ProductName FROM
                            dbo.tblProducts
                            WHERE ProductCode = %s"""
                            %(key,))
        return cursor.fetchall()
    

    def get_id(self, cursor, type_code):
        cursor.execute("""SELECT ProductType_Id
                        FROM dbo.tblProductTypes
                        WHERE ProductTypeCode = %s"""
                       %(type_code,))
        res = cursor.fetchall()
        if res:
            return res[0][0]
        else:
            return res

    def get_exists(self, cursor):
        cursor.execute("SELECT [ProductCode] FROM dbo.tblProducts")
        return cursor.fetchall()

 
    def add(self
            ,cursor
            ,product_code
            ,product_shortname
            ,product_name
            ,type_code
            ,brand_id
            ,unit_code
            ,unit_weight
            ,package_qty
            ,EAN_code):
        
        type_id = self.get_id(cursor, type_code)
        if not type_id:
            print("%s not found" %(type_code,))
            return

        unit_id = unit_code #наразі заглушечка

        cursor.execute("""
        insert into dbo.tblProducts      (ProductCode,
                                           ProductType_Id,
                                           ProductName,
                                           ProductShortName,
                                           Unit_Id,
                                           UnitWeight,
                                           Package_QTY,
                                           SortOrder,
                                           DLM,
                                           Status,
                                           ULM,
                                           Price,
                                           IsMix,
                                           IsTare,
                                           IsConcurrent,
                                           ProductVolume,
                                           IsProductWeight,
                                           IsBonuse,
                                           EANCode,
                                           ProductBrand_ID,
                                           Delisted,
                                           SyncToDDB,
                                           ServiceBit,
                                           IsPromotional,
                                           IsLinkedToAllCustomers)
                    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (product_code
                     ,type_id
                     ,product_name
                     ,product_shortname
                     ,unit_id
                     ,unit_weight
                     ,package_qty
                     ,0
                     ,datetime.datetime.now()
                     ,2
                     ,1
                     ,0
                     ,0
                     ,0
                     ,0
                     ,0
                     ,0
                     ,0
                     ,EAN_code
                     ,brand_id
                     ,0
                     ,1
                     ,0
                     ,0
                     ,1))


    def add_all(self, cursor):
        _a = 0
        _p = 0
        for k in self.data.keys():
            if not self.search(cursor, k):
                print(self.data[k][7])
                self.add(cursor,
                        k,
                        self.data[k][0],
                        self.data[k][1],
                        self.data[k][2],
                        self.data[k][3],
                        self.data[k][4],
                        self.data[k][5],
                        self.data[k][6],
                        self.data[k][7])

                view.print("Add new product:%s (%s (type:%s)" % (k,
                                                               self.data[k][0],
                                                               self.data[k][1],
                                                               ))
                _a += 1
            else:
                _p += 1
        view.print("""products:
                      total: %s
                      added: %s
                      passed: %s
                      """% (_a+_p, _a, _p))


class act_types_table():
    data = {}

    def read(self, xml_file):
        tree = ET.ElementTree(file=xml_file)
        root = tree.getroot()
        appointments = root.getchildren()
        for appointment in appointments:
            appt_children = appointment.getchildren()
            self.data[appointment.items()[0][1]] = []
            for appt_child in appt_children:
                self.data[appointment.items()[0][1]].append(appt_child.text)
                
    def search(self, cursor, key):
        res = cursor.execute("""SELECT ActivityType
                                FROM dbo.tblProductsActTypesLinks
                                WHERE Product_id = %s"""
                                % (key,))
        return cursor.fetchall()


    def get_id(self, cursor, product_code):
        cursor.execute("""SELECT Product_Id
                        FROM dbo.tblProducts
                        WHERE ProductCode = %s"""
                        % (product_code,))
        res = cursor.fetchall()
        if res:
            return res[0][0]
        else:
            return res
    

  
    def add(self, cursor, product_code):
        cursor.execute("""insert into
                        dbo.tblProductsActTypesLinks(
                            Product_id,
                            ActivityType,
                            Status,
                            DLM)
                            values (?,?,?,?)""",
                            (product_code,
                            1,
                            2, 
                            datetime.datetime.now()))

    def add_all(self, cursor):
        _a = 0
        _p = 0
        for k in self.data.keys():
            prod_code = self.get_id(cursor, k)
            if not prod_code:
                view.print("Product %s not found!" % (k,))
                _p += 1
            else:
                if not self.search(cursor, prod_code):
                    self.add(cursor, prod_code)
                    view.print("Set ast.type 1 to product %s"
                               %(k,))
                    _a += 1
                else:
                    _p += 1
        view.print("""act.types:
                      total: %s
                      added: %s
                      passed: %s
                      """% (_a+_p, _a, _p))


class product_by_country_table():
    data = {}

    def read(self, xml_file):
        tree = ET.ElementTree(file=xml_file)
        root = tree.getroot()
        appointments = root.getchildren()
        for appointment in appointments:
            appt_children = appointment.getchildren()
            self.data[appointment.items()[0][1]] = []
            for appt_child in appt_children:
                self.data[appointment.items()[0][1]].append(appt_child.text)
                
    def search(self, cursor, key):
        res = cursor.execute("""SELECT Product_Id
                                FROM dbo.tblProductsByCountry
                                WHERE Product_id = %s"""
                                % (key,))
        return cursor.fetchall()


    def get_id(self, cursor, product_code):
        cursor.execute("""SELECT Product_Id
                          FROM dbo.tblProducts
                          WHERE ProductCode = %s"""
                          % (product_code,))
        res = cursor.fetchall()
        if res:
            return res[0][0]
        else:
            return res

   
    def get_geo_id(self, cursor, product_id):
        cursor.execute("""SELECT GeographyID
                       FROM dbo.tblProductsByCountry
                       WHERE Product_Id = %s"""
                       % (product_id,))
        res = cursor.fetchall()
        if res:
            return res[0][0]
        else:
            return res

  
    def add(self, cursor, product_code, geo_id):
        cursor.execute("""
        insert into dbo.tblProductsByCountry(Product_id,
                                             Country_id,
                                             VAT,
                                             IsPresent,
                                             DLM,
                                             GeographyID)
                    values (?,?,?,?,?,?)""",
                    (product_code,
                     1,
                     20,
                     1,
                     datetime.datetime.now(),
                     geo_id))


    

    def add_all(self, cursor):
        _a = 0
        _p = 0

        geo_id = self.get_geo_id(cursor, self.get_id(cursor, "106"))
        for k in self.data.keys():
            prod_id = self.get_id(cursor, k)
            if not prod_id:
                view.print("Product %s not found!" % (k,))
                _p += 1
            else:
                if not self.search(cursor, prod_id):
                    self.add(cursor, prod_id, geo_id)
                    view.print("Set geo_id %s(106) to product %s"
                               %(geo_id, self.data[k],))
                    _a += 1
                else:
                    _p += 1
        view.print("""geo linking:
                      total: %s
                      added: %s
                      passed: %s
                      """% (_a+_p, _a, _p))

def export(data, filename):
    if data:
        with open(filename, "wt") as f:
            for i in data:
                f.write(i[0]+"\n")
        view.print("""Export existing prods. to %s""" %(filename,))

 
if __name__ == "__main__":
    LOG_FOLDER = "D:\\scripts\\Logs\\"
    DATA_FOLDER = "D:\\0001\\"
    view = View("%siMETAData.txt"%(LOG_FOLDER,))
    sql = SQL()
    
    view.print("Import Categories")
    icat = category_table()
    icat.read(DATA_FOLDER+"iCat.xml")
    icat.add_all(sql.cursor)
    sql.update()

    view.print("Import Groups")
    igrp = groups_table()
    igrp.read(DATA_FOLDER+"iGrp.xml")
    igrp.add_all(sql.cursor)
    sql.update()

    view.print("Import Types")
    itps = types_table()
    itps.read(DATA_FOLDER+"iTps.xml")
    itps.add_all(sql.cursor)
    sql.update()

    view.print("Import Brands")
    ibrnd = brands_table()
    ibrnd.read(DATA_FOLDER+"iBrnds.xml")
    ibrnd.add_all(sql.cursor)
    sql.update()

    view.print("Import Products")
    iprods = products_table()
    iprods.read(DATA_FOLDER+"iProds.xml")
    iprods.add_all(sql.cursor)
    sql.update()

    view.print("Set action types")
    iacttps = act_types_table()
    iacttps.read(DATA_FOLDER+"iProds.xml")
    iacttps.add_all(sql.cursor)
    sql.update()

    view.print("Set country linking")
    ipbc = product_by_country_table()
    ipbc.read(DATA_FOLDER+"iProds.xml")
    ipbc.add_all(sql.cursor)
    sql.update()

    view.print("Export existing products")
    export(iprods.get_exists(sql.cursor), DATA_FOLDER+"iExport.txt")
    
    sql.close()
    view =""

    
