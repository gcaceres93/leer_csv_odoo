import csv
import psycopg2

conexion_string = "host='190.52.182.122' port='8432' dbname='cms' user='postgres' password='cominsasql*2017'"
conn = psycopg2.connect(conexion_string)
cursor = conn.cursor()

reader = csv.reader(open('account_move.csv','rt',encoding='utf8'))
s = 0
for row in reader:
    print (row[2]) #impresion de prueba del campo num_asiento
    print (row[3]) #impresion de prueba del campo journal_id
    if row[2] == '':
        row[2] = 0 #seteamos a 0 el campo num_asiento en caso que no exista
    if s > 0:
        statement = "INSERT INTO account_move(name,num_asiento,journal_id,partner_id,date,ref,create_date,migra,state)"\
        "VALUES(%s,%s,(select res_id from ir_model_data where module || '.' || name = %s),(select res_id from ir_model_data where module || '.' || name = %s),%s,%s,'2018-12-09',TRUE,'draft') RETURNING id"
        cursor.execute(statement,(row[1],row[2],row[3],row[4],row[5],row[6]))
        conn.commit()
        move_id=cursor.fetchone()[0]#obtenemos el id generado

        statement2 = "insert into ir_model_data(create_date,write_uid,name,module,model,res_id)"\
        "VALUES('2018-12-09',1,'__export__' || %s,'__export__','account.move',%s)"
        cursor.execute(statement2,(move_id,move_id)) #insertamos manualmente en la tabla ir_model_data los valores para futuras exportaciones
        conn.commit()
    s =+ 1




