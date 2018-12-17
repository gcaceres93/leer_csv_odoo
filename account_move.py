import csv
import psycopg2

conexion_string = "host='localhost' port='5432' dbname='cms' user='postgres' password='cominsasql*2017'"
conn = psycopg2.connect(conexion_string)
cursor = conn.cursor()

reader = csv.reader(open('account_move.csv','rt',encoding='utf8'))
s = 0


for row in reader:

    print (row[2]) #impresion de prueba del campo num_asiento
    print (row[3]) #impresion de prueba del campo journal_id

    if row[1] == '':
        num_asiento = 0
    else:
        num_asiento = row[1]
    if row[5] == '':
        diario = 'null' #seteamos a 0 el campo num_asiento en caso que no exista
    else:
        diario = row[5]
    if row[6] == '':
        partner = 'null'
    else:
        partner = row[6]
    if s > 0:
        viejo_id = ''.join(i for i in row[0] if i.isdigit()) #separamos el numero viejo de id del id externo
        statement = "INSERT INTO account_move(name,num_asiento,journal_id,partner_id,date,ref,create_date,migra,state)"\
        "VALUES(%s,%s,(select res_id from ir_model_data where module || '.' || name = %s),(select res_id from ir_model_data where module || '.' || name = %s),%s,%s,'2018-12-09',TRUE,'draft') RETURNING id"
        cursor.execute(statement,(row[2],num_asiento,diario,partner,row[4],row[3]))
        conn.commit()
        move_id=cursor.fetchone()[0]#obtenemos el id generado
        #
        statement2 = "insert into ir_model_data(create_date,write_uid,name,module,model,res_id,migra)"\
        "VALUES('2018-12-09',1,'account_move_' || %s,'__export__','account.move',%s,TRUE)"
        cursor.execute(statement2,(move_id,move_id)) #insertamos manualmente en la tabla ir_model_data los valores para futuras exportaciones
        conn.commit()
        print('Fila creada:',s)
    s =+ 1




