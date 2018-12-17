import csv
import psycopg2
#
conexion_string = "host='localhost' port='5432' dbname='cms' user='postgres' password='cominsasql*2017'"
conn = psycopg2.connect(conexion_string)
cursor = conn.cursor()

reader = csv.reader(open('account_move_line.csv','rt',encoding='utf8'))
s = 0

for row in reader:
    if s > 0:

        if s == 1:
            move_actual = row[0]
        if row[0] != '':
                move_actual = row[0]

        if row[1] == '':
            debe = 0
        else:
            debe = row[1]
        if row[2] == '':
            haber = 0
        else:
            haber = row[2]
        viejo_id = ''.join(i for i in row[0] if i.isdigit()) #separamos el numero viejo de id del id externo
        statement = "INSERT INTO account_move_line(name,debit,credit,create_date,date_maturity,account_id,move_id,supermigra)"\
        "VALUES(%s,%s,%s,%s,%s,(select res_id from ir_model_data where module || '.' || name =%s),(select res_id from ir_model_data where module || '.' || name=%s),TRUE) RETURNING id"
        cursor.execute(statement,(row[5],debe,haber,row[3],row[3],row[13],move_actual))
        conn.commit()
        print('Move line generado',s)
        move_id=cursor.fetchone()[0]#obtenemos el id generado
        print('id generado',move_id)
        statement2 = "insert into ir_model_data(create_date,write_uid,name,module,model,res_id,migra)"\
        "VALUES('2018-12-09',1,'account_move_line_' || %s,'__export__','account.move_line',%s,TRUE)"
        cursor.execute(statement2,(move_id,move_id)) #insertamos manualmente en la tabla ir_model_data los valores para futuras exportaciones
        conn.commit()
    s += 1
    print(s)




