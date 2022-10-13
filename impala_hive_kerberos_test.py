from impala.dbapi import connect

conn = connect(
    host = "host.name",
    port = portnumber,
    auth_mechanism = "GSSAPI",
    use_ssl = True,
    use_http_transport = False,
    kerberos_service_name="impala")
    
cursor = conn.cursor()
cursor.execute("SHOW DATABASES")
res = cursor.fetchall()
print(res)
cursor.close()
conn.close()