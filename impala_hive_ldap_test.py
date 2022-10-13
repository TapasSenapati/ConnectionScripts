from impala.dbapi import connect
conn = connect(
    host = "host.name",
    port = portnumber,
    auth_mechanism = "LDAP",
    use_ssl = True,
    use_http_transport = True,
    http_path = "http.path",
    user = "aaaaaaa",
    password = "xxxxx")
    
cursor = conn.cursor()
cursor.execute("SHOW DATABASES")
res = cursor.fetchall()
print(res)
cursor.close()
conn.close()