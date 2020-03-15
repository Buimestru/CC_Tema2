import json
import re
from http.server import BaseHTTPRequestHandler, HTTPServer

import pyodbc


class RequestHandlerClass(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == '/Books':
            con = pyodbc.connect(Trusted_Connection='yes', driver='{SQL Server}',
                                 server='LAPTOP-2QFTLS8H\\SQLEXPRESS',
                                 database='LibraryDB')
            cursor = con.cursor()

            cursor.execute("SELECT * FROM Books")
            response = cursor.fetchall()
            self.send_response(200, 'Ok')
            self.send_header('Content-type', 'JSON')
            self.end_headers()
            text = []
            for row in response:
                row = '{"Id":%d, "Title":"%s", "Author":"%s", "PublishingHouse":"%s", "SerialNumber":"%s"}' \
                      % (row[0], row[1], row[2], row[3], row[4])
                row = json.loads(row)
                row = json.dumps(row)
                text.append(row)
            self.wfile.write(str(text).encode())
            con.close()
        elif self.path.startswith('/Books/') and len(self.path.split('/')) == 3:
            resourceId = self.path.split('/')[2]
            con = pyodbc.connect(Trusted_Connection='yes', driver='{SQL Server}',
                                 server='LAPTOP-2QFTLS8H\\SQLEXPRESS',
                                 database='LibraryDB')
            cursor = con.cursor()
            if re.match("[1-9][0-9]*", resourceId):
                cursor.execute("SELECT * FROM Books where Id=%s" % resourceId)
                response = cursor.fetchall()
                if len(response) == 1:
                    self.send_response(200, 'Ok')
                    self.send_header('Content-type', 'JSON')
                    self.end_headers()
                    text = []
                    for row in response:
                        row = '{"Id":%d, "Title":"%s", "Author":"%s", "PublishingHouse":"%s", "SerialNumber":"%s"}' \
                              % (row[0], row[1], row[2], row[3], row[4])
                        row = json.loads(row)
                        row = json.dumps(row)
                        text.append(row)
                    self.wfile.write(str(text).encode())
                else:
                    self.send_error(404, 'Not Found', 'Id not found')
            else:
                self.send_error(404, 'Not Found', 'Invalid Id')
            con.close()
        else:
            self.send_error(400, 'Invalid Path', 'Please, select the correct path')

    def do_POST(self):
        if self.path == '/Books':
            con = pyodbc.connect(Trusted_Connection='yes', driver='{SQL Server}',
                                 server='LAPTOP-2QFTLS8H\\SQLEXPRESS',
                                 database='LibraryDB')
            con.autocommit = True
            cursor = con.cursor()
            post_body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if len(post_body) != 4 \
                    or sorted(['Title', 'Author', 'PublishingHouse', 'SerialNumber']) != sorted(list(post_body.keys())):
                self.send_error(400, 'Invalid body', 'Your body\'s data are invalid')
            else:
                query = "Select * from Books where SerialNumber = '%s'" % post_body["SerialNumber"]
                cursor.execute(query)
                response = cursor.fetchall()
                if len(response) > 0:
                    self.send_error(409, 'Conflict', 'Resource already exists')
                else:
                    query = "INSERT INTO Books(Title,Author,PublishingHouse,SerialNumber) VALUES ('%s','%s','%s','%s')"\
                            % (post_body["Title"], post_body["Author"], post_body["PublishingHouse"],
                               post_body["SerialNumber"])
                    cursor.execute(query)
                    query = "Select * from Books where SerialNumber = '%s'" % post_body["SerialNumber"]
                    cursor.execute(query)
                    response = cursor.fetchall()
                    if len(response) == 1:
                        self.send_response(201, 'Created')
                        self.send_header('Location', "/Books/" + str(response[0][0]))
                        self.end_headers()
                    else:
                        self.send_error(500, 'Internal server error', 'Database error')
            con.close()
        elif self.path.startswith('/Books/'):
            self.send_error(404, 'Not Found', 'The path not found')
        else:
            self.send_error(400, 'Invalid Path', 'Please, select the correct path')

    def do_PUT(self):
        if self.path.startswith('/Books/') and len(self.path.split('/')) == 3:
            resourceId = self.path.split('/')[2]
            con = pyodbc.connect(Trusted_Connection='yes', driver='{SQL Server}',
                                 server='LAPTOP-2QFTLS8H\\SQLEXPRESS',
                                 database='LibraryDB')
            con.autocommit = True
            cursor = con.cursor()
            post_body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if len(post_body) != 4 \
                    or sorted(['Title', 'Author', 'PublishingHouse', 'SerialNumber']) != sorted(list(post_body.keys())):
                self.send_error(400, 'Invalid body', 'Your body\'s data are invalid')
            else:
                if re.match("[1-9][0-9]*", resourceId):
                    cursor.execute("SELECT * FROM Books where Id=%s" % resourceId)
                    response = cursor.fetchall()
                    if len(response) == 1:
                        query = "UPDATE Books SET Title='%s',Author='%s',PublishingHouse='%s',SerialNumber='%s' where "\
                                "Id='%s'" % (post_body["Title"], post_body["Author"], post_body["PublishingHouse"],
                                             post_body["SerialNumber"], resourceId)
                        cursor.execute(query)
                        query = "Select * from Books where Id = '%s'" % resourceId
                        cursor.execute(query)
                        response = cursor.fetchall()
                        if response[0][1] == post_body["Title"] and response[0][2] == post_body["Author"] \
                                and response[0][3] == post_body["PublishingHouse"] \
                                and response[0][4] == post_body["SerialNumber"]:
                            self.send_response(204, 'No Content')
                            self.end_headers()
                        else:
                            self.send_error(500, 'Internal server error', 'Database error')
                    else:
                        self.send_error(404, 'Not Found', 'Id not found')
                else:
                    self.send_error(404, 'Not Found', 'Invalid Id')
            con.close()
        elif self.path == '/Books':
            self.send_error(405, 'Method Not Allowed', 'This method is not allowed')
        else:
            self.send_error(400, 'Invalid Path', 'Please, select the correct path')

    def do_PATCH(self):
        if self.path.startswith('/Books/') and len(self.path.split('/')) == 3:
            resourceId = self.path.split('/')[2]
            con = pyodbc.connect(Trusted_Connection='yes', driver='{SQL Server}',
                                 server='LAPTOP-2QFTLS8H\\SQLEXPRESS',
                                 database='LibraryDB')
            con.autocommit = True
            cursor = con.cursor()
            post_body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if len([k for k in list(post_body.keys())
                    if k not in ['Title', 'Author', 'PublishingHouse', 'SerialNumber']]) != 0:
                self.send_error(400, 'Invalid body', 'Your body\'s data are invalid')
            else:
                if re.match("[1-9][0-9]*", resourceId):
                    cursor.execute("SELECT * FROM Books where Id=%s" % resourceId)
                    response = cursor.fetchall()
                    if len(response) == 1:
                        query = "UPDATE Books SET "
                        for key in list(post_body.keys()):
                            query += key + "='" + str(post_body[key]) + "',"
                        query = query[:-1]
                        query += " where Id=" + str(resourceId)
                        cursor.execute(query)
                        ok = 1
                        for key in list(post_body.keys()):
                            query = "Select " + key + " from Books where Id = '%s'" % resourceId
                            cursor.execute(query)
                            response = cursor.fetchall()
                            if response[0][0] != post_body[key]:
                                ok = 0
                                break
                        if ok == 1:
                            self.send_response(204, 'No Content')
                            self.end_headers()
                        else:
                            self.send_error(500, 'Internal server error', 'Database error')
                    else:
                        self.send_error(404, 'Not Found', 'Id not found')
                else:
                    self.send_error(404, 'Not Found', 'Invalid Id')
            con.close()
        elif self.path == '/Books':
            self.send_error(405, 'Method Not Allowed', 'This method is not allowed')
        else:
            self.send_error(400, 'Invalid Path', 'Please, select the correct path')

    def do_DELETE(self):
        if self.path.startswith('/Books/') and len(self.path.split('/')) == 3:
            resourceId = self.path.split('/')[2]
            con = pyodbc.connect(Trusted_Connection='yes', driver='{SQL Server}',
                                 server='LAPTOP-2QFTLS8H\\SQLEXPRESS',
                                 database='LibraryDB')
            con.autocommit = True
            cursor = con.cursor()
            if re.match("[1-9][0-9]*", resourceId):
                cursor.execute("SELECT * FROM Books where Id=%s" % resourceId)
                response = cursor.fetchall()
                if len(response) == 1:
                    cursor.execute("DELETE from Books where Id='%s'" % resourceId)
                    cursor.execute("SELECT * FROM Books where Id=%s" % resourceId)
                    response = cursor.fetchall()
                    if len(response) == 0:
                        self.send_response(200, 'Ok')
                        self.end_headers()
                    else:
                        self.send_error(500, 'Internal server error', 'Database error')
                else:
                    self.send_error(404, 'Not Found', 'Id not found')
            else:
                self.send_error(404, 'Not Found', 'Invalid Id')
            con.close()
        elif self.path == '/Books':
            self.send_error(405, 'Method Not Allowed', 'This method is not allowed')
        else:
            self.send_error(400, 'Invalid Path', 'Please, select the correct path')

    def send_error(self, code, message, explanation):
        self.send_response(code, message)
        self.send_header('Content-type', 'JSON')
        self.end_headers()
        msg = '{"message" : "%s"}' % explanation
        self.wfile.write(msg.encode())


http_server = HTTPServer(('localhost', 5005), RequestHandlerClass)
http_server.serve_forever()
