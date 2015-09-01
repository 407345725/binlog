 #!/usr/bin/env python
#encoding=utf-8

import cherrypy
 
class HelloWorld:
    @cherrypy.expose
    def hello(self):
        return "hello"

    def index(self):
        return "Hello world!"
    index.exposed = True

conf = {'global': {'server.socket_port': 3721, 'server.socket_host': '127.0.0.1'}} 
cherrypy.config.update(conf)

cherrypy.quickstart(HelloWorld())