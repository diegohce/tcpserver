import sys
import os
import time
import datetime
import signal

import socket
import select
import threading
import traceback

#~ import ssl


__all__ = ['Worker', 'MainThread', 'LogMixin']

def log_print( s, thname = "__main__"):
	print datetime.datetime.utcnow().isoformat().replace('T', ' '), "- (" + thname + ") -", s
	sys.stdout.flush()
	
class LogMixin:
	log_class_name = 'LogMixin'
	
	def log(self, s):
		log_print(s, self.log_class_name)

class Worker(object):

	def __init__(self, sock=None, **kwargs):
		self.sock = sock
		self.close_socket = False
		self.rth_name = threading.currentThread().getName()
		self.ctx = kwargs.get('context', None)
		self.timeout = None
		
	def log(self, text):
		log_print(text, self.rth_name)
		
	def run(self, buffer=None):
		return True

	def set_timeout(self, timeout):
		self.sock.settimeout(timeout)
		self.timeout = timeout

	def get_timeout(self):
		return self.timeout
		
	def handle_connect(self):
		if self.ctx['app_name']:
			self.sock.send('%s\n' % self.ctx['app_name'])
		
	def handle_close(self):
		self.log('Ending TCP session')
		
	def handle_error(self, ex):
		self.log( traceback.format_exc() )
		self.log('Trying to send -ERR %s response to client' % repr(ex))
		try:			
			self.sock.send('-ERR %s' % repr(ex))
			self.log('-ERR sent')
		except Exception, ex:
			self.log('Unable to send -ERR response to client. Reason: %s' % repr(ex))



class ChatWorker(Worker):
	
	def __init__(self, *args, **kwargs):
		Worker.__init__(self, *args, **kwargs)
		self.__terminator = None
		self.__buffer = ''

	def push(self, data):
		self.sock.send(data)
	
	def run(self, buffer):
		if not self.__terminator:
			self.collect_incoming_data(buffer)
			return
		
		self.__buffer += buffer
		while self.__buffer:
			if type(self.__terminator) == int:
				if len(self.__buffer) >= self.__terminator:
					self.collect_incoming_data(self.__buffer[:self.__terminator])
					self.__buffer = self.__buffer[self.__terminator:]
					self.found_terminator()
				else:
					self.collect_incoming_data(self.__buffer)
					self.__terminator = self.__terminator - len(self.__buffer)
					self.__buffer = ''
				
			elif type(self.__terminator) == str:
				pos = self.__buffer.find(self.__terminator)
				if pos != -1:
					if pos > 0:
						self.collect_incoming_data(self.__buffer[:pos])
					self.__buffer = self.__buffer[pos+len(self.__terminator):]
					self.found_terminator()
				else:
					tlen = len(self.__terminator) - 1
					while tlen and not self.__buffer.endswith(self.__terminator[:tlen]):
						tlen -= 1
					if tlen:
						if tlen != len(self.__terminator):
							self.collect_incoming_data(self.__buffer[:-tlen])
							self.__buffer = self.__buffer[-tlen:]
						break
					else:
						self.collect_incoming_data(self.__buffer)
						self.__buffer = ''
				
				
	def set_terminator(self, term):
		self.__terminator = term

	def get_terminator(self):
		return self.__terminator
		
	def close_when_done(self):
		self.close_socket = True

	def discard_buffers(self):
		self.__buffer = ''

	def collect_incoming_data(self, data):
		pass

	def terminator_found(self):
		pass
			

class WorkingThread(threading.Thread):

	def __init__(self, client, welcome_name=None, worker_class=Worker):
		threading.Thread.__init__(self)
		self.__ctx = client[2]
		self.__sock = client[0]
				
		self.__remote_addr = client[1]
		self.__thname = self.getName()
		self.__welcome_name = welcome_name
		self.__worker_class = worker_class
	
	def run(self):
		log_print('Incomming connection from %s to %s' % \
			( str(self.__remote_addr), str(self.__sock.getsockname()) ), \
			self.__thname)

		#~ if self.__ctx.get('ssl', False):
			#~ self.__sock = ssl.wrap_socket(self.__sock,
			                              #~ keyfile='./key.pem',
										  #~ certfile='./cert.pem',
										  #~ server_side=True)

		work = self.__worker_class(self.__sock, context=self.__ctx)
		work.handle_connect()
		
		try:
			while True:
				if work.close_socket:
					break
				if not work.get_timeout():
					self.__sock.settimeout(self.__ctx.get('sock_timeout', 40))

				buffer = self.__sock.recv(4096)
				work.set_timeout(None)
				
				if not buffer:
					break
				
				if work.run(buffer):
					work.on_close()
					break
			
		except Exception, ex:
			work.handle_error(ex)
		finally:
			work.handle_close()
			self.__sock.close()

		
class MainThread(threading.Thread):
		
	def __init__(self, context=None):

		threading.Thread.__init__(self)

		self.__ctx = context
		self.__run_forest = True
		self.__welcome_name = context.get('app_name', None)
		self.__listen_port = context.get('listen_port', 1066)
		self.__bind_ip = context.get('bind_ip', '')
		self.__sock = []

	def run(self):
		port_count = self.__ctx.get('port_spawn', 1)
		if port_count <= 0:
			port_count = 1
			
		portno_list = []
		for i in range(port_count):
			portno_list.append( self.__listen_port+i )
			
		for portno in portno_list:
			s = socket.socket()
			s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			s.bind( (self.__bind_ip, portno) )
			s.listen(socket.SOMAXCONN)
			self.__sock.append(s)
			log_print('Listening on port %d with name %s' % (portno, self.__welcome_name), 'MainThread')

		#~ self.__sock = socket.socket()
		#~ self.__sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		#~ self.__sock.bind( (self.__bind_ip, self.__listen_port) )
		#~ self.__sock.listen(socket.SOMAXCONN)
		#~ log_print('Listening on port %d with name %s' % (self.__listen_port, self.__welcome_name), 'MainThread')
	
		while self.__run_forest:
			rread, rwrite, werr = select.select( self.__sock, [], [], 2.0)
			for s in rread:
				client_sock, client_addr = s.accept()
			
				client_info = (client_sock, client_addr, self.__ctx)
				th = WorkingThread(client_info, self.__welcome_name, worker_class=self.__ctx.get('worker_class', Worker))
				th.setDaemon(True)
				th.start()

		for s in self.__sock:
			try:
				s.close()
			except:
				pass
				
	def stop(self):
		self.__run_forest = False


if __name__ == '__main__':
	
	class W(Worker):
		
		def run(self, buffer):
			self.sock.send('\0'+buffer)
			print '-----------', buffer
	
	
	def on_sig(s, st):
		global main_thread
		#~ global ssl_thread
		
		main_thread.stop()
		#~ ssl_thread.stop()
		sys.exit(0)

	signal.signal(signal.SIGINT, on_sig)
	signal.signal(signal.SIGTERM, on_sig)

	context = {
			'app_name':None,
			'bind_ip':'',
			'listen_port':1066,
			#'port_spawn':3,
			'worker_class':W,
		}	

	main_thread = MainThread(context)
	main_thread.run()
	
	#main_thread.join()
#	context = {
#			'app_name':None,
#			'bind_ip':'',
#			'listen_port':1067,
#			'worker_class':Worker,
#			'ssl':True,
#		}	
#	ssl_thread = MainThread(context)
#	ssl_thread.start()
	
	while True:
		time.sleep(0.01)
		

