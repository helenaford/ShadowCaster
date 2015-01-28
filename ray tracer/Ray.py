from Vector import Vector
import math

class Ray:
	def __init__(self, o=None, d=None):
		if o == None:
			self.o = Vector()
		if d == None:
			self.d = Vector()
		
		self.o = o
		self.d = d
