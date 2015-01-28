 
import math

class Tuple(object):
	def __init__(self, x=0.0,y=0.0,z=0.0):
		self.v=[x,y,z]
	
	def __getitem__(self, i):
		return self.v[i]
	
	def __setitem__(self, i, data):
		self.v[i]=data
	
	def __add__(self, other):
		return Tuple(self[0]+other[0], self[1]+other[1],self[2]+other[2])
	
	def __iadd__(self, other):
		self[0]+=other[0]
		self[1]+=other[1]
		self[2]+=other[2]
		return self
	
	def __sub__(self, other):
		return Tuple(self[0]-other[0], self[1]-other[1],self[2]-other[2])
	
	def __isub__(self, other):
		self[0]-=other[0]
		self[1]-=other[1]
		self[2]-=other[2]
		return self
	
	def __mul__(self, other): # pairwise multiplication
		return Tuple(self[0]*other[0], self[1]*other[1], self[2]*other[2])
	
	def iscale(self, n):
		self[0]*=n
		self[1]*=n
		self[2]*=n
	
	def scale(self, n):
		return Tuple(self[0]*n, self[1]*n, self[2]*n)
	
	def negate(self):
		self[0]=-self[0]
		self[1]=-self[1]
		self[2]=-self[2]
	
	def toString(self):
		return "{}, {}, {}\n".format(*self.v)
	
	