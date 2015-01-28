from Tuple import Tuple
import math

class Vector(Tuple):
	def __init__(self, x=0.0, y=0.0, z=0.0):
		super(Vector, self).__init__(x,y,z)

	def cross(self, a,b):
		self[0]=a[1]*b[2] - a[2]*b[1]
		self[1]=a[2]*b[0] - a[0]*b[2]
		self[2]=a[0]*b[1] - a[1]*b[0]

	def dot(self, other):
		return self[0]*other[0] + self[1]*other[1] + self[2]*other[2]

	def lengthSquared(self):
		return self.v[0]*self.v[0]+self.v[1]*self.v[1]+self.v[2]*self.v[2]

	def length(self):
		return math.sqrt(self.lengthSquared())

	def normalize(self):
		n=self.length()
		self[0]/=n
		self[1]/=n
		self[2]/=n
		