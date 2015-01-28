from Vector import Vector
from Ray import Ray
from Point import Point
from Tuple import Tuple

class Light:
	#point light source
	def __init__(self, p, i): 
		self.pos = p # Point p
		self.intensity = i #rgb intensity tuple
	
	def getRay(self, p):
		return Ray(p, self.pos)

class IntensityRecord: #records a single intersection to calculate color
	def __init__(self, color):
		self.color = color #rgbtuple
		self.intensity = Tuple(0.0,0.0,0.0) #sum of sources
	
	def addSource(self, s):
		self.intensity += s
	
	def getIntensity(self):
		return self.color*self.intensity
