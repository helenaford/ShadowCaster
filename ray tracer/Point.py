from Tuple import Tuple
import math

class Point(Tuple):
  def __init__(self, x=0.0, y=0.0, z=0.0):
    super(Point, self).__init__(x,y,z)
  
  def distanceTo(self, other):
    return math.sqrt( (self[0]-other[0])**2 +
		      (self[1]-other[1])**2 +
		      (self[2]-other[2])**2 )