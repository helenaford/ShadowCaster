
from Ray import *
from Object import *
from Point import *
from Light import *
from random import random
import boto.sqs
from boto.sqs.message import RawMessage
import boto.utils
from queueOperator import *
import math

class Window:
    def __init__(self, x, y):
        self.xmax=int(x)
        self.ymax=int(y)
        self.viewpoint = Point(0,0,1.2)

        self.window = [[Tuple(0,0,0) for i in range(y)] for i in range(x)]

    def getRay(self, x,y):
        p = Point((float(x)/self.xmax)-0.5, (float(y)/self.ymax)-0.5, 0.0)

        r = Ray()

        r.o=self.viewpoint
        r.d=Vector(*(p-self.viewpoint).v)

        return r

    def outputPPM(self, fname):
        f = open(fname, mode='w')
        f.write("P3\n{} {}\n255\n".format(self.xmax, self.ymax))

        for j in range(self.ymax-1, -1, -1):
            for i in range(self.xmax):
                color = self.window[i][j].v
                color = [min(int(k*255.0), 255) for k in self.window[i][j].v]
                f.write( "{} {} {} ".format(color[0],color[1], color[2]))

            f.write("\n")

        f.close()



class Tracer:
    def __init__(self, x, y):
        self.objects = []
        self.lights = []
        self.w = Window(x,y)

    def addObject(self, o):
        self.objects.append(o)
    def addLight(self, l):
        self.lights.append(l)

    def _trace(self, r, intensityList, recursion):
        r.d.normalize()
        close = float("inf")
        closeObj = None

        for obj in self.objects: #find the closest intersection
            t = obj.intersect(r)
            if t<close and t>0.0:
                close = t
                closeObj = obj

        if closeObj != None: # if there was in fact an intersection do this...
            close-=.000001
            rr,normal = closeObj.getReflectedNormal(r, close) #calculate reflected + normal vector
            intersectPoint = rr.o

            intensityList.append(IntensityRecord(closeObj.color))

            for l in self.lights:
                inshade = False
                Lray = Ray(intersectPoint, Vector(*(l.pos-intersectPoint).v))
                Lray.d.normalize()
                #print "lray: ", Lray.d.v, Lray.o.v
                for obj in self.objects:
                    odist = obj.intersect(Lray)
                    if odist>0.001:
                        #print odist
                        inshade = True
                if not inshade:
                    #Lray.d.negate()
                    Lscale = normal.dot(Lray.d)
                    #print "normal dot lray", normal.dot(Lray.d)
                    #print "normal: ", normal.v, "Lray.d", Lray.d.v
                    if Lscale >0.0001:
                        intensityList[-1].intensity+= l.intensity.scale(Lscale)
                else:
                    intensityList[-1].intensity+=Tuple(.1,.1,.1)

            #so after we do all the lights then we can use the reflected ray to trace again
            if recursion > 0:
                self._trace(rr, intensityList, recursion-1)


    def trace(self,message,q):
        for i in range(self.w.xmax):
            if i%100 == 99:
                    print 'changing visibility'
                    releaseMessage(q,message,120)
                    print 'changed visibility'
            for j in range(self.w.ymax):
                #print i,j,
                
                r = self.w.getRay(i,j)
                intensityList = []
                self._trace(r, intensityList, 4)
                intensity = Tuple(0.0,0.0,0.0)
                while( len(intensityList) > 0 ):
                    curin = intensityList.pop()
                    #print curin.intensity.v
                    intensity+= curin.getIntensity()

                self.w.window[i][j] = intensity




