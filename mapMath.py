
import math

  
class mapMath:
    
  
    def latlongToIndex(self, lat, long, res=1):
        """
        Get the grid index related to input long/lat and grid resolution
    
        Parameters 
            long = range -180 -> 180
            lat  = range  -90 -> 90
            res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5
        
         NOTE to get grid index related to geo coordinates ((-180,-90),(-179,-89)), input is in the range:
           -179 >= long > -180
           -89  >= lat  > -90
        """        
        x,y = self.latlongToGridCoordinate(lat, long, res)
        return self.gridCoordinateToIndex(x,y, res)


    def latlongToGridCoordinate(self, lat, long, res=1):
        """
        Convert input longitude and latitude input to coordinates in the grid.
        The grid is a matrix from representing the -180,-90 to 180,90 map. With 1 as resolution the matrix has 64800 cells
        Starting with coordinates 1,1 
    
       Parameters 
            long = range -180 -> 180
            lat  = range  -90 -> 90
            res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5
        """        
        lat  = lat  + 90   # Adjust from range -90 -> 90   to 0 -> 180
        long = long + 180  # Adjust from range -180 -> 180 to 0 -> 360

        # round down to index number
        roundlat  = float(res * math.ceil(lat / res ))
        roundlong = float(res * math.ceil(long / res))

        y = roundlat // res
        x = roundlong // res
        
        return int(x), int(y)


    def gridCoordinateToIndex(self, x, y, res=1): 
        """
        Get the index of specific grid-coordinate, given resolution
    
        Parameters 
            x = long, range with res = 1, 1 -> 360
            y = lat, range with res = 1, 1 -> 180
            res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5
    
        """        
        return int((x-1) * 180/res + y)



    def indexToGridCoordinate(self,index,res=1): #returns cartesian grid coordinates, given index and resolution
        """
        Get cartesian grid coordinates, given index and resolution
    
        Parameters 
            index, with res = 1, 1 -> 64800
            res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5
    
        """        
        
        lat_range  = res*180
    
        x_loc  = (index-1)//lat_range+1
        y_loc   = (index-1)%lat_range+1

        return x_loc,y_loc


    def indexToMapCoordinate(self,index,res=1):
        """
        Get longetude & latetude based on index
    
        Parameters 
            index, with res = 1, 1 -> 64800
            res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5
    
        """        
        x,y = self.indexToGridCoordinate(index,res)
        longitude = -180 + x/res 
        latitude  = -90 + y/res

        return longitude, latitude

 
    def cornerCoordinatesToAllCoordinates(self,resolution=1,*corners):  #tuple of tuples ->((x1,y1),(x2,y2)) or ((x1,x2))
        """
        """        
        x1,y1 = corners[0][0],corners[0][1]
        if len(corners)==1:
            x2,y2 = corners[0][0], corners[0][1]
        else:
            x2,y2 = corners[1][0],corners[1][1]
        boxCoords  = [] #list of all coordinate tuples within the box
        boxIndexes = [] #list of all indexes within the box
        for x in range(min(x1,x2),max(x1,x2)+1):
            for y in range(min(y1,y2),max(y1,y2)+1):
                boxCoords.append((x,y))
                boxIndexes.append(self.gridCoordinateToIndex(x,y,resolution))
        return boxCoords,boxIndexes

