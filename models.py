import math

from django.db.models.functions import ACos, Cos, Sin, Radians
from django.db.models import F
from django.db import models


try:
	from localflavor.us.models import USStateField
except ImportError:
	from django.contrib.localflavor.us.models import USStateField



from django.db.backends.signals import connection_created
from django.dispatch import receiver

from django.db.models import Aggregate, Func
from django.contrib.gis.measure import D

@receiver(connection_created)
def extend_sqlite(sender, **kwargs):
	"""
	Adds the following functions to SQLite: 
	ArcCOS, COS, SIN, Covert to Radians
	"""
	from django.db import connection, transaction
	from django.conf import settings
	cursor = connection.cursor()
	if 'sqlite' in settings.DATABASES['default']['ENGINE']:
		connection.connection.create_function('acos', 1, math.acos)
		connection.connection.create_function('cos', 1, math.cos)
		connection.connection.create_function('radians', 1, math.radians)
		connection.connection.create_function('sin', 1, math.sin)	

def haversine(lon1, lat1, lon2, lat2, use_miles=True):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians 
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

	# haversine formula 
	dlon = lon2 - lon1 
	dlat = lat2 - lat1 
	a = sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
	c = 2 * math.asin(sqrt(a)) 
	if use_miles:
		r = 3956
	else:
		r = 6371 # Radius of earth in kilometers. Use 3956 for miles
	return c * r


# from django.contrib.gis.db.models.functions import Distance

class Distance(Func):

	output_field = models.FloatField()

		
	def __init__(self, location, uom="imperial", **extra_context):		
		self.longitude, self.latitude = location.values()
		if uom == "imperial":
			self.earth_radius = 3959
		else:
			self.earth_radius = 6371
		super().__init__(**extra_context)

	def as_sqlite(self, compiler, connection, **extra_context):
		
		for col in compiler.get_default_columns():
			if 'longitude' == col.target.column:
				# col.alias
				alias, column = col.alias, col.target.column
				identifiers = (alias, column) if alias else (column,)
				extra_context['field_long'] = '.'.join(map(compiler.quote_name_unless_alias, identifiers))
			if 'latitude' == col.target.column:
				alias, column = col.alias, col.target.column
				identifiers = (alias, column) if alias else (column,)
				extra_context['field_lat'] = '.'.join(map(compiler.quote_name_unless_alias, identifiers))
		
		
		if not extra_context['field_lat'] and extra_context['field_long']:
			raise Exception('Model %s requires fields named longitude and latitude' % (compiler.get_default_columns()[0].alias) )

		# extra_context['field_lat'] = '.'.join(["GeoLocations_zip","latitude"])
		# extra_context['field_long'] = '.'.join(["GeoLocations_zip","longitude"])
		
		extra_context['lat'] = self.latitude
		extra_context['long'] = self.longitude
		
		extra_context['earth_radius'] = self.earth_radius
		# haversine with sql.
		template = (
			"%(earth_radius)i * acos( cos( radians(%(lat)f) ) * "
			"cos( radians( %(field_lat)s ) ) * "
			"cos( radians( %(field_long)s ) - radians(%(long)f) ) + "
			"sin( radians(%(lat)f) ) * "
			"sin( radians( %(field_lat)s ) ) )"
		)	
		return self.as_sql(
			compiler, connection,
			template=template,
			**extra_context
		)

class LocationManager(models.Manager):

	def nearby_locations(self, location, radius, max_results=100, uom="imperial"):
		"""
		Search for all zipcodes in a given radius.
		location <GeoLocations.models.Zip>
		radius <int> 
		max_results <int>
		uom <string> Unit of Measure
		"""
		qs = Zip.objects.annotate(
			distance=Distance(location, uom=uom) 
		).filter(distance__lte=radius)

		return qs

	def radius_search_by_zip(self, zipcode, radius=10, uom="imperial"):
		"""
		Find all zip codes in a given radius
		zipcode <string>
		radius <int> 
		uom <string> Unit of Measure
		"""
		zip_obj = Zip.objects.filter(code=zipcode)
		if not zip_obj:
			# raise Exception('Zipcode not found %s' % zipcode)
			return zip_obj		
		location = zip_obj.first().location
		return self.nearby_locations(location, radius, uom=uom).order_by('distance')


# class DistanceAbstractModel(models.Model):
# 	latitude = models.FloatField()
# 	longitude = models.FloatField()

# 	location_manager = LocationManager()

# 	class Meta:
# 		abstract = True

# 	@property
# 	def location(self):		
# 		return {"longitude": self.longitude, "latitude": self.latitude}

# 	@location.setter
# 	def location(self, location):
# 		self.latitude = location['latitude']
# 		self.longitude = location['longitude']

# 	def set_location(self, zip_code):
# 		self.location = Zip.objects.get(code=zip_code).location
	
# 	def nearby_locations(self, radius):
# 		return Zip.location_manager.nearby_locations(self.location, radius)		

class Zip(models.Model):
	code = models.CharField("Zip", max_length=5)
	city = models.CharField("City", max_length=128)
	state = USStateField("State")

	latitude = models.FloatField()
	longitude = models.FloatField()

	location_manager = LocationManager()
	objects = models.Manager()

	class Meta:
		ordering = ["code"]

	def __unicode__(self):
		return "%s: %s" % (self.state, self.code)

	@property
	def location(self):		
		return {"longitude": self.longitude, "latitude": self.latitude}

	@location.setter
	def location(self, location):
		self.latitude = location['latitude']
		self.longitude = location['longitude']

	def set_location(self, zip_code):
		self.location = Zip.objects.get(code=zip_code).location
	
	def nearby_locations(self, radius):
		return Zip.location_manager.nearby_locations(self.location, radius)				