# Sección que corresponde a las paqueterías importadas.
from dataclasses import dataclass
from bs4 import BeautifulSoup
import requests
from requests import get
import operator
from functools import reduce
from pymongo import MongoClient
from fake_useragent import UserAgent
import re
from re import sub
from math import ceil
from datetime import datetime


# Sección que corresponde a los decoradores.

def decorador1(funcion): # Conexión a base de datos. Links.
	
	def base_datos(*args,**kwargs):
		
		print('Se abre conexión a la base de datos.')
		MONGO_CONEXION = 'mongodb://localhost'
		cliente = MongoClient(MONGO_CONEXION)
		db = cliente['ScraperCentury21']
		coleccion = db['Links']

		diccionario = funcion(*args,**kwargs)

		coleccion.insert_one(diccionario) 
		cliente.close()
		print('Se cierra la conexión a la base de datos.')
		
	return base_datos

def decorador2(funcion): # Conexión a base de datos. Propiedades.
	
	def base_datos(*args,**kwargs):
		
		print('Se abre conexión a la base de datos.')
		MONGO_CONEXION = 'mongodb://localhost'
		cliente = MongoClient(MONGO_CONEXION)
		db = cliente['ScraperCentury21']
		coleccion = db['Propiedades']

		diccionario = funcion(*args,**kwargs)

		coleccion.insert_many(diccionario) 
		cliente.close()
		print('Se cierra la conexión a la base de datos.')
		
	return base_datos

# Sección que corresponde a las clases.
@dataclass
class ObtenerLinks: # Clase que obtiene todos los links de las propiedades del sitio.

	def obtener_numero_elementos(self,estado):
		self.ua = UserAgent()
		self.headers = {'User-Agent':self.ua.random}
		r=get('https://century21mexico.com/'+estado+'?pagina=1',headers=self.headers,timeout=50)
		encontrar=r.text.find('<strong>Propiedades: 1-')
		numero_elementos=r.text[encontrar+28:encontrar+34]
		return sub("[^0-9]", "",numero_elementos)
	
	def obtener_lista_paginadores(self):
		estados=[
					'Ciudad-De-Mexico',
					'Aguascalientes',
					'Baja-California',
					'Baja-California-Sur',
					'Campeche',
					'Chiapas',
					'Chihuahua',
					'Coahuila',
					'Colima',
					'Durango',
					'Estado-De-Mexico',
					'Guanajuato',
					'Guerrero',
					'Hidalgo',
					'Jalisco',
					'Michoacan',
					'Morelos',
					'Nayarit',
					'Nuevo-Leon',
					'Oaxaca',
					'Puebla',
					'Queretaro',
					'Quintana-Roo',
					'San-Luis-Potosi',
					'Sinaloa',
					'Sonora',
					'Tabasco',
					'Tamaulipas',
					'Tlaxcala',
					'Veracruz',
					'Yucatan'
				]
		numeros=[self.obtener_numero_elementos(estados[posicion]) for posicion in range(len(estados))]
		numeros=list(map(int, numeros))
		paginas = [ceil(numero/21) for numero in numeros]
		listas = ['https://century21mexico.com/{}?pagina={}'.format(estados[posicion],paginador) for posicion in range(len(estados)) for paginador in range(1,paginas[posicion]+1)]
		return listas

	def obtener_21_urls(self,posicion):
		propiedades=[]
		listas = self.obtener_lista_paginadores()
		#print(len(listas))
		r= get(listas[posicion],headers=self.headers,timeout=120)
		html = r.content
		soup = BeautifulSoup(html,'html.parser')
		for div in soup.find_all('div', class_="row propiedades"):
			for div1 in div.find_all('div', class_="card rounded-0"):
				for item in div1.find_all('a'):
					link=item.get('href')
					propiedades.append('https://century21mexico.com{}'.format(link))
		return propiedades

	def obtener_todos_urls(self):
		urls = [self.obtener_21_urls(posicion) for posicion in range(1)] #Aquí debería ser range(len(listas)) para poder obtener todos los links.
		return reduce(operator.concat,urls)

@dataclass
class ObtenerDatos: # Clase que obtiene la información de cada propiedad.

	def limpiarInt(self, info):
		return info.replace('\n','').replace('\t','').replace(' ','').replace(',','')

	#Datos de la propiedad
	def obtener_campos(self, soup):
		lista=[]
		try:
			for x in soup.find_all('div', class_="offset-sm-2 col-sm-8"):
				lista.append(x.getText())
			return lista
		except:
			return ['Lista vacía']

	def obtener_titulo(self,soup):
		titulo=None
		try:
			for x in soup.find_all('main', class_="container content "):

				for y in x.find_all('div', class_="col-md-10 header text-uppercase family-barlowSemiCondensedMedium"):
					titulo=y.getText()
					return titulo

			if not titulo:
				titulo=soup.find('div', class_="col-md-10 header text-uppercase family-barlowSemiCondensedMedium").getText()

			return titulo
		except:
			return None

	def obtener_descripcion(self,soup):
		descripcion=None

		try:
			descripcion = soup.find(class_='col-12 text-center')
			descripcion = descripcion.text

			return descripcion
		except:
			return None

	def obtener_links_imagenes(self,soup):
		imagenes = []

		try:
			for x in soup.find_all('ul', class_="fotosMini"):
				for y in x.find_all('a'):
					imagenes.append(y.get('href'))
			
			imagenes=list(set(imagenes))
			imagenes.sort()

			return imagenes[:15]
		except:
			return []

	def obtener_superficie_terreno(self,campos):
		terreno=None

		try:
			if campos.find('Superficie de terreno:')!=-1:
				inicio=campos.find('Superficie de terreno:')+22
				final=campos.find('m',inicio+2)
				sup=campos[inicio:final]
				sup=sup.replace('\n','')
				terreno= int(self.limpiarInt(sup))

			if (terreno == 1):
				terreno = None

			return terreno
		except:
			return None

	def obtener_superficie_construccion(self,campos):
		construccion=None

		try:
			Info = self.campos
			
			if campos.find('Superficie de construcción:')!=-1:
				inicio=campos.find('Superficie de construcción:')+28
				final=campos.find('m',inicio+2)
				construccion= int(self.limpiarInt(campos[inicio:final]))

			if (construccion == 1):
				construccion = None

			return construccion
		except:
			return None

	def obtener_numero_cuartos(self,campos):
		recamaras=None
		try:
			if campos.find('Recámaras:')!=-1:
				inicio=campos.find('Recámaras:')+10
				final=campos.find('\t',inicio+2)
				recamaras= int(campos[inicio:final])
			
			return recamaras
		except:
			return None

	def obtener_numero_baños_completos(self,campos):
		baño_completo=None
		try:
			if campos.find('Baños completos:')!=-1:
				inicio=campos.find('Baños completos:')+16
				final=campos.find('\t',inicio+2)
				baño_completo=int(campos[inicio:final])
			
			return abs(baño_completo)
		except:
			return None  

	def obtener_numero_baños_medios(self,campos):
		baño_medio=None

		try:

			if campos.find('Medios baños:')!=-1:
				inicio=campos.find('Medios baños:')+13
				final=campos.find('\t',inicio+2)
				baño_medio=int(campos[inicio:final])

			return abs(baño_medio)
		except:
			return None

	def obtener_antiguedad(self,campos):
		antiguedad=None

		try:

			if campos.find('Edad: ')!=-1:
				inicio=campos.find('Edad: ')+5
				final=campos.find('\n\n',inicio)
				antiguedad=int(campos[inicio:final])

			return antiguedad
		except:
			return None

@dataclass
class ScrapearInformacion(ObtenerDatos): # Clase que ejecuta a la clase que obtiene la información y la organiza en un diccionario.

	def verificar_link_valido(self, html):
		if html.find('Aviso terminado') > -1 or html.find('EN CON LOS PROFESIONALES DEL SACO DORADO Y DESCUBRE TODO LO QUE PODEMOS HACER POR TI.') > -1:
			return True

	def hacer_trabajo(self, url):

		response = get(url, headers=self.headers, timeout=120)

		if response.status_code == 200:

			html = response.text
			self.soup = BeautifulSoup(html, 'html.parser')
			self.url = url
			self.campos = self.obtener_campos(self.soup)
			try:
				self.campos=self.campos[0]
			except:
				self.campos=None
			return self.diccionario_informacion()
		else:
			html = response.text
			verificacion = self.verificar_link_valido(html)
			if verificacion:
				return self.diccionario_expirado()
			else:
				return self.diccionario_vacio()

	def diccionario_informacion(self):

		return {
			"origen":{
						"nombre_sitio": "Century21",
						"pagina_web": "https://century21mexico.com",
						"link_propiedad": self.url
					},
			"fecha_creación": datetime.now(),

			"título": self.obtener_titulo(self.soup),
			"descripción": self.obtener_descripcion(self.soup),
			"imágenes": self.obtener_links_imagenes(self.soup),
			"superificie_terreno": self.obtener_superficie_terreno(self.campos),
			"superficie_construcción": self.obtener_superficie_construccion(self.campos),
			"cuartos": self.obtener_numero_cuartos(self.campos),	
			"baños":{
						"medios": self.obtener_numero_baños_medios(self.campos),
						"completos": self.obtener_numero_baños_completos(self.campos)
					},
			"antigüedad": self.obtener_antiguedad(self.campos)
		}

	def diccionario_expirado(self):

		return {
			"origen":{
						"nombre_sitio": "Century21",
						"pagina_web": "https://century21mexico.com",
						"link_propiedad": self.url
					},
			"fecha_creacion": datetime.now(),

			"detalles": "Aviso expirado."
		}

	def diccionario_vacio(self):

		return {
			"origen":{
						"nombre_sitio": "Century21",
						"pagina_web": "https://century21mexico.com",
						"link_propiedad": self.url
					},
			"fecha_creacion": datetime.now(),

			"detalles": "Hubo un problema."
		}

@dataclass
class EjecutarScraper(ObtenerLinks,ScrapearInformacion): # Clase que ejecuta a la clase anterior para todos los links.
	
	#@decorador1
	def ejecutar_links(self):
		urls=self.obtener_todos_urls()
		return urls#{'links':urls}

	@decorador2
	def ejecutar_scraper(self,lista):
		diccionario = [self.hacer_trabajo(link) for link in lista]
		return diccionario

	def ejecutar_scraper_prueba(self):
		lista = ['https://century21mexico.com/casa/venta/Ciudad-De-México/Cuajimalpa-De-Morelos/Bosques-de-Santa-Fe/324651_Casa-de-Lujo-en-Club-de-Golf-Bosques-de-Sta.-Fé']
		diccionario = [self.hacer_trabajo(link) for link in lista]
		return diccionario


# Aquí se instancían las clases

resultado = EjecutarScraper()
links = resultado.ejecutar_links()
propiedades = resultado.ejecutar_scraper(links)
