import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# Leo gm_sitios parquet crudos
gm_sitios_10PC = pd.read_parquet(r'C:\Users\felip\Desktop\Stuff\Cursos\SoyHenry\Clases\LABS\PF\PF_google_yelp\Proyecto-FInal\Proyecto-FInal\Victoria\25-02-2025\gm_sitios_raw_10PC.parquet' , engine='fastparquet')

# Codigo de Felipe
import os

file_path = r'C:\Users\felip\Desktop\Stuff\Cursos\SoyHenry\Clases\LABS\PF\PF_google_yelp\Proyecto-FInal\Proyecto-FInal\Victoria\25-02-2025\gm_sitios_raw_10PC.parquet'
file_size = os.path.getsize(file_path)  # Tamaño en bytes

print(f"El archivo pesa {file_size / (1024 * 1024):.2f} MB")

# Leo gm_reviews_NJNY 90% y 10%
gm_reviews_10PC = pd.read_parquet('datos/gm_raw_parquet/gm_rev_raw_10PC.parquet' , engine='fastparquet')

# Eliminamos columnas que no serán utilizadas
# NO ELIMINO 'price', 'MISC', PUEDE SERVIR PARA LAS RECOMENDACIONES
# Por ahora conservo latitude y longitude porque puede servirnos para mostrar locales en el dashboard

gm_sitios_10PC = gm_sitios_10PC.drop(columns=['description', 'state', 'relative_results', 'url'])

# selecciono las filas que tienen pizza restaurant
# df_mt_sitios_pizza = df_mt_sitios[df_mt_sitios['category'].apply(lambda x: 'Pizza restaurant' in x)]
# TypeError: argument of type 'NoneType' is not iterable

gm_sitios_10PC_pizza = gm_sitios_10PC[gm_sitios_10PC['category'].apply(lambda x: isinstance(x, list) and 'Pizza restaurant' in x)]

# Extract state (two uppercase letters) using regex
gm_sitios_10PC_pizza['state'] = gm_sitios_10PC_pizza['address'].str.extract(r',\s*([A-Z]{2})\s*\d{5}') 

# selecciono las pizzerias de NJ y NY
gm_sitios_10PC_pizza_NJNY = gm_sitios_10PC_pizza[gm_sitios_10PC_pizza['state'].isin(['NJ', 'NY'])]

# en algunas direcciones dice ', United States' al final / Remove ', United States' if present
gm_sitios_10PC_pizza_NJNY['cleaned_address'] = [addr.replace(", United States", "") for addr in gm_sitios_10PC_pizza_NJNY['address']]

# Regex pattern to extract street address, city, and ZIP code
pattern_1 = r'(?P<street_address_temp>.+),\s*(?P<city>[^,]+),\s*[A-Z]{2}\s*(?P<zip_code>\d{5})$'

# Aplicar regex a la columna 'address'
df_extracted10 = gm_sitios_10PC_pizza_NJNY['cleaned_address'].str.extract(pattern_1)

# Combinar con el DataFrame original
gm_sitios_10PC_pizza_NJNY = gm_sitios_10PC_pizza_NJNY.join(df_extracted10)

# To capture only the part to the right of the comma
gm_sitios_10PC_pizza_NJNY["street_address"] = gm_sitios_10PC_pizza_NJNY["street_address_temp"].str.split(",", n=1).str[1].str.strip()

# elimino las columnas que ya no usamos
# df.drop(['Column1', 'Columns2'], axis=1)
gm_sitios_10PC_pizza_NJNY.drop(['address', 'cleaned_address', 'street_address_temp'], axis=1, inplace=True)

# elimino los duplicados manteniendo la primera instancia
gm_sitios_10PC_pizza_NJNY.drop_duplicates(subset=['gmap_id'], keep='first', inplace=True)

# Convert to dictionary format para extraer los horarios por dia de cada local. La columna es una lista de listas
gm_sitios_10PC_pizza_NJNY['hours_dict'] = gm_sitios_10PC_pizza_NJNY['hours'].apply(lambda x: dict(x) if isinstance(x, list) else {})

# Expand 'hours_dict' into separate columns (one per day)
hours_expanded10 = gm_sitios_10PC_pizza_NJNY['hours_dict'].apply(pd.Series)

# Merge expanded columns back into original DataFrame
gm_sitios_10PC_pizza_NJNY = pd.concat([gm_sitios_10PC_pizza_NJNY, hours_expanded10], axis=1)

# reemplazar '₩', '₩₩' por $ o $$ - esta forma de hacerlo debe ser muy lenta
#replacements = str.maketrans({"h": "H", "e": "E", "o": "O"})
#res = s.translate(replacements)
# string keys in translate table must be of length 1
#gm_sitios_pizza_NJNY['price'] = (
#    gm_sitios_pizza_NJNY['price']
#    .str.replace("₩₩", "$$", regex=False)  # Primero reemplaza ₩₩ por $$
#    .str.replace("₩", "$", regex=False)    # Luego reemplaza ₩ por $
#)
gm_sitios_10PC_pizza_NJNY['price'] = gm_sitios_10PC_pizza_NJNY['price'].replace({'₩': '$', '₩₩': '$$'})

# completo con un diccionario vacios las filas que tienen la columna MISC en NaN
gm_sitios_10PC_pizza_NJNY['MISC'] = gm_sitios_10PC_pizza_NJNY['MISC'].apply(lambda x: {} if pd.isnull(x) else x)

# Expand 'MISC' into separate columns (one per iem)
MISC_expanded10 = gm_sitios_10PC_pizza_NJNY['MISC'].apply(pd.Series)

# elimino aca las columnas que no usare
MISC_expanded10.drop(['Offerings', 'Dining options', 'Health & safety', 'Accessibility', 'Crowd', 'Planning', 
'Payments', 'Highlights','From the business' ], axis=1, inplace=True)

# Merge expanded columns back into original DataFrame
# creo que se concatena por los indices. Por eso no necesita que haya una columna en comun
gm_sitios_10PC_pizza_NJNY = pd.concat([gm_sitios_10PC_pizza_NJNY, MISC_expanded10], axis=1)

# df.drop(['Column1', 'Columns2'], axis=1)
# elimino columnas que ya no necesito
gm_sitios_10PC_pizza_NJNY.drop(['category', 'MISC', 'hours', 'hours_dict'], axis=1, inplace=True)

# Apply function to filter only "Lunch" and "Dinner" and join them as a string
# Handle NaN values and apply filtering
#gm_sitios_pizza_NJNY['Service options'] = gm_sitios_pizza_NJNY['Service options'].apply(
#    lambda x: ', '.join([item for item in x if item in ['Delivery', 'Takeout', 'Dine-in']]) 
#    if isinstance(x, list) else ''
#)
gm_sitios_10PC_pizza_NJNY['Service options'] = gm_sitios_10PC_pizza_NJNY['Service options'].apply(
    lambda x: ','.join([item for item in x if item in ['Delivery', 'Takeout', 'Dine-in']]) 
    if isinstance(x, list) else ''
)

# Apply function to filter only "good for kids" and join them as a string
# Handle NaN values and apply filtering

gm_sitios_10PC_pizza_NJNY['Amenities'] = gm_sitios_10PC_pizza_NJNY['Amenities'].apply(
    lambda x: 'Good for kids' if isinstance(x, list) and 'Good for kids' in x else ''
)

# Apply function to Atmosphere to filter only "casual" and join them as a string
# Handle NaN values and apply filtering

gm_sitios_10PC_pizza_NJNY['Atmosphere'] = gm_sitios_10PC_pizza_NJNY['Atmosphere'].apply(
    lambda x: 'Casual' if isinstance(x, list) and 'Casual' in x else ''
)

# Apply function to filter only "Lunch" and "Dinner" and join them as a string
# Handle NaN values and apply filtering

gm_sitios_10PC_pizza_NJNY['Popular for'] = gm_sitios_10PC_pizza_NJNY['Popular for'].apply(
    lambda x: ','.join([item for item in x if item in ['Lunch', 'Dinner']]) 
    if isinstance(x, list) else ''
)

# Aplicar get_dummies() para convertir en variables dummies
df_dummies_serv10 = gm_sitios_10PC_pizza_NJNY['Service options'].str.get_dummies(sep=',')
df_dummies_am10 = gm_sitios_10PC_pizza_NJNY['Amenities'].str.get_dummies(sep=',')
df_dummies_at10 = gm_sitios_10PC_pizza_NJNY['Atmosphere'].str.get_dummies(sep=',')
df_dummies_pop10 = gm_sitios_10PC_pizza_NJNY['Popular for'].str.get_dummies(sep=',')

# df.drop(['Column1', 'Columns2'], axis=1)
# elimino columnas que ya no necesito
gm_sitios_10PC_pizza_NJNY.drop(['Service options', 'Amenities', 'Atmosphere', 'Popular for'], axis=1, inplace=True)

# Concatenate all DataFrames along columns
gm_sitios_10PC_pizza_NJNY = pd.concat([gm_sitios_10PC_pizza_NJNY, df_dummies_serv10, df_dummies_am10, df_dummies_at10, df_dummies_pop10], axis=1)

# Eliminamos, por ahora, name, pics y resp. no usaremos esas columnas
gm_reviews_10PC = gm_reviews_10PC.drop(columns=['name', 'pics', 'resp'])

# eliminar los duplicados manteniendo la primera instancia
gm_reviews_10PC = gm_reviews_10PC.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first')

# convertir columna time en int64 a datetime
gm_reviews_10PC['date'] = pd.to_datetime(gm_reviews_10PC['time'], unit='ms')

# Eliminamos la columan time
gm_reviews_10PC = gm_reviews_10PC.drop(columns=['time'])

# uno los dataframes de reviews hacer join con el dataframe de sitios /
# asi solo conservare los locales para los que haya reviews y las reviews para las que conozcamos los locales
#df_rev_NJNY = pd.concat([df_rev_NJ, df_rev_NY], ignore_index=True)

# ESTO NO ES NECESARIO PORQUE LEO UN SOLO ARCHIVO

# merge entre los dos dataframe . Luego guardare solo los que tengan datos en ambos.
gm_10PC = pd.merge(gm_sitios_10PC_pizza_NJNY, gm_reviews_10PC, how='inner', on='gmap_id') # tengo que hacer inner join

# vuelvo a separar los archivos para dejarlos listos para la base de datos
# unique_age_city = df[['Age', 'City']].drop_duplicates()
gm_sitios_NJNY_10PC = gm_10PC[['gmap_id', 'name', 'street_address', 'city', 'state', 'zip_code',   'latitude','longitude','avg_rating','num_of_reviews','price',
                   'Monday','Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Delivery', 'Dine-in', 'Takeout', 'Good for kids', 'Casual', 
                   'Dinner', 'Lunch']].drop_duplicates()

# ahora selecciono las reviews
gm_rev_NJNY_10PC = gm_10PC[['gmap_id', 'user_id', 'date', 'rating' , 'text']]

# ESTOS SON LOS ARCHIVOS PROCESADOS. DEJO ESTA CELDA POR SI A ALGUNO LE SIVE

# guardo gm_sitios_NJNY
gm_sitios_NJNY_10PC.to_parquet('datos/gm_raw_parquet/gm_sitios_NJNY_10PC.parquet' , engine='fastparquet', index=False)

# guardo gm_rev_NJNY
gm_rev_NJNY_10PC.to_parquet('datos/gm_raw_parquet/gm_rev_NJNY_10PC.parquet' , engine='fastparquet', index=False)