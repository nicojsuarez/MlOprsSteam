
from fastapi import FastAPI
import pandas as pd

from fastapi.responses import RedirectResponse # esta libreria funciona para la url.

app=FastAPI()

# Cargar los datos desde los archivos CSV y Parquet
genre_data = pd.read_csv(r'C:\Users\Administrador\Desktop\vitual\MlOprsSteam\genre.csv')
items_data = pd.read_parquet(r'C:\Users\Administrador\Desktop\vitual\MlOprsSteam\items1.parquet')
reviews_data = pd.read_csv(r'C:\Users\Administrador\Desktop\vitual\MlOprsSteam\reviews.csv')
steam_data = pd.read_csv(r'C:\Users\Administrador\Desktop\vitual\MlOprsSteam\steam.csv')


@app.get("/", include_in_schema=False)  # esta funcion genera que la url ya venga con /doc*/ haciendo que se abra directo el API. 
async def redirect_to_docs():
    return RedirectResponse("/docs")


# Se definen los endpoints de la API:

@app.get("/Gracias por entrar a mi api, me tomo muchas lagrimas :') ") # Funcion que saluda a los ingresantes.
def index():
        return {"message": "¡HOLA, Bienvenido!"}

@app.get('/desarrollador/ {developer: str}')  # Cantidad de items de los desarrolladores.
def desarrollador(developer: str):
   
    developer_found = steam_data[steam_data['developer'] == developer]
    
    if not developer_found.empty:
        item_id = developer_found.iloc[0]['item_id']
        return {'developer': developer, 'item_id': int(item_id)}
    else:
        return {'message': "Desarrollador NO encontrado"}, 404

@app.get('/userdata/ {user_id: str}') # Debe devolver cantidad de dinero gastado por el usuario, el porcentajede recomendación en base a reviews.recommend y cantidad de items.
def userdata (user_id :str):
    if user_id in reviews_data:
        return reviews_data[user_id]
    else: 
        return 'user NO encontrado', 404

@app.get('/UserForGenre/ {genero: str}') 
def UserForGenre( genero: str ): 
     return

@app.get('/best_developer_year/ {año: int}') 
def best_developer_year( año: int ):
     return
     
@app.get('/desarrollador_reviews_analysis/ {desarrolladora: str}') 
def desarrollador_reviews_analysis( desarrolladora: str ):
     return

# Modelo de aprendizaje automatico: 


@app.get('/ recomendacion_juego/ {id_de_producto}') 
def recomendacion_juego(id_producto):
        return

@app.get('/recomendacion_usuario/ {id_de_usuario}') 
def recomendacion_usuario(id_de_usuario ) :
     return


