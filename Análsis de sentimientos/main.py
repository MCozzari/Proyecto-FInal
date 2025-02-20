from fastapi import FastAPI
from pydantic import BaseModel
import joblib

# Cargar el modelo final y el vectorizador con la ruta completa
modelo_final = joblib.load(r'C:\Users\guard\OneDrive\Desktop\Henry Data Science\Proyecto-FInal\Análsis de sentimientos\modelo_sentimientos_final.pkl')
vectorizer = joblib.load(r'C:\Users\guard\OneDrive\Desktop\Henry Data Science\Proyecto-FInal\Análsis de sentimientos\vectorizador_tfidf.pkl')

# Inicializar FastAPI
app = FastAPI()

# Clase para el cuerpo de la petición
class Comentario(BaseModel):
    texto: str

# Endpoint para clasificar el comentario
@app.get("/clasificar_comentario")
def clasificar_comentario(texto: str):
    # Vectorizar el comentario
    texto_tfidf = vectorizer.transform([texto])
    # Predecir el sentimiento
    prediccion = modelo_final.predict(texto_tfidf)[0]
    return {"sentimiento": prediccion}
