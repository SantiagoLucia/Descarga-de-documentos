import asyncio
from tqdm.asyncio import tqdm
import httpx
import zeep
from zeep.transports import AsyncTransport
import oracledb
from pathlib import Path
import configparser


config = configparser.ConfigParser()
config.read("config.ini")

WS_USER = config["GDEBA"]["WS_USER"]
WS_PASSW = config["GDEBA"]["WS_PASSW"]
TOKEN_URL = config["GDEBA"]["TOKEN_URL"]
WSDL_URL = config["GDEBA"]["WSDL_URL"]
DB_USER = config["GDEBA"]["DB_USER"]
DB_PASSW = config["GDEBA"]["DB_PASSW"]
HOST = config["GDEBA"]["HOST"]
PORT = config["GDEBA"]["PORT"]
SERVICE_NAME = config["GDEBA"]["SERVICE_NAME"]
CANTIDAD_TAREAS = int(config["GDEBA"]["CANTIDAD_TAREAS"])
LIMITE_CONCURRENCIA = int(config["GDEBA"]["LIMITE_CONCURRENCIA"])


async def get_token(user: str, passw: str) -> str:
    """Obtiene el token de autenticación"""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(TOKEN_URL, auth=(user, passw))
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")


class BearerAuth(httpx.Auth):
    """Clase de autenticación Bearer"""
    def __init__(self, token):
        self.token = token

    def auth_flow(self, request):
        """Agrega el token de autenticación a la cabecera de la petición"""
        request.headers["authorization"] = f"Bearer {self.token}"
        yield request

    def update_token(self, new_token):
        """Actualiza el token de autenticación"""
        self.token = new_token


async def get_documento(
        client: zeep.AsyncClient, 
        sem: asyncio.Semaphore, 
        auth: BearerAuth, 
        nro_exp: str, 
        nro_doc: str
        ) -> None:
    """Obtiene un documento de GDEBA"""
    async with sem:
        request = {
            "assignee": False,
            "numeroDocumento": nro_doc,
            "usuarioConsulta": "USERT",
        }
        try:
            response = await client.service.buscarPDFPorNumero(request)
        except:
            new_token = await get_token(WS_USER, WS_PASSW)
            auth.update_token(new_token)
            response = await client.service.buscarPDFPorNumero(request)
        
        with open(f"./descargas/{nro_exp}/{nro_doc}.pdf", "wb") as file:
            file.write(response)


def consultar_documentos(userdb: str, passwdb: str, query: str) -> list:
    """Consulta documentos en base de datos Oracle"""
    try:
        # Parámetros de conexión
        dsn = oracledb.makedsn(HOST, PORT, service_name=SERVICE_NAME)

        # Crear la conexión
        with oracledb.connect(user=userdb, password=passwdb, dsn=dsn) as conn:        
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
            
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"Error de base de datos: {error.message}")


async def main() -> None:
    """Función principal"""
    
    query = """
    select  
    ee.tipo_documento || '-' || ee.anio || '-' || lpad(ee.numero,8,0) || '- -' || 
    ee.codigo_reparticion_actuacion || '-' || ee.codigo_reparticion_usuario as expediente,
    d.numero_sade as documento
    from
    ee_ged.ee_expediente_electronico ee
    inner join ee_ged.ee_expediente_documentos ed on ee.id = ed.id
    inner join ee_ged.documento d on ed.id_documento = d.id
    inner join gedo_ged.gedo_documento gd on d.numero_sade = gd.numero
    inner join ee_ged.trata t on ee.id_trata = t.id
    where
    ee.codigo_reparticion_usuario = 'TESTGDEBA'
    and ee.fecha_creacion > trunc(sysdate)
    order by ee.id
    """

    lista_documentos = consultar_documentos(DB_USER, DB_PASSW, query)
    lista_expedientes = {row[0] for row in lista_documentos}
    
    token = await get_token(WS_USER, WS_PASSW)
    auth = BearerAuth(token)

    async with httpx.AsyncClient(auth=auth) as httpx_client:
        async with zeep.AsyncClient(wsdl=WSDL_URL, transport=AsyncTransport(client=httpx_client)) as async_client:        
            semaforo = asyncio.Semaphore(LIMITE_CONCURRENCIA)

            for expediente in lista_expedientes:
                directory_path = Path(f"descargas/{expediente}")
                directory_path.mkdir(parents=True, exist_ok=True)    
                
                tareas_descarga = []
                for row in lista_documentos:
                    expediente_a_descargar = row[0]
                    documento_a_descargar = row[1]
                    if expediente_a_descargar == expediente:
                        tareas_descarga.append(
                            get_documento(async_client, semaforo, auth, expediente_a_descargar, documento_a_descargar)
                            )
              
                for tarea in tqdm.as_completed(tareas_descarga, desc=expediente):
                    await tarea

if __name__ == "__main__":
    asyncio.run(main())
