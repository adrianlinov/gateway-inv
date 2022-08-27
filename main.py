from sys import path
from time import sleep
import time
from SX127x.LoRa import *
from SX127x.board_config import BOARD
import json
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from datetime import datetime
from colorama import Fore, Style
import threading
import requests
import psycopg2


# Clase que maneja modulo LoRa
class LoraTransiver(LoRa):

    # Incializar modulo LoRa
    def __init__(self, verbose=False):
        # Inicializar clase padre
        super(LoraTransiver, self).__init__(verbose)
        # Colocar el modulo en modo SLEEP y configurar DIO
        self.set_mode(MODE.SLEEP)
        self.set_dio_mapping([0] * 6)
        # Inicializar base de datos
        self.db = DBManager()

    def start(self):
        # Preparar el modulo para recibir datos
        self.reset_ptr_rx()
        self.set_mode(MODE.RXCONT)
        while True:
            sleep(.1)
            sys.stdout.flush()

    # Funcion ejecutada al recibir datos
    # Lee los datos, los convierte a string y ejecuta la función onRecive
    def on_rx_done(self):
        print("\nInformación Recibida: ")
        self.clear_irq_flags(RxDone=1)
        payload = self.read_payload(nocheck=True)

        try:
            strPayload = bytes(payload)[4:].decode("utf-8")
        except:
            strPayload = None
        self.onRecive(strPayload)
        self.set_mode(MODE.SLEEP)
        self.reset_ptr_rx()
        self.set_mode(MODE.RXCONT)

    # Funcion que carga la data recibida por el modulo a la base de datos local y nube
    def onRecive(self, payload):
        if payload != None:
            if self.db.verifyPayload(payload) == True:
                try:
                    print("Cargando Payload")
                    print(payload)
                    self.db.beginSession()
                    self.db.insertPayload(payload)
                    self.db.closeSession()
                    print(f"{Fore.GREEN}Data Cargada Correctamente{Style.RESET_ALL}".
                          capitalize())
                except Exception as e:
                    print(e)
                    print(
                        f"{Fore.RED}No se pudo cargar la data{Style.RESET_ALL}"
                        .capitalize())

            else:
                print(f"{Fore.RED}Error en el payload{Style.RESET_ALL}".
                      capitalize())


class DBManager():

    def __init__(self):
        # Inicializa los endpoints de la base de datos y crea un hilo paralelo
        # que se encarga de sincronizar la base de datos local con la de nube
        self.base = automap_base()
        self.syncBase = automap_base()
        self.cloudBase = automap_base()
        # self.engine = create_engine(
        #     "postgresql://pi:adrian486@raspberrypi.local/POOLFARMDB")
        # self.syncEngine = create_engine(
        #     "postgresql://pi:adrian486@raspberrypi.local/POOLFARMDB")
        # self.cloudEngine = create_engine(
        #     "postgresql://pi:adrian486@raspberrypi.local/POOLFARMDB_CLOUD")
        self.engine = create_engine(
            "postgresql://pi:adrian486@raspberrypi.local/POOLFARMDB")
        self.syncEngine = create_engine(
            "postgresql://pi:adrian486@raspberrypi.local/POOLFARMDB")
        self.cloudEngine = create_engine(
            "postgresql://postgres:poolfarm12@132.145.173.95/poolfarm")
        # self.engine = create_engine("postgresql://postgres:poolfarm12@poolfarmdb.ctxzqyue9fjw.us-east-1.rds.amazonaws.com/poolfarmdb_test")
        # self.cloudEngine = create_engine("postgresql://postgres:poolfarm12@poolfarmdb.ctxzqyue9fjw.us-east-1.rds.amazonaws.com/poolfarmdb")
        self.base.prepare(self.engine, reflect=True)
        self.syncBase.prepare(self.syncEngine, reflect=True)
        
        syncThread = threading.Thread(
            name="Sync Thread", target=self.initSyncCloudDatabase)
        syncThread.start()

    # Funciones que inicializan o cierran las sesiones con las bases de datos
    def beginSession(self):
        self.session = Session(self.engine)

    def closeSession(self):
        self.session.close()

    def beginSyncSession(self):
        self.syncSession = Session(self.syncEngine)

    def closeSyncSession(self):
        self.syncSession.close()

    def beginCloudSession(self):
        self.cloudSession = Session(self.cloudEngine)

    def closeCloudSession(self):
        self.cloudSession.close()

    def initSyncCloudDatabase(self):
        while True:
            try:
                self.cloudBase.prepare(self.cloudEngine, reflect=True)
                break
            except:
                pass
        self.syncCloudDatabase()
    
    # Funcion que verifica se hay datos para sincronizar y en caso existan realiza la
    # sincronizacion.
    def syncCloudDatabase(self):
        requiereSync = True
        while True:
            if requiereSync == True:
                if (self.checkInternetConnection() == True):
                    try:
                        self.beginSyncSession()
                        self.beginCloudSession()
                        self.syncCenterTable()
                        self.syncPoolTable()
                        self.syncMsTypeTable()
                        self.syncMeasurementTable()
                        self.closeSyncSession()
                        self.closeCloudSession()
                        requiereSync = False
                        syncedTime = datetime.now()
                        print(f"{Fore.BLUE}Data Sincronizada Correctamente{Style.RESET_ALL}".
                          capitalize())
                    except psycopg2.errors.UniqueViolation as e:
                        self.syncSession.execute("SELECT pg_catalog.setval(pg_get_serial_sequence('measurement', 'id'), MAX(id)) FROM measurement;")
                        self.syncSession.commit()
                    
                    except psycopg2.OperationalError as e:
                        try:
                            self.closeSyncSession()
                            self.closeCloudSession()
                        finally:
                            self.syncCloudDatabase()

                    finally:
                        self.closeSyncSession()
                        self.closeCloudSession()

                        
            else:
                # Verifica si han pasado 10 segundos desde la ultima sincronizacion para
                # volver a verificar 
                if (datetime.now() - syncedTime).seconds > 10:
                    requiereSync = True


    # Funcion para sincronizar la tabla CENTER
    def syncCenterTable(self):
        center = self.syncBase.classes.center
        centersLocalDataframe = self.syncSession.query(
            center).filter(center.synced == None)
        for fila in centersLocalDataframe:
            new = self.cloudBase.classes.center(
                id=fila.id,
                phone=fila.phone,
                address=fila.address,
                department=fila.department,
                province=fila.province,
                gis=fila.gis,
                name=fila.name,
            )
            print(fila)
            self.cloudSession.add(new)
        self.cloudSession.commit()
        for fila in centersLocalDataframe:
            fila.synced = True
            self.syncSession.add(fila)
        self.syncSession.commit()

    # Funcion para sincronizar la tabla POOL
    def syncPoolTable(self):
        pool = self.syncBase.classes.pool
        poolsLocalDataframe = self.syncSession.query(
            pool).filter(pool.synced == None)
        for fila in poolsLocalDataframe:
            new = self.cloudBase.classes.pool(
                id=fila.id,
                name=fila.name,
                specie=fila.specie,
                volume=fila.volume,
                prod_density=fila.prod_density,
                texture=fila.texture,
                structure=fila.structure,
                taxonomy=fila.taxonomy,
                hydraulic_conduct=fila.hydraulic_conduct,
                center_id=fila.center_id,
            )
            print(fila)
            self.cloudSession.add(new)
        self.cloudSession.commit()
        for fila in poolsLocalDataframe:
            fila.synced = True
            self.syncSession.add(fila)
        self.syncSession.commit()

    # Funcion para sincronizar la tabla TYPE
    def syncMsTypeTable(self):
        ms_type = self.syncBase.classes.ms_type
        ms_typesLocalDataframe = self.syncSession.query(
            ms_type).filter(ms_type.synced == None)
        for fila in ms_typesLocalDataframe:
            new = self.cloudBase.classes.ms_type(
                id=fila.id,
                type=fila.type,
                unit=fila.unit,
            )
            print(fila)
            self.cloudSession.add(new)
        self.cloudSession.commit()
        for fila in ms_typesLocalDataframe:
            fila.synced = True
            self.syncSession.add(fila)
        self.syncSession.commit()

    # Funcion para sincronizar la tabla MEASUREMENT
    def syncMeasurementTable(self):
        measurment = self.syncBase.classes.measurement
        measurementsLocalDataframe = self.syncSession.query(
            measurment).filter(measurment.synced == None)
        print(measurementsLocalDataframe.count())
        for fila in measurementsLocalDataframe:
            new = self.cloudBase.classes.measurement(
                value=fila.value,
                ms_timestamp=fila.ms_timestamp,
                pool_id=fila.pool_id,
                ms_type_id=fila.ms_type_id,
            )
            print(fila)
            self.cloudSession.add(new)
        self.cloudSession.commit()
        for fila in measurementsLocalDataframe:
            fila.synced = True
            self.syncSession.add(fila)
        self.syncSession.commit()


    # Verifica si hay conexion a internet haciendo una peticion a google.com
    def checkInternetConnection(self):
        try:
            url = "http://google.com/"
            _ = requests.get(url, timeout=10)
            return True
        except requests.ConnectionError:
            return False

    # Inserta la carga en la base de datos
    def insertPayload(self, payload):
        payload = payload.split("}-")
        payload[0] = str(payload[0]) + "}"
        jSonDic = json.loads(payload[0])
        ms_type = self.base.classes.ms_type
        ms_typeDataframe = self.session.query(ms_type).all()
        for fila in ms_typeDataframe:
            if (fila.type in jSonDic.keys()):
                measurement = self.base.classes.measurement(
                    value=jSonDic[fila.type],
                    ms_timestamp=datetime.now(),
                    pool_id=1,
                    ms_type_id=fila.id)
                self.session.add(measurement)
        self.session.commit()

    # Realiza una operacion de checksum para validar la integridad de la data
    def verifyPayload(self, payload):
        validate = payload.split("}-")
        validate[0] = str(validate[0]) + "}"
        print(sum(bytearray(validate[0], encoding='utf8')), " - ",
              int(validate[1]))
        if sum(bytearray(validate[0], encoding='utf8')) == int(validate[1]):
            return True
        else:
            return False


def main():
    BOARD.setup()
    print(f"{Fore.YELLOW}LORA START{Style.RESET_ALL}")
    lora = LoraTransiver(verbose=True)
    lora.set_mode(MODE.STDBY)
    lora.set_pa_config(pa_select=1)

    try:
        lora.start()

    except KeyboardInterrupt:
        sys.stdout.flush()
        print("")
        lora.db.closeSession()
        sys.stderr.write("KeyboardInterrupt\n")

    finally:
        sys.stdout.flush()
        print("")
        lora.set_mode(MODE.SLEEP)
        BOARD.teardown()


if __name__ == '__main__':
    main()

# REFERENCIAS:
# https://circuitdigest.com/microcontroller-projects/raspberry-pi-with-lora-peer-to-peer-communication-with-arduino
