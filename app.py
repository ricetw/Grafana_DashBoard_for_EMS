import datetime
import json
import os
import paho.mqtt.client as mqtt

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import (
    EnergyConsumption
)

tz_delta = datetime.timedelta(hours=0)
tz = datetime.timezone(tz_delta)
current_directory = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(current_directory, "./", ".env")
load_dotenv(dotenv_path)

engine = create_engine(os.environ.get("SQL_SERVER"), echo=True)
Session = sessionmaker(bind=engine)

client = mqtt.Client()

task = BackgroundScheduler(timezone="Asia/Taipei")

global electric_current
electric_current = {
    "frequency": 60,
    "Uan": 110,
    "Ubn": 110,
    "Ucn": 110,
    "lnAV": 110,
    "Uab": 190.52,
    "Ubc": 190.52,
    "Uca": 190.52,
    "PFAV": 100,
    "kVarH": 0
}

def timenow():
    return datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def add_electric_current(data):
    print(f"electric_current - {data}")
    try:
        with Session.begin() as session:
            session.add(EnergyConsumption(
                frequency=data["frequency"],
                Uan=data["Uan"],
                Ubn=data["Ubn"],
                Ucn=data["Ucn"],
                lnAV=data["lnAV"],
                Uab=data["Uab"],
                Ubc=data["Ubc"],
                Uca=data["Uca"],
                PFAV=data["PFAV"],
                kVarH=data["kVarH"],
                Ia=data["Ia"],
                Ib=data["Ib"],
                Ic=data["Ic"],
                In=data["In"],
                IAV=data["IAV"],
                kWa=data["kWa"],
                kWb=data["kWb"],
                kWc=data["kWc"],
                kW=data["kW"],
                kWH_total=data["kWH_total"],
                kWH_last1H=data["kWH_last1H"],
                demand=data["demand"],
                timestamp=timenow()
            ))
            electric_current.clear()
    except Exception as e:
        print('Error: ', e)
            


@client.connect_callback()
def on_connect(clinet, userdata, flags, rc):
    print(f"=============== {'Connect':^15} ===============")
    client.subscribe("devices/ZG_GW02_pwrmeter02/messages/events/")


@client.message_callback()
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
    except:
        data = msg.payload.decode('utf-8')
    print(f"{msg.topic} - {data}")
    if msg.topic == "devices/ZG_GW02_pwrmeter02/messages/events/":
        data = msg.payload.decode('utf-8').split(" | ")
        for datainfo in data:
            dataout = {}
            for info in datainfo.split("  "):
                dataout[info.split(":")[0]] = info.split(":")[1]
            if dataout["Name"] == "Ia":
                electric_current["Ia"] = float(dataout["Value"])
            elif dataout["Name"] == "Ib":
                electric_current["Ib"] = float(dataout["Value"])
            elif dataout["Name"] == "Ic":
                electric_current["Ic"] = float(dataout["Value"])
            elif dataout["Name"] == "In":
                electric_current["In"] = float(dataout["Value"])
            else:
                electric_current["IAV"] = float(dataout["Value"])
        electric_current["kWa"] = float((electric_current["Ia"]*electric_current["Uan"])/1000)
        electric_current["kWb"] = float((electric_current["Ib"]*electric_current["Uan"])/1000)
        electric_current["kWc"] = float((electric_current["Ic"]*electric_current["Uan"])/1000)
        session = Session()
        dbdata = session.query(EnergyConsumption).order_by(EnergyConsumption.id.desc()).first()
        if dbdata:
            electric_current['kW'] = float(dbdata.kW) + electric_current["kWa"] + electric_current["kWb"] + electric_current["kWc"]
        else:
            electric_current["kW"] = float(electric_current["kWa"]+electric_current["kWb"]+electric_current["kWc"])
        if 0 <= datetime.datetime.now().second < 5 and datetime.datetime.now().minute == 0:
            electric_current["kWH_total"] = electric_current['kW'] /3600
            electric_current["kWH_last1H"] = electric_current["kWH_total"] - float(dbdata.kWH_total)
        elif dbdata == None:
            electric_current["kWH_total"] = 0
            electric_current["kWH_last1H"] = 0
            electric_current["demand"] = 0
        else:
            electric_current["kWH_total"] = float(dbdata.kWH_total)
            electric_current["kWH_last1H"] = float(dbdata.kWH_last1H)
        if electric_current["kWH_last1H"] > float(dbdata.demand):
            electric_current["demand"] = electric_current["kWH_last1H"]
        else:
            electric_current["demand"] = float(dbdata.demand)


if __name__ == "__main__":
    client.connect(os.environ.get("MQTT_IP"),
                   int(os.environ.get("MQTT_PORT")))
    task.add_job(add_electric_current, 'interval', seconds=10, args=[electric_current])
    client.loop_forever()
