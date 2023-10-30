import datetime
import json
import os
import paho.mqtt.client as mqtt

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import (
    EnergyConsumption, EM1, EM2, EM3, EM4, EM5, EM6, EM7, EM8, EM9, EM10, EM11, EM12, EM14, EM15
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
em1, em2, em3, em4, em5, em6, em7, em8, em9, em10, em11, em12, em14, em15 = {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

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


def add_EM(tablename, data):
    print(f"{tablename} - {data}")
    try:
        with Session.begin() as session:
            session.add(tablename(
                I1=data["I1"],
                I2=data["I2"],
                I3=data["I3"],
                EffectivePower=data["EffectivePower"],
                ReactivePower=data["ReactivePower"]
            ))
    except Exception as e:
        print('Error: ', e)


def add_EM14(data):
    print(f"EM14 - {data}")
    try:
        with Session.begin() as session:
            session.add(EM14(
                flow_rate=data["flow_rate"],
                flow= data["flow"]
            ))
    except Exception as e:
        print('Error: ', e)


def add_EM15(data):
    print(f"EM15 - {data}")
    try:
        with Session.begin() as session:
            session.add(EM15(
                EM1500=data["EM1500"],
                EM1502=data["EM1502"],
                EM1504=data["EM1504"],
                EM1510=data["EM1510"],
                EM1511=data["EM1511"],
                EM1512=data["EM1512"],
                EM1513=data["EM1513"],
                EM1514=data["EM1514"],
                EM1515=data["EM1515"],
                EM1516=data["EM1516"],
                EM1517=data["EM1517"],
                EM1518=data["EM1518"],
                EM1519=data["EM1519"],
                EM1520=data["EM1520"],
                EM1521=data["EM1521"],
                EM1522=data["EM1522"],
                EM1523=data["EM1523"],
                EM1524=data["EM1524"]
            ))
    except Exception as e:
        print('Error: ', e)


@client.connect_callback()
def on_connect(clinet, userdata, flags, rc):
    print(f"=============== {'Connect':^15} ===============")
    client.subscribe("devices/ZG_GW02_pwrmeter02/messages/events/")
    client.subscribe("devices/ZG/#")


@client.message_callback()
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
    except:
        data = msg.payload.decode('utf-8')
    # print(f"{msg.topic} - {data}")
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
    elif msg.topic == "devices/ZG/EM1":
        em1['I1'] = data['EM100']
        em1['I2'] = data['EM110']
        em1['I3'] = data['EM120']
        em1['EffectivePower'] = data['EM130']
        em1['ReactivePower'] = data['EM140']
    elif msg.topic == "devices/ZG/EM2":
        em2['I1'] = data['EM200']
        em2['I2'] = data['EM210']
        em2['I3'] = data['EM220']
        em2['EffectivePower'] = data['EM230']
        em2['ReactivePower'] = data['EM240']
    elif msg.topic == "devices/ZG/EM3":
        em3['I1'] = data['EM300']
        em3['I2'] = data['EM310']
        em3['I3'] = data['EM320']
        em3['EffectivePower'] = data['EM330']
        em3['ReactivePower'] = data['EM340']
    elif msg.topic == "devices/ZG/EM4":
        em4['I1'] = data['EM400']
        em4['I2'] = data['EM410']
        em4['I3'] = data['EM420']
        em4['EffectivePower'] = data['EM430']
        em4['ReactivePower'] = data['EM440']
    elif msg.topic == "devices/ZG/EM5":
        em5['I1'] = data['EM500']
        em5['I2'] = data['EM510']
        em5['I3'] = data['EM520']
        em5['EffectivePower'] = data['EM530']
        em5['ReactivePower'] = data['EM540']
    elif msg.topic == "devices/ZG/EM6":
        em6['I1'] = data['EM600']
        em6['I2'] = data['EM610']
        em6['I3'] = data['EM620']
        em6['EffectivePower'] = data['EM630']
        em6['ReactivePower'] = data['EM640']
    elif msg.topic == "devices/ZG/EM7":
        em7['I1'] = data['EM700']
        em7['I2'] = data['EM710']
        em7['I3'] = data['EM720']
        em7['EffectivePower'] = data['EM730']
        em7['ReactivePower'] = data['EM740']
    elif msg.topic == "devices/ZG/EM8":
        em8['I1'] = data['EM800']
        em8['I2'] = data['EM810']
        em8['I3'] = data['EM820']
        em8['EffectivePower'] = data['EM830']
        em8['ReactivePower'] = data['EM840']
    elif msg.topic == "devices/ZG/EM9":
        em9['I1'] = data['EM900']
        em9['I2'] = data['EM910']
        em9['I3'] = data['EM920']
        em9['EffectivePower'] = data['EM930']
        em9['ReactivePower'] = data['EM940']
    elif msg.topic == "devices/ZG/EM10":
        em10['I1'] = data['EM1000']
        em10['I2'] = data['EM1010']
        em10['I3'] = data['EM1020']
        em10['EffectivePower'] = data['EM1030']
        em10['ReactivePower'] = data['EM1040']
    elif msg.topic == "devices/ZG/EM11":
        em11['I1'] = data['EM1100']
        em11['I2'] = data['EM1110']
        em11['I3'] = data['EM1120']
        em11['EffectivePower'] = data['EM1130']
        em11['ReactivePower'] = data['EM1140']
    elif msg.topic == "devices/ZG/EM12":
        em12['I1'] = data['EM1200']
        em12['I2'] = data['EM1210']
        em12['I3'] = data['EM1220']
        em12['EffectivePower'] = data['EM1230']
        em12['ReactivePower'] = data['EM1240']
    elif msg.topic == "devices/ZG/EM14":
        em14['flow_rate'] = data['EM1400'].rstrip("0") + "." + data['EM1410']
        em14['flow'] = data['EM1420'] + "." + data['EM1430']
    elif msg.topic == "devices/ZG/EM15":
        em15['EM1500'] = data['EM1500']
        em15['EM1502'] = data['EM1502']
        em15['EM1504'] = data['EM1504']
        em15['EM1510'] = data['EM1510']
        em15['EM1511'] = data['EM1511']
        em15['EM1512'] = data['EM1512']
        em15['EM1513'] = data['EM1513']
        em15['EM1514'] = data['EM1514']
        em15['EM1515'] = data['EM1515']
        em15['EM1516'] = data['EM1516']
        em15['EM1517'] = data['EM1517']
        em15['EM1518'] = data['EM1518']
        em15['EM1519'] = data['EM1519']
        em15['EM1520'] = data['EM1520']
        em15['EM1521'] = data['EM1521']
        em15['EM1522'] = data['EM1522']
        em15['EM1523'] = data['EM1523']
        em15['EM1524'] = data['EM1524']


if __name__ == "__main__":
    client.connect(os.environ.get("MQTT_IP"),
                   int(os.environ.get("MQTT_PORT")))
    task.add_job(add_electric_current, 'interval', seconds=10, args=[electric_current])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM1, em1])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM2, em2])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM3, em3])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM4, em4])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM5, em5])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM6, em6])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM7, em7])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM8, em8])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM9, em9])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM10, em10])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM11, em11])
    task.add_job(add_EM, 'interval', seconds=10, args=[EM12, em12])
    task.add_job(add_EM14, 'interval', seconds=10, args=[em14])
    task.add_job(add_EM15, 'interval', seconds=10, args=[em15])
    client.loop_forever()
