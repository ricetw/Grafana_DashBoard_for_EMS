import datetime
import json
import os
import paho.mqtt.client as mqtt

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import (
    EnergyConsumption, EM1, EM2, EM3, EM4, EM5, EM6, EM7, EM8, EM9, EM10, EM11, EM12, EM14, EM15, Kwh
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

# global electric_current
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
em1, em2, em3, em4, em5, em6, em7, em8, em9, em10, em11, em12, em14, em15, kwh = {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

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
                ReactivePower=data["ReactivePower"],
                timestamp=timenow()
            ))
        if tablename == "EM1":
            em1.clear()
        elif tablename == "EM2":
            em2.clear()
        elif tablename == "EM3":
            em3.clear()
        elif tablename == "EM4":
            em4.clear()
        elif tablename == "EM5":
            em5.clear()
        elif tablename == "EM6":
            em6.clear()
        elif tablename == "EM7":
            em7.clear()
        elif tablename == "EM8":
            em8.clear()
        elif tablename == "EM9":
            em9.clear()
        elif tablename == "EM10":
            em10.clear()
        elif tablename == "EM11":
            em11.clear()
        elif tablename == "EM12":
            em12.clear()
    except Exception as e:
        print('Error: ', e)


def add_EM14(data):
    print(f"EM14 - {data}")
    try:
        with Session.begin() as session:
            session.add(EM14(
                flow_rate=data["flow_rate"],
                flow= data["flow"],
                timestamp=timenow()
            ))
        em14.clear()
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
                EM1524=data["EM1524"],
                timestamp=timenow()
            ))
        em15.clear()
    except Exception as e:
        print('Error: ', e)

def add_Kwh(data):
    print(f"KWH - {data}")
    try:
        with Session.begin() as session:
            session.add(Kwh(
                em1=data['em1'],
                em2=data['em2'],
                em3=data['em3'],
                em4=data['em4'],
                em5=data['em5'],
                em6=data['em6'],
                em7=data['em7'],
                em8=data['em8'],
                em9=data['em9'],
                em10=data['em10'],
                em11=data['em11'],
                em12=data['em12'],
                timestamp=timenow()
            ))
        Kwh.clear()
    except Exception as e:
        print("Error: ", e)


def str_to_float(data):
    return float(data) / 100


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
            if electric_current["kWH_last1H"] > float(dbdata.demand):
                electric_current["demand"] = electric_current["kWH_last1H"]
            else:
                electric_current["demand"] = float(dbdata.demand)
        elif dbdata == None:
            electric_current["kWH_total"] = 0
            electric_current["kWH_last1H"] = 0
            electric_current["demand"] = 0
        else:
            electric_current["kWH_total"] = float(dbdata.kWH_total)
            electric_current["kWH_last1H"] = float(dbdata.kWH_last1H)
    elif msg.topic == "devices/ZG/EM1":
        em1['I1'] = str_to_float(data['EM100'])
        em1['I2'] = str_to_float(data['EM110'])
        em1['I3'] = str_to_float(data['EM120'])
        em1['EffectivePower'] = str_to_float(data['EM130'])
        em1['ReactivePower'] = str_to_float(data['EM140'])
        kwh['em1'] = str_to_float(data['EM130'])
    elif msg.topic == "devices/ZG/EM2":
        em2['I1'] = str_to_float(data['EM200'])
        em2['I2'] = str_to_float(data['EM210'])
        em2['I3'] = str_to_float(data['EM220'])
        em2['EffectivePower'] = str_to_float(data['EM230'])
        em2['ReactivePower'] = str_to_float(data['EM240'])
        kwh['em2'] = str_to_float(data['EM230'])
    elif msg.topic == "devices/ZG/EM3":
        em3['I1'] = str_to_float(data['EM300'])
        em3['I2'] = str_to_float(data['EM310'])
        em3['I3'] = str_to_float(data['EM320'])
        em3['EffectivePower'] = str_to_float(data['EM330'])
        em3['ReactivePower'] = str_to_float(data['EM340'])
        kwh['em3'] = str_to_float(data['EM330'])
    elif msg.topic == "devices/ZG/EM4":
        em4['I1'] = str_to_float(data['EM400'])
        em4['I2'] = str_to_float(data['EM410'])
        em4['I3'] = str_to_float(data['EM420'])
        em4['EffectivePower'] = str_to_float(data['EM430'])
        em4['ReactivePower'] = str_to_float(data['EM440'])
        kwh['em4'] = str_to_float(data['EM430'])
    elif msg.topic == "devices/ZG/EM5":
        em5['I1'] = str_to_float(data['EM500'])
        em5['I2'] = str_to_float(data['EM510'])
        em5['I3'] = str_to_float(data['EM520'])
        em5['EffectivePower'] = str_to_float(data['EM530'])
        em5['ReactivePower'] = str_to_float(data['EM540'])
        kwh['em5'] = str_to_float(data['EM530'])
    elif msg.topic == "devices/ZG/EM6":
        em6['I1'] = str_to_float(data['EM600'])
        em6['I2'] = str_to_float(data['EM610'])
        em6['I3'] = str_to_float(data['EM620'])
        em6['EffectivePower'] = str_to_float(data['EM630'])
        em6['ReactivePower'] = str_to_float(data['EM640'])
        kwh['em6'] = str_to_float(data['EM630'])
    elif msg.topic == "devices/ZG/EM7":
        em7['I1'] = str_to_float(data['EM700'])
        em7['I2'] = str_to_float(data['EM710'])
        em7['I3'] = str_to_float(data['EM720'])
        em7['EffectivePower'] = str_to_float(data['EM730'])
        em7['ReactivePower'] = str_to_float(data['EM740'])
        kwh['em7'] = str_to_float(data['EM730'])
    elif msg.topic == "devices/ZG/EM8":
        em8['I1'] = str_to_float(data['EM800'])
        em8['I2'] = str_to_float(data['EM810'])
        em8['I3'] = str_to_float(data['EM820'])
        em8['EffectivePower'] = str_to_float(data['EM830'])
        em8['ReactivePower'] = str_to_float(data['EM840'])
        kwh['em8'] = str_to_float(data['EM830'])
    elif msg.topic == "devices/ZG/EM9":
        em9['I1'] = str_to_float(data['EM900'])
        em9['I2'] = str_to_float(data['EM910'])
        em9['I3'] = str_to_float(data['EM920'])
        em9['EffectivePower'] = str_to_float(data['EM930'])
        em9['ReactivePower'] = str_to_float(data['EM940'])
        kwh['em9'] = str_to_float(data['EM930'])
    elif msg.topic == "devices/ZG/EM10":
        em10['I1'] = str_to_float(data['EM1000'])
        em10['I2'] = str_to_float(data['EM1010'])
        em10['I3'] = str_to_float(data['EM1020'])
        em10['EffectivePower'] = str_to_float(data['EM1030'])
        em10['ReactivePower'] = str_to_float(data['EM1040'])
        kwh['em10'] = str_to_float(data['EM1030'])
    elif msg.topic == "devices/ZG/EM11":
        em11['I1'] = str_to_float(data['EM1100'])
        em11['I2'] = str_to_float(data['EM1110'])
        em11['I3'] = str_to_float(data['EM1120'])
        em11['EffectivePower'] = str_to_float(data['EM1130'])
        em11['ReactivePower'] = str_to_float(data['EM1140'])
        kwh['em11'] = str_to_float(data['EM1130'])
    elif msg.topic == "devices/ZG/EM12":
        em12['I1'] = str_to_float(data['EM1200'])
        em12['I2'] = str_to_float(data['EM1210'])
        em12['I3'] = str_to_float(data['EM1220'])
        em12['EffectivePower'] = str_to_float(data['EM1230'])
        em12['ReactivePower'] = str_to_float(data['EM1240'])
        kwh['em12'] = str_to_float(data['EM1230'])
    elif msg.topic == "devices/ZG/EM14":
        if data['EM1400'] == "00000":
            em14['flow_rate'] = float("0" + "." + str(int(data['EM1410'])))
        else:
            em14['flow_rate'] = float(data['EM1400'].rstrip("0") + "." + str(int(data['EM1410'])))
        em14['flow'] = float(data['EM1420'].rstrip("0") + "." + str(int(data['EM1430'])))
    elif msg.topic == "devices/ZG/EM15":
        em15['EM1500'] = float(data['EM1500'])
        em15['EM1502'] = float(data['EM1502'])
        em15['EM1504'] = float(data['EM1504'])
        em15['EM1510'] = float(data['EM1510'])
        em15['EM1511'] = float(data['EM1511'])
        em15['EM1512'] = float(data['EM1512'])
        em15['EM1513'] = float(data['EM1513'])
        em15['EM1514'] = float(data['EM1514'])
        em15['EM1515'] = float(data['EM1515'])
        em15['EM1516'] = float(data['EM1516'])
        em15['EM1517'] = float(data['EM1517'])
        em15['EM1518'] = float(data['EM1518'])
        em15['EM1519'] = float(data['EM1519'])
        em15['EM1520'] = float(data['EM1520'])
        em15['EM1521'] = float(data['EM1521'])
        em15['EM1522'] = float(data['EM1522'])
        em15['EM1523'] = float(data['EM1523'])
        em15['EM1524'] = float(data['EM1524'])


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
    task.add_job(add_Kwh, 'interval', seconds=10, args=[kwh])
    task.start()
    client.loop_forever()
