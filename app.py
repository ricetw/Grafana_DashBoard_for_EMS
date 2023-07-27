import datetime
import json
import os

from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import (
    BondingMachine1, BondingMachine2, ChipRemovalMachine, DrillerMachine1, DrillerMachine2, RollerMachine, SawingMachine
)


tz_delta = datetime.timedelta(hours=8)
tz = datetime.timezone(tz_delta)
current_directory = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(current_directory, "./", ".env")
load_dotenv(dotenv_path)

engine = create_engine(os.environ.get("SQL_SERVER"), echo=True)
Session = sessionmaker(bind=engine)

client = mqtt.Client()


@client.connect_callback()
def on_connect(clinet, userdata, flags, rc):
    print(f"=============== {'Connect':^15} ===============")
    client.subscribe("devices/bonding_machine1/#")
    client.subscribe("devices/bonding_machine2/#")
    client.subscribe("devices/chip_removal_machine/#")
    client.subscribe("devices/driller_machine1/#")
    client.subscribe("devices/driller_machine2/#")
    client.subscribe("devices/roller_machine/#")
    client.subscribe("devices/sawing_machine/#")
    # client.subscribe("devices/#")


@client.message_callback()
def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode('utf-8'))
    # print(f"{msg.topic} - {data}")
    with Session.begin() as session:
        if msg.topic == "devices/bonding_machine1/messages/events/":
            session.add(BondingMachine1(
                status=data["status"],
                demand=data["demand"],
                frequency=data["frequency"],
                Uan=data["Uan"],
                Ubn=data["Ubn"],
                Ucn=data["Ucn"],
                lnAV=data["lnAV"],
                Uab=data["Uab"],
                Ubc=data["Ubc"],
                Uca=data["Uca"],
                Ia=data["Ia"],
                Ib=data["Ib"],
                Ic=data["Ic"],
                IAV=data["IAV"],
                kWa=data["kWa"],
                kWb=data["kWb"],
                kWc=data["kWc"],
                kW=data["kW"],
                kWH=data["kWH"],
                PFAV=data["PFAV"],
                kVarH=data["kVarH"],
                datetime=data["datetime"]
            ))
        elif msg.topic == "devices/bonding_machine2/messages/events/":
            session.add(BondingMachine2(
                status=data["status"],
                demand=data["demand"],
                frequency=data["frequency"],
                Uan=data["Uan"],
                Ubn=data["Ubn"],
                Ucn=data["Ucn"],
                lnAV=data["lnAV"],
                Uab=data["Uab"],
                Ubc=data["Ubc"],
                Uca=data["Uca"],
                Ia=data["Ia"],
                Ib=data["Ib"],
                Ic=data["Ic"],
                IAV=data["IAV"],
                kWa=data["kWa"],
                kWb=data["kWb"],
                kWc=data["kWc"],
                kW=data["kW"],
                kWH=data["kWH"],
                PFAV=data["PFAV"],
                kVarH=data["kVarH"],
                datetime=data["datetime"]
            ))
        elif msg.topic == "devices/chip_removal_machine/messages/events/":
            session.add(ChipRemovalMachine(
                status=data["status"],
                demand=data["demand"],
                frequency=data["frequency"],
                Uan=data["Uan"],
                Ubn=data["Ubn"],
                Ucn=data["Ucn"],
                lnAV=data["lnAV"],
                Uab=data["Uab"],
                Ubc=data["Ubc"],
                Uca=data["Uca"],
                Ia=data["Ia"],
                Ib=data["Ib"],
                Ic=data["Ic"],
                IAV=data["IAV"],
                kWa=data["kWa"],
                kWb=data["kWb"],
                kWc=data["kWc"],
                kW=data["kW"],
                kWH=data["kWH"],
                PFAV=data["PFAV"],
                kVarH=data["kVarH"],
                datetime=data["datetime"]
            ))
        elif msg.topic == "devices/driller_machine1/messages/events/":
            session.add(DrillerMachine1(
                status=data["status"],
                demand=data["demand"],
                frequency=data["frequency"],
                Uan=data["Uan"],
                Ubn=data["Ubn"],
                Ucn=data["Ucn"],
                lnAV=data["lnAV"],
                Uab=data["Uab"],
                Ubc=data["Ubc"],
                Uca=data["Uca"],
                Ia=data["Ia"],
                Ib=data["Ib"],
                Ic=data["Ic"],
                IAV=data["IAV"],
                kWa=data["kWa"],
                kWb=data["kWb"],
                kWc=data["kWc"],
                kW=data["kW"],
                kWH=data["kWH"],
                PFAV=data["PFAV"],
                kVarH=data["kVarH"],
                datetime=data["datetime"]
            ))
        elif msg.topic == "devices/driller_machine2/messages/events/":
            session.add(DrillerMachine2(
                status=data["status"],
                demand=data["demand"],
                frequency=data["frequency"],
                Uan=data["Uan"],
                Ubn=data["Ubn"],
                Ucn=data["Ucn"],
                lnAV=data["lnAV"],
                Uab=data["Uab"],
                Ubc=data["Ubc"],
                Uca=data["Uca"],
                Ia=data["Ia"],
                Ib=data["Ib"],
                Ic=data["Ic"],
                IAV=data["IAV"],
                kWa=data["kWa"],
                kWb=data["kWb"],
                kWc=data["kWc"],
                kW=data["kW"],
                kWH=data["kWH"],
                PFAV=data["PFAV"],
                kVarH=data["kVarH"],
                datetime=data["datetime"]
            ))
        elif msg.topic == "devices/roller_machine/messages/events/":
            session.add(RollerMachine(
                status=data["status"],
                demand=data["demand"],
                frequency=data["frequency"],
                Uan=data["Uan"],
                Ubn=data["Ubn"],
                Ucn=data["Ucn"],
                lnAV=data["lnAV"],
                Uab=data["Uab"],
                Ubc=data["Ubc"],
                Uca=data["Uca"],
                Ia=data["Ia"],
                Ib=data["Ib"],
                Ic=data["Ic"],
                IAV=data["IAV"],
                kWa=data["kWa"],
                kWb=data["kWb"],
                kWc=data["kWc"],
                kW=data["kW"],
                kWH=data["kWH"],
                PFAV=data["PFAV"],
                kVarH=data["kVarH"],
                datetime=data["datetime"]
            ))
        elif msg.topic == "devices/sawing_machine/messages/events/":
            session.add(SawingMachine(
                status=data["status"],
                demand=data["demand"],
                frequency=data["frequency"],
                Uan=data["Uan"],
                Ubn=data["Ubn"],
                Ucn=data["Ucn"],
                lnAV=data["lnAV"],
                Uab=data["Uab"],
                Ubc=data["Ubc"],
                Uca=data["Uca"],
                Ia=data["Ia"],
                Ib=data["Ib"],
                Ic=data["Ic"],
                IAV=data["IAV"],
                kWa=data["kWa"],
                kWb=data["kWb"],
                kWc=data["kWc"],
                kW=data["kW"],
                kWH=data["kWH"],
                PFAV=data["PFAV"],
                kVarH=data["kVarH"],
                datetime=data["datetime"]
            ))


if __name__ == "__main__":
    client.connect(os.environ.get("MQTT_IP"),
                   int(os.environ.get("MQTT_PORT")))
    client.loop_forever()
