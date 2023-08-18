import requests
import xml.etree.ElementTree as Xet

import pandas as pd
import time
import requests

import time
import pandas as pd

output_path = "/home/vishal/Documents/git/dags/src/traffic_data/data/"


def traffic_xml2csv(root):
    sample_12 = []
    sample_9 = []
    for child in root:
        if child.tag == "fecha_hora":
            continue
        sample = []
        for i in range(len(child)):
            sample.append(child[i].text)
        if len(child) == 12:
            sample_12.append(sample)
        else:
            sample_9.append(sample)

    cols1 = [
        "idelem",
        "descripcion",
        "accesoAsociado",
        "intensidad",
        "ocupacion",
        "carga",
        "nivelServicio",
        "intensidadSat",
        "error",
        "subarea",
        "st_x",
        "st_y",
    ]
    cols2 = [
        "idelem",
        "intensidad",
        "ocupacion",
        "carga",
        "nivelServicio",
        "velocidad",
        "error",
        "st_x",
        "st_y",
    ]
    data1 = pd.DataFrame(sample_12, columns=cols1)
    data2 = pd.DataFrame(sample_9, columns=cols2)
    data = pd.concat([data1, data2], axis=0)

    return data


def get_feed():
    timestr = time.strftime("%Y%m%d-%H%M%S")
    url = "https://informo.madrid.es/informo/tmadrid/pm.xml"
    try:
        resp = requests.get(url, allow_redirects=True)
        root = Xet.fromstring(str(resp.content, "utf-8"))
        data = traffic_xml2csv(root)
        data.to_csv(
            output_path + "mtd_" + timestr + ".csv",
            index=False,
        )
    except:
        print("Oops!  That was no valid data. Try again...\n\n" + resp.content)


if __name__ == "__main__":
    get_feed()
