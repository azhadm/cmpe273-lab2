import logging, re, sys
import requests, datetime, operator
import simplejson as json
from spyne import Application, rpc, ServiceBase, Integer, Unicode, String, Float
from spyne import Iterable
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication


logging.basicConfig(level=logging.DEBUG)


class CrimeReport(ServiceBase):
    @rpc(Float, Float, Float, _returns=Iterable(Unicode))
    def checkcrime(self, lat, lon, radius):
        get_url = "https://api.spotcrime.com/crimes.json?lat=" + str(lat) + "&lon=" + str(lon) + "&radius=" + \
                  str(radius) + "&key=."
        get_response = requests.get(get_url)
        json_response = json.loads(get_response.content)
        all_crimes = json_response["crimes"]
        total_crimes = len(all_crimes)

        datadict = {}
        streetnames = {}
        timedict = {'12:01am-3am': 0, '3:01am-6am': 0, '6:01am-9am': 0, '9:01am-12noon': 0, '12:01pm-3pm': 0,
                    '3:01pm-6pm': 0, '6:01pm-9pm': 0, '9:01pm-12midnight': 0}
        pat = r'([\d]*\sblock\s)*([\S\s]+\w)'

        if get_response.status_code == 200:
            for crime in all_crimes:
                if crime['type'] not in datadict:
                    datadict[crime['type']] = 1
                else:
                    datadict[crime['type']] += 1

                crime_date = datetime.datetime.strptime(crime['date'], "%m/%d/%y %I:%M %p")
                if crime_date.replace(hour=0, minute=1) < crime_date <= crime_date.replace(hour=3, minute=0):
                    timedict['12:01am-3am'] += 1
                elif crime_date.replace(hour=3, minute=1) < crime_date <= crime_date.replace(hour=6, minute=0):
                    timedict['3:01am-6am'] += 1
                elif crime_date.replace(hour=6, minute=1) < crime_date <= crime_date.replace(hour=9, minute=0):
                    timedict['6:01am-9am'] += 1
                elif crime_date.replace(hour=9, minute=1) < crime_date <= crime_date.replace(hour=12, minute=0):
                    timedict['9:01am-12noon'] += 1
                elif crime_date.replace(hour=12, minute=1) < crime_date <= crime_date.replace(hour=15, minute=0):
                    timedict['12:01pm-3pm'] += 1
                elif crime_date.replace(hour=15, minute=1) < crime_date <= crime_date.replace(hour=18, minute=0):
                    timedict['3:01pm-6pm'] += 1
                elif crime_date.replace(hour=18, minute=1) < crime_date <= crime_date.replace(hour=21, minute=0):
                    timedict['6:01pm-9pm'] += 1
                else:
                    timedict['9:01pm-12midnight'] += 1

                text = crime['address'].replace('BLOCK BLOCK', 'BLOCK OF').replace('BLOCK OF', 'BLOCK').\
                    replace(' AND ', ' & ')
                match = re.search(pat, text, re.I)
                if match:
                    text, " >>> ", match.group(1), " >>> ", match.group(2)
                    if match.group(2) not in streetnames:
                        streetnames[match.group(2)] = 1
                    else:
                        streetnames[match.group(2)] += 1

            streetnames = sorted(streetnames.items(), key=operator.itemgetter(1), reverse=True)
            dangerousstreets = [streetnames[x][0] for x in range(min(3, len(streetnames)))]
        yield {"the_most_dangerous_streets": dangerousstreets, "total_crime": total_crimes,
               "crime_type_count": datadict, "event_time_count": timedict}


application = Application([CrimeReport], tns='spyne.examples.hello', in_protocol=HttpRpc(validator='soft'),
                          out_protocol=JsonDocument())
if __name__ == '__main__':
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()