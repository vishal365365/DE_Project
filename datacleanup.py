from datetime import datetime
import json


def fix_state_name(row):
    # found records having state name like "punjab***" fixing the record
    if row['state_name'][-1] == '*':
        starting_index = row['state_name'].find('*')
        row['state_name'] = row['state_name'][0:starting_index]
    # found records having spelling mistake in state name fixing the record
    elif row['state_name'] == 'Himanchal Pradesh':
        row['state_name'] = 'Himachal Pradesh'
    elif row['state_name'] == 'Orissa':
        row['state_name'] = 'Odisha'
    elif row['state_name'] in ['Dadra and Nagar Haveli','Daman and Diu']:
        row['state_name'] = 'Dadra and Nagar Haveli and Daman and Diu'
    elif row['state_name'] == 'Karanataka':
        row['state_name'] = 'Karnataka'
    elif row['state_name'] == 'Telangana':
            row['state_name'] = 'Telengana'
    
def fix_state_code(row,date):
    # found corupted record having state code 00 raising error so that they cannot inserted into db
    if row['state_code'] =='00':
        raise ValueError
    # found two different state code for Dadra and Nagar Haveli and Daman and Diu with state id
    # 25,26 considering 26 only
    elif row['state_code'] == '25':
        row['state_code'] = '26'

    #record of andhra pardesh coming from date 2022-01-12 has state code 35 which is of
    #Andaman and Nicobar Islands making the record voilating primary key so here chnaged the
    #state_code with 28 which right state code for andhra pardesh
    elif row['state_name'] == 'Andhra Pradesh' and date == '2022-01-12':
         row['state_code'] = '28'
    
         
         
#validating the data type of records according to schema if not the coloumn type will through
#value error which is catched and error is logged into log file
def validate_data_type(row,date):
    datetime.strptime(date, "%Y-%m-%d")
    int(row['state_code'])
    int(row['cured'])
    int(row['death'])
    int(row['positive'])

# one of the file with url https://github.com/datameet/covid19/blob/master/downloads/mohfw-backup/data_json/2020-07-28.json
# does not have json enclosed in square brackets which is throughing json decode error and also not have 
# state name and state code so adding them with the help of state named code
def jsonError(data):
    data = data[:-1]
    data = '['+data+']'
    transformedData = json.loads(data)
    state_codes = {
    "ap": ["Andhra Pradesh", "28"],
    "an": ["Andaman and Nicobar Islands", "35"],
    "ar": ["Arunachal Pradesh", "12"],
    "as": ["Assam", "18"],
    "br": ["Bihar", "10"],
    "ch": ["Chandigarh", "4"],
    "cg": ["Chhattisgarh", "22"],
    "dh": ["Dadra and Nagar Haveli", "26"],
    "dd": ["Daman and Diu", "25"],
    "dl": ["Delhi", "7"],
    "ga": ["Goa", "30"],
    "gj": ["Gujarat", "24"],
    "hr": ["Haryana", "6"],
    "hp": ["Himachal Pradesh", "2"],
    "jk": ["Jammu and Kashmir", "1"],
    "jh": ["Jharkhand", "20"],
    "ka": ["Karnataka", "29"],
    "kl": ["Kerala", "32"],
    "ld": ["Lakshadweep", "31"],
    "mp": ["Madhya Pradesh", "23"],
    "mh": ["Maharashtra", "27"],
    "mn": ["Manipur", "14"],
    "ml": ["Meghalaya", "17"],
    "mz": ["Mizoram", "15"],
    "nl": ["Nagaland", "13"],
    "or": ["Orissa", "21"],
    "pb": ["Punjab", "3"],
    "rj": ["Rajasthan", "8"],
    "sk": ["Sikkim", "11"],
    "tn": ["Tamil Nadu", "33"],
    "tr": ["Tripura", "16"],
    "uk": ["Uttarakhand", "5"],
    "up": ["Uttar Pradesh", "9"],
    "wb": ["West Bengal", "19"]
}
    corrected_data = []
    for row in transformedData:
        state_code = row["value"]['state']
        if state_code in state_codes:
            state_name = state_codes[state_code][0]
            state_no = state_codes[state_code][1]
            cured = row["value"]["cured"]
            death = row['value']['death']
            positive = row['value']['confirmed']

            newrow = {"state_name":state_name,"state_code": state_no,"positive":positive,\
                   "cured":cured,'death':death}
            corrected_data.append(newrow)
    return corrected_data        


