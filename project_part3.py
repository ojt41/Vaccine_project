import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine, text
import pandas as pd
from psycopg2.errors import DuplicateDatabase
import numpy as np
import matplotlib.pyplot as plt 
from matplotlib.ticker import FormatStrFormatter
from matplotlib import ticker
from datetime import datetime
from dateutil.relativedelta import relativedelta

try:

    database="grp18db_2023"    
    user="grp18_2023"    
    password="6BjZg4fv"   
    host="dbcourse.cs.aalto.fi"
    port = "5432"

    connection = psycopg2.connect(
                                        database=database,              
                                        user=user,       
                                        password=password,   
                                        host=host,
                                        port=port
                                    )
    connection.autocommit = True
    cursor = connection.cursor()

    DIALECT = 'postgresql+psycopg2://'
    db_uri = "%s:%s@%s/%s" % (user, password, host, database)

    engine = create_engine(DIALECT + db_uri)
    psql_conn  = engine.connect()

    if not psql_conn:
        print("DB connection is not OK!")
        exit()
    else:
        print("DB connection is OK.")


    PatientSymptoms = '''
    select ssno, gender, birthday, symptom, date as diagnosisDate
    from patients, diagnosis
    where ssno = patient
    group by ssno, symptom, date;
    '''
    cursor.execute(PatientSymptoms)
    patientSymptoms_df = pd.DataFrame(cursor.fetchall())
    patientSymptoms_df = patientSymptoms_df.rename(columns= {0 : 'ssNo', 1: 'gender', 2: 'dateOfBirth', 3 : 'symptom', 4 : 'diagnosisDate'})
    print(patientSymptoms_df)
    patientSymptoms_df.to_sql(name = "PatientSymptoms", con=psql_conn, if_exists='replace', index = True)

    PatientVaccineInfo = '''
    select distinct ssno, A.date as date1, VB1.type as vaccinetype1, B.date as date2, VB2.type as vaccinetype2
    from patients left join vaccinepatients A on A.patientssno = ssno
    left join vaccinepatients B on (B.date <> A.date) and ssno = B.patientssno
    left join vaccinations C on A.location = C.location 
    and A.date = C.date 
    left join vaccinebatch VB1 on C.batchid = VB1.batchid
    left join vaccinations D on B.location = D.location 
    and B.date = D.date 
    left join vaccinebatch VB2 on D.batchid = VB2.batchid
    where A.date < B.date or B.date isnull;
    '''
    cursor.execute(PatientVaccineInfo)
    patientVaccineInfo_df = pd.DataFrame(cursor.fetchall())
    patientVaccineInfo_df = patientVaccineInfo_df.rename(columns={0 : 'patientssNO', 1: 'date1', 2: 'vaccinetype1', 3 : 'date2', 4 : 'vaccinetype2'})
    print(patientVaccineInfo_df)
    patientVaccineInfo_df.to_sql(name = "PatientVaccineInfo", con=psql_conn, if_exists='replace', index = True)

    femaleSymptoms = patientSymptoms_df.loc[patientSymptoms_df['gender'] == 'F']
    maleSymptoms = patientSymptoms_df.loc[patientSymptoms_df['gender'] == 'M']
    #print(femaleSymptoms)
    #print(maleSymptoms)

    mostCommonF = femaleSymptoms.groupby(['symptom'])['ssNo'].count().reset_index().sort_values(by= 'ssNo', ascending= False).rename(columns={'ssNo' : 'count'})
    mostCommonM = maleSymptoms.groupby(['symptom'])['ssNo'].count().reset_index().sort_values(by= 'ssNo', ascending=False).rename(columns={'ssNo' : 'count'})
    print('Female symptoms')
    print(mostCommonF.iloc[:3])
    print('-' * 40)
    print('Male symptoms')
    print(mostCommonM.iloc[:3])

    patient = '''SELECT * FROM patients;'''
    cursor.execute(patient)
    patient_df = pd.DataFrame(cursor.fetchall())
    patient_df = patient_df.rename(columns={0: 'ssno', 1: 'name', 2: 'birthday', 3: 'gender'})
    

    def age_range(birthday):
        diff = relativedelta(datetime.now(), birthday).years
        if diff <= 10:
            return "0-10"
        elif diff > 10 and diff <= 20:
            return "10-20"
        elif diff > 20 and diff <=40:
            return "20-40"
        elif diff > 40 and diff <= 60:
            return "40-60"
        else:
            return "60+"
        
    patient_df['ageGroup'] = patient_df['birthday'].map(age_range)
    print('Exercise 4')
    print(patient_df)

    vaccStatus_df = patientVaccineInfo_df.loc[:, ['patientssNO', 'date1', 'date2']]
    vaccStatus_df['vacc_count'] = vaccStatus_df.count(axis = 'columns') - 1
    vaccStatus_df = vaccStatus_df.drop(columns=['date1', 'date2']).rename(columns={'patientssNO' : 'ssno'})
    patient_df = pd.merge(patient_df, vaccStatus_df, on= ['ssno'])
    print('Exercise 5')
    print(patient_df)

    vaccination_by_age = patient_df.loc[:, ['ageGroup', 'ssno', 'vacc_count']]
    age_sum = vaccination_by_age.loc[:, ['ageGroup', 'ssno']].groupby(['ageGroup'])['ssno'].count().reset_index().set_index('ageGroup').T.to_dict('list')
    for i in list(age_sum.keys()):
        age_sum[i] = age_sum[i][0]
    vaccination_by_age = vaccination_by_age.groupby(['vacc_count','ageGroup'])['ssno'].count().reset_index()
    vaccination_by_age['percent'] = vaccination_by_age.apply(lambda x: (x['ssno'] / age_sum[x['ageGroup']])*100, axis=1)
    vaccination_by_age = vaccination_by_age.pivot(index= 'vacc_count', columns= 'ageGroup', values= 'percent')
    print(vaccination_by_age)
    total = vaccination_by_age.sum()
    print(total.T.to_dict())
    print("\n" * 2)

    frequencyOfSymptoms = '''
    select q2.ID, symptom, cast(singular as float)/total as frequency from
    (select ID, count(distinct patientssno) as total
    from vaccinetype, vaccinations, vaccinepatients, vaccinebatch
    where vaccinepatients.location = vaccinations.location and
        vaccinepatients.date = vaccinations.date and
        vaccinations.batchid = vaccinebatch.batchid and
        id = type
    group by id) as q1,
    (select ID, symptom, count(distinct patient) as singular
    from vaccinetype, vaccinations, vaccinepatients, diagnosis, symptoms, vaccinebatch
    where vaccinepatients.location = vaccinations.location and
        vaccinepatients.date = vaccinations.date and
        vaccinations.batchid = vaccinebatch.batchid and
        vaccinepatients.patientssno = diagnosis.patient and
        symptoms.name = diagnosis.symptom and 
        diagnosis.date >= vaccinations.date and 
        id = type
    group by ID, symptom
    order by ID, symptom) as q2
    where q1.ID = q2.ID order by q2.ID, symptom;
    '''
    symptoms = '''
    select * from symptoms;
    '''
    cursor.execute(symptoms)
    symptoms_df = pd.DataFrame(cursor.fetchall())
    symptoms_df = symptoms_df.rename(columns={0 : 'name', 1: 'criticality'})

    cursor.execute(frequencyOfSymptoms)
    frequencyOfSymptoms_df = pd.DataFrame(cursor.fetchall())
    frequencyOfSymptoms_df = frequencyOfSymptoms_df.rename(columns={0 : 'id', 1: 'name', 2: 'frequency'})
    frequencyOfSymptoms_df = frequencyOfSymptoms_df.pivot(index= 'name', columns= 'id', values= 'frequency').fillna(0.0).reset_index()

    def relative_frequency(frequency):
        if frequency >= 0.1:
            return "very common"
        elif frequency >= 0.05:
            return "common"
        elif frequency > 0.0:
            return "rare"
        else:
            return "-"

    frequencyOfSymptoms_df['V01'] = frequencyOfSymptoms_df['V01'].map(relative_frequency)
    frequencyOfSymptoms_df['V02'] = frequencyOfSymptoms_df['V02'].map(relative_frequency)
    frequencyOfSymptoms_df['V03'] = frequencyOfSymptoms_df['V03'].map(relative_frequency)

    symptoms_df = pd.merge(symptoms_df, frequencyOfSymptoms_df, on= ['name'], how='left').fillna("-")
    print(symptoms_df)
    print("\n" * 2)

    amounts = '''
    select date, vaccinations.location, amount 
    from vaccinations, vaccinebatch where vaccinations.batchid = vaccinebatch.batchid;'''
    cursor.execute(amounts)
    amount_df = pd.DataFrame(cursor.fetchall())
    amount_df = amount_df.rename(columns={0 : 'date', 1: 'location', 2: 'amount'})

    vaccinationByNumber = '''SELECT * FROM VACCINEPATIENTS;'''
    cursor.execute(vaccinationByNumber)
    vaccinationByNumber_df = pd.DataFrame(cursor.fetchall())
    vaccinationByNumber_df = vaccinationByNumber_df.rename(columns={0 : 'date', 1: 'location', 2: 'patientssno'})
    vacDate_df =vaccinationByNumber_df
    vaccinationByNumber_df = vaccinationByNumber_df.groupby(['date', 'location']).count().reset_index()
    vaccinationByNumber_df = pd.merge(vaccinationByNumber_df, amount_df, on=['date', 'location'])
    vaccinationByNumber_df['percentage'] = vaccinationByNumber_df['patientssno'] / vaccinationByNumber_df['amount']
    print(vaccinationByNumber_df)
    expectedValue = vaccinationByNumber_df['percentage'].mean()
    std = vaccinationByNumber_df['percentage'].std()
    print("\nEstimation of vaccine amout")
    print(20 * "-")
    print(expectedValue + std)

    vacDate_df = vacDate_df.drop_duplicates('patientssno')
    vacDate_df['count'] = 1
    vacDate_df = vacDate_df.drop(columns=['location', 'patientssno']).groupby(['date']).sum()
    vacDate_df = vacDate_df.cumsum().reset_index()
    vacDate_df['date'] = vacDate_df['date'].map(lambda x : x.strftime("%d-%m-%y"))
    print(vacDate_df)

    patientVaccineInfo_df['count'] = (patientVaccineInfo_df.count('columns') - 1)/2
    patientVaccineInfo_df = patientVaccineInfo_df.loc[patientVaccineInfo_df['count'] == 2] \
    .drop(columns=['patientssNO', 'date1', 'vaccinetype1', 'vaccinetype2'])
    patientVaccineInfo_df['count'] = 1
    patientVaccineInfo_df = patientVaccineInfo_df.rename(columns={'date2' : 'date'}).groupby(['date']).sum()
    patientVaccineInfo_df = patientVaccineInfo_df.cumsum().reset_index()
    patientVaccineInfo_df['date'] = patientVaccineInfo_df['date'].map(lambda x : x.strftime("%d-%m-%y"))
    print(patientVaccineInfo_df)
    print("\n")

    met_df = '''
    select distinct worker, name from shifts, staffmembers where
    weekday in (select weekday from shifts where worker = '19740919-7140')
    and station  in (select station from shifts where worker = '19740919-7140')
    and worker <> '19740919-7140'
    and ssn = worker
    union
    select distinct  patientssno, name from vaccinepatients, patients where
    date < '2021.5.15' and date >= '2021.5.05'
    and to_char(vaccinepatients.date, 'FMDay') in (select weekday from shifts where worker = '19740919-7140')
    and location in (select station from shifts where worker = '19740919-7140')
    and ssno = patientssno;'''
    cursor.execute(met_df)
    met_df = pd.DataFrame(cursor.fetchall())
    met_df = met_df.rename(columns={0 : 'ssno', 1: 'name'})
    print(met_df)

    fig =  plt.figure(figsize=(10, 6))
    ax1 = fig.add_subplot(111)
    ax1.plot(vacDate_df['date'], vacDate_df['count'], color = 'b', label = "total number vaccinated")
    ax1.plot(patientVaccineInfo_df['date'], patientVaccineInfo_df['count'], color = 'g', label = 'number of people with 2 doses')

    ax1.set_title('Number of people vaccinated over time')
    ax1.set_ylabel("Count")
    ax1.set_xlabel("Date")
    plt.legend(loc="upper left")
    fig.tight_layout()
    plt.show()



except Exception as e:
        print ("FAILED due to:" + str(e)) 
