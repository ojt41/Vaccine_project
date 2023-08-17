CREATE TABLE VaccineType (
  ID VARCHAR(100) NOT NULL,
  name VARCHAR(100) NOT NULL,
  doses INT NOT NULL CHECK (doses > 0),
  tempMin INT NOT NULL, 
  tempMax INT NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE Manufacturer (
  ID VARCHAR(100) NOT NULL,
  country VARCHAR(100) NOT NULL,
  phone Varchar(100) NOT NULL,
  vaccine VARCHAR(100) NOT NULL,
  PRIMARY KEY (ID),
  FOREIGN KEY (vaccine) REFERENCES VaccineType(ID)
);


CREATE TABLE VaccineBatch (
  batchID VARCHAR(100) NOT NULL,
  amount INT NOT NULL,
  type VARCHAR(100) NOT NULL,
  manufacturer VARCHAR(100) NOT NULL,
  manufDate DATE NOT NULL CHECK (manufDate > '2019-12-31'),
  expiration DATE NOT NULL,
  location varchar(100) NOT NULL,
  FOREIGN KEY (type) REFERENCES VaccineType(ID),
  FOREIGN KEY (manufacturer) REFERENCES manufacturer(ID),
  PRIMARY KEY (batchID)
);



CREATE TABLE VaccinationStations (
  name varchar(100) NOT NULL,
  address varchar(100) NOT NULL,
  phone varchar(100) NOT null,
  PRIMARY KEY (name)
);


CREATE TABLE TransportationLog (
  batchID varchar(100) NOT NULL,
  arrival varchar(100) NOT NULL,
  departure varchar(100) NOT NULL,
  dateArr Date NOT NULL,
  dateDep DATE NOT NULL,
  PRIMARY KEY (batchID, arrival, departure, dateDep)
);


CREATE TABLE StaffMembers (
  ssn VARCHAR(100) NOT NULL,
  name VARCHAR(100) NOT NULL,
  birthday Date NOT NULL,
  phone VARCHAR(100) NOT NULL,
  ROLE VARCHAR(100) NOT NULL,
  vaccinationStatus INT NOT NULL,
  hospital VARCHAR(100) NOT NULL,
  PRIMARY KEY (ssn),
  FOREIGN KEY (hospital) REFERENCES vaccinationStations(name)
);

CREATE TABLE Shifts (
  station VARCHAR(100) NOT NULL,
  weekday varchar(100) NOT NULL,
  worker VARCHAR(100) NOT NULL,
  FOREIGN KEY (worker) REFERENCES StaffMembers(SSN),
  PRIMARY KEY (weekday, worker)
);


CREATE TABLE Vaccinations (
  date DATE NOT NULL,
  location VARCHAR(100) NOT NULL,
  batchID VARCHAR(100) NOT NULL,
  PRIMARY KEY (date,location),
  FOREIGN KEY (batchID) REFERENCES VaccineBatch(batchID)
  );


CREATE TABLE Patients (
  ssNo VARCHAR(100) NOT NULL,
  name varchar(100) NOT NULL,
  birthday DATE NOT NULL, 
  gender varchar(5) NOT NULL,
  PRIMARY KEY (ssNo)
  );
 
 CREATE TABLE VaccinePatients (
  date DATE NOT NULL,
  location VARCHAR(100) NOT NULL,
  patientSsNo VARCHAR(100) NOT NULL,
  PRIMARY KEY (date,location,patientSsNo)
);


CREATE TABLE Symptoms (
  name VARCHAR(100) NOT NULL ,
  criticality INT NOT NULL,
  PRIMARY KEY (name)
);


CREATE TABLE Diagnosis (
  patient VARCHAR(100) NOT NULL ,
  symptom VARCHAR(100) NOT NULL,
  date DATE NOT NULL, 
  PRIMARY KEY (patient, symptom, date)
);

select ssn, name, phone, role, vaccinationstatus, hospital -- 1st query
from staffmembers, vaccinations, shifts
where date = '2021-05-10' and worker = ssn and weekday = to_char(date '2021-05-10', 'FMDay')
and station = hospital and location = hospital; 

select ssn, name, phone, vaccinationstatus, hospital  -- 2nd query
from staffmembers, shifts 
where worker = ssn and weekday = 'Wednesday' and role = 'doctor';



select q1.batchid, location, arrival from transportationlog, vaccinebatch, -- 3rd part 1 query
(select batchID, MAX(datearr) as date 
from transportationlog 
group by batchid) as q1
where transportationlog.batchid = q1.batchid and date = datearr 
and vaccinebatch.batchid = transportationlog.batchid and location = arrival;

select q1.batchid, location, arrival, phone from transportationlog, vaccinebatch, vaccinationstations, -- 3rd part2 query
(select batchID, MAX(datearr) as date 
from transportationlog 
group by batchid) as q1
where transportationlog.batchid = q1.batchid and date = datearr 
and vaccinebatch.batchid = transportationlog.batchid and location <> arrival
and location = name;

select distinct patientssno, vaccinations.batchid, type, vaccinepatients.date, vaccinepatients.location  -- 4th query
from vaccinations, symptoms, diagnosis, vaccinepatients, vaccinebatch
  where diagnosis.patient = vaccinepatients.patientssno and
  diagnosis.symptom = symptoms.name and 
  criticality = 1 and
  diagnosis.date > (date '2021-05-10') and
  vaccinepatients.location = vaccinations.location and
  vaccinations.date = vaccinepatients.date and
  vaccinations.batchid = vaccinebatch.batchid;


create view Patient as ( -- 5th query
  select vaccinepatients.patientssno, patients.name, patients.birthday, patients.gender, 
  count(vaccinepatients.patientssno) as VaccinationStatus
  from Patients, vaccinepatients, vaccinetype, vaccinations, vaccinebatch 
  where patients.ssno = vaccinepatients.patientssno and vaccinations.date = vaccinepatients.date and 
  vaccinations.batchid = vaccinebatch.batchid and vaccinebatch.type = vaccinetype.id  
  group by vaccinepatients.patientssno, patients.name, patients.birthday, patients.gender, doses
  having count(vaccinepatients.patientssno) = doses);


select location, type, SUM(doses_left) FROM -- 6th query
(select vaccinations.location, vaccinebatch.type, (vaccinebatch.amount - count(patientssno)) as doses_left
from vaccinebatch right join vaccinations on vaccinebatch.batchid = vaccinations.batchid
join vaccinepatients on vaccinepatients.date = vaccinations.date and vaccinepatients.location = vaccinations.location
group by vaccinations.location, vaccinebatch.batchid, vaccinebatch.type
union all
select location, type, SUM(amount) 
from vaccinebatch
where batchid not in (select batchid from vaccinations)
group by location, type) as together
group by location, type;

select location, SUM(doses_left) from
(select vaccinations.location, (vaccinebatch.amount - count(patientssno)) as doses_left
from vaccinebatch right join vaccinations on vaccinebatch.batchid = vaccinations.batchid
join vaccinepatients on vaccinepatients.date = vaccinations.date and vaccinepatients.location = vaccinations.location
group by vaccinations.location, vaccinebatch.batchid
union all
select location, SUM(amount) 
from vaccinebatch
where batchid not in (select batchid from vaccinations)
group by location) as together
group by location;
 
 

select q2.ID, symptom, cast(singular as float)/total as frequency from -- 7th query
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






