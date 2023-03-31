/* [OT254-19] H group, Sprint 1
   Makes queries to extract data between 09/01/2020 to 02/01/2021 from Universidad
   del Cine and Universidad de Buenos Aires tables. If the task fails, shows error after 5 retries.
   The queries are done from .sql files.
   This is the query to extract data from Universidad del Cine. */

SELECT
    universities AS university,
    careers AS career,
    inscription_dates AS inscription_date,
    REVERSE(SPLIT_PART(REVERSE(names), '-' , 2)) AS "first_name",
    REVERSE(SPLIT_PART(REVERSE(names), '-' , 1))  AS "last_name",
    sexo AS gender,
    AGE(CURRENT_DATE, TO_DATE(birth_dates, 'DD-MM-YYYY')) as age,
    locations AS location,
    emails AS email
FROM lat_sociales_cine
WHERE universities = 'UNIVERSIDAD-DEL-CINE'
AND TO_DATE(inscription_dates, 'DD-MM-YYYY')
BETWEEN '01-09-2020' AND '01-02-2021';