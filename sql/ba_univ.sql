/* [OT254-19] H group, Sprint 1
   Makes queries to extract data between 09/01/2020 to 02/01/2021 from Universidad
   del Cine and Universidad de Buenos Aires tables. If the task fails, shows error after 5 retries.
   The queries are done from .sql files.
   This is the query to extract data from Universidad de Buenos Aires. */

SELECT
    universidades AS university,
    carreras AS career,
    TO_CHAR(TO_DATE(fechas_de_inscripcion, 'YY-Mon-DD'), 'DD-MM-YYYY') AS inscription_date,
    REVERSE(SPLIT_PART(REVERSE(nombres), '-' , 2)) AS "first_name",
    REVERSE(SPLIT_PART(REVERSE(nombres), '-' , 1))  AS "last_name",
    sexo AS gender,
    fechas_nacimiento,
    AGE(CURRENT_DATE, TO_DATE(fechas_nacimiento, 'YY-Mon-DD')) as age,
    codigos_postales AS postal_code,
    emails AS email
FROM uba_kenedy
WHERE universidades = 'universidad-de-buenos-aires'
AND fechas_de_inscripcion >= '20-Sep-01'
AND fechas_de_inscripcion <= '21-Feb-01';