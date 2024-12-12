-- EXEMPLU CLASIC
-- În ambele cazuri trebuie să avem grijă la constrângeri

-- Exemplu clasic, valorile sunt inserate în ordinea de creare a coloanelor
INSERT INTO dbo.employees -- Dacă omitem coloanele, trebuie să respectăm tipul de date
VALUES (
	3 -- Valoare coloana 1
	,'NEW' -- Valoare coloana 2
	,'EMPLOYEE' -- Valoare coloana 3
	,'newemployee@exhample.com' -- Valoare coloana 4
	,'+40712345679' -- Valoare coloana 5
	,'2020-11-22' -- Valoare coloana 6
	,'FI_MGR' -- Valoare coloana 7
	,15002 -- Valoare coloana 8
	,NULL -- Valoare coloana 9
	,NULL -- Valoare coloana 10
	,100 -- Valoare coloana 11
	)
GO

-- Când specificăm numele coloanei, ordinea o alegem noi
INSERT INTO [dbo].[employees]
           ([employee_id]
           ,[first_name]
           ,[last_name]
           ,[email]
           ,[phone_number]
           ,[hire_date]
           ,[job_id]
           ,[salary]
           ,[commission_pct]
           ,[manager_id]
           ,[department_id]) -- Putem să precizăm una sau mai multe coloane
VALUES (
	2 -- Valoare coloana 1
	,'NEW' -- Valoare coloana 2
	,'EMPLOYEE' -- Valoare coloana 3
	,'new.employee@exhample.com' -- Valoare coloana 4
	,'+40712345678' -- Valoare coloana 5
	,'2020-11-22' -- Valoare coloana 6
	,'FI_MGR' -- Valoare coloana 7
	,15000 -- Valoare coloana 8
	,NULL -- Valoare coloana 9
	,NULL -- Valoare coloana 10
	,100 -- Valoare coloana 11
	)
GO


-- Tabela clonă va avea coloane care au numele și tipul de date al rezultatului
-- Toate constrângerile și toți indecșii se pierd (nu se știe de existența lor)
SELECT *
INTO [dbo].[employees_clone]
FROM [dbo].[employees]

INSERT INTO [dbo].[employees_clone]
SELECT *
FROM [dbo].[employees]

-- Ex. 1 Ștergere a tabelei [employees_clone] dacă există, apoi clonare și inserare doar a angajaților noi

SELECT * FROM [dbo].[employees_clone];

INSERT INTO [dbo].[employees_clone]
SELECT * 
FROM [dbo].[employees] AS src
WHERE NOT EXISTS (
    SELECT 1 
    FROM [dbo].[employees_clone] AS dest
    WHERE dest.employee_id = src.employee_id
);
SELECT * FROM [dbo].[employees_clone];

-- Ex. 2 Mărirea salariului angajaților cu 15% doar dacă sunt într-un departament 
-- ce conține un număr par de angajați
UPDATE e
SET e.salary = e.salary * 1.15
FROM [dbo].[employees] e
INNER JOIN (
    SELECT department_id
    FROM [dbo].[employees]
    GROUP BY department_id
    HAVING COUNT(employee_id) % 2 = 0 
) d ON e.department_id = d.department_id;

SELECT * FROM [dbo].[employees];

-- Ex. 3 Ștergerea angajaților din [employees_clone] care au departamentul situat în “US”
DELETE e
FROM [dbo].[employees_clone] e
INNER JOIN [dbo].[departments] d ON e.department_id = d.department_id
INNER JOIN [dbo].[locations] l ON d.location_id = l.location_id
WHERE l.country_id = 'US';

SELECT * FROM [dbo].[employees_clone];

-- Ex. 4 Clonarea tabelei [departments] doar pentru departamentele care au litera "E" 
-- în numele lor și apoi golirea ei folosind TRUNCATE

SELECT *
INTO [dbo].[departments_clone]
FROM [dbo].[departments]
WHERE department_name LIKE '%E%';

TRUNCATE TABLE [dbo].[departments_clone];
SELECT * FROM [dbo].[departments_clone];

-- Ex. 5 Sincronizarea salariului și bonusului în [employees] din [employees_clone]
-- doar pentru angajații cu un [job_id] ce conține litera "A"
MERGE INTO [dbo].[employees] AS t
USING [dbo].[employees_clone] AS s
ON t.employee_id = s.employee_id
WHEN MATCHED AND t.job_id LIKE '%A%' THEN
    UPDATE SET
        t.salary = s.salary,
        t.commission_pct = s.commission_pct;

SELECT * FROM [dbo].[employees];

SELECT *
FROM [dbo].[employees] AS t
WHERE t.job_id LIKE '%A%';


-- Ex. 6 Ne dăm seama că nu este suficient să ștergem tabelele, dorim și să scăpăm de ele. 
-- Scrieți o clauză care face asta. (folosiți DROP)
DECLARE @SQL NVARCHAR(500);
DECLARE @Cursor CURSOR;

-- Inițializarea cursorului pentru a găsi toate tabelele care conțin "CLONE" în nume
SET @Cursor = CURSOR FAST_FORWARD FOR 
    SELECT 'DROP TABLE [' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']'
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME LIKE '%CLONE';

-- Deschiderea cursorului și rularea comenzii de DROP pentru fiecare tabel găsit
OPEN @Cursor;
FETCH NEXT FROM @Cursor INTO @SQL;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT @SQL;  -- Afișează comanda pentru verificare
    EXEC sp_executesql @SQL;
    FETCH NEXT FROM @Cursor INTO @SQL;
END

-- Închiderea și dealocarea cursorului
CLOSE @Cursor;
DEALLOCATE @Cursor;



-- Ex. 7 Scrieți o funcție care întoarce salariul unui angajat și modificați funcția de mai sus.

-- Ex. 8 De ce credeți că este nevoie de SQL Dinamic?


/*
pt fiecare functie afisam urm info:
    1. Popularitatea in fiecare departament -2-3p
ex: sales, 10:1, 20:2, 50:3
    2. Daca pt functie exista sansa de promovare 3p
ex: pt un angajat se mai poate mari salariul cu 10% si sa fie in continuare 
        sub salariul maxim sau daca exista un manager sau un manager de departament cu functia lui)
    3. In medie nu se aloca foarte multi bani pt aceasta functie
ex: este maxim 60% dintre (sal.min+sal.max)/2   
*/
-- Ex. 2
declare @SQL NVARCHAR(500)

declare @Cursor CURSOR
declare @first_name NVARCHAR(50)
declare @last_name NVARCHAR(50)
declare @max_sal INT
declare @min_sal INT
declare @salary INT
declare @eid INT
declare @exists INT
declare @empJID VARCHAR(20)
declare @manJID VARCHAR(20)
declare @num INT

SET @Cursor = CURSOR FAST_FORWARD FOR 
    SELECT e.salary, e.employee_id, max_salary, min_salary, e.job_id, m.job_id
    FROM employees e
    JOIN jobs j on e.job_id = j.job_id
    JOIN employees m on e.manager_id = m.employee_id
   

OPEN @Cursor
FETCH NEXT FROM @Cursor INTO @salary, @eid, @max_sal, @min_sal, @empJID, @manJID
WHILE (@@FETCH_STATUS = 0 OR @exists = 0)
BEGIN
    if (@salary * 1.1 <= @max_sal)
        set @exists = 1
    if (@empJID = @manJID)
        set @exists = 1
    SELECT @num = COUNT(employee_id)
    FROM employees
    WHERE manager_id = @eid
    if (@empJID = @manJID AND @num > 0)
        set @exists = 1
    FETCH NEXT FROM @Cursor INTO @salary, @eid, @max_sal, @min_sal, @empJID, @manJID
END
CLOSE @Cursor

if (@exists = 1)
    print 'Exista sansa de promovare'
else
    print 'Nu exista sansa de promovare'

DEALLOCATE @Cursor