CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,   -- Unique identifier for each employee
    ManagerID INT NULL,           -- This refers to the EmployeeID of the manager (NULL if top-level employee)
    EmployeeName VARCHAR(100) NOT NULL -- Name of the employee
);

-- Inserting Sample Data

INSERT INTO Employees (EmployeeID, ManagerID, EmployeeName)
VALUES
(1, NULL, 'Alice'),   
(2, 1, 'Bob'),       
(3, 1, 'Charlie'),   
(4, 2, 'David'),     
(5, 2, 'Eve'),        
(6, 3, 'Frank'),      
(7, 4, 'Grace');


select * from EMPLOYEES;

with cte as(
    select employeeid, managerid, employeename, 1 as Level from employees where managerid is null 
    UNION all
    select a.employeeid, a.managerid, a.employeename, b.Level + 1 from employees as a
    inner join cte as b
    on a.managerid = b.employeeid
)

select * from cte;
