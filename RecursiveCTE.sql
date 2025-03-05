-- Query to create table:
CREATE TABLE CaseProgress (
    Center_ID VARCHAR(10),
    Case_ID INT,
    Stage1 DATE,
    Stage2 DATE,
    Stage3 DATE,
    Stage4 DATE,
    Stage5 DATE
);

-- Insert data
INSERT INTO CaseProgress (Center_ID, Case_ID, Stage1, Stage2, Stage3, Stage4, Stage5) VALUES
('C1', 1, '2024-01-01', NULL, NULL, '2024-01-13', NULL),
('C1', 2, '2024-01-05', '2024-01-10', NULL, NULL, NULL),
('C2', 3, NULL, '2024-01-10', NULL, NULL, '2024-01-20'),
('C3', 4, '2024-01-05',  '2024-01-12', '2024-01-12', '2024-01-14', '2024-01-20'),
('C3', 5, '2024-01-10', '2024-01-15', NULL, NULL, NULL),
('C3', 6, NULL, NULL, NULL, '2024-01-15', NULL);

select * from CaseProgress;

select CENTER_ID,
count(coalesce(Stage1, Stage2, Stage3, Stage4, Stage5)) as Stage1,
count(coalesce(Stage2, Stage3, Stage4, Stage5)) as Stage2,
count(coalesce(Stage3, Stage4, Stage5)) as Stage3,
count(coalesce(Stage4, Stage5)) as Stage4,
count(Stage5) as Stage5,
from CaseProgress
group by center_id;

