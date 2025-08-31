INSERT INTO de_assignment.user_profiles (user_id, signup_date, tier, region)
VALUES
-- Gold tier users (15+ activities)
(1, '2022-01-05', 'gold', 'North America'),
(2, '2022-02-10', 'gold', 'Europe'),
(3, '2022-03-15', 'gold', 'Asia'),
-- Silver tier users (5-15 activities)
(4, '2022-04-20', 'silver', 'North America'),
(5, '2022-05-25', 'silver', 'Europe'),
(6, '2022-06-30', 'silver', 'Asia'),
-- Bronze tier users (<5 activities)
(7, '2022-07-05', 'bronze', 'North America'),
(8, '2022-08-10', 'bronze', 'Europe'),
(9, '2022-09-15', 'bronze', 'Asia'),
-- Additional users with varying activity patterns
(10, '2022-10-20', 'silver', 'North America'),
(11, '2022-11-25', 'gold', 'Europe'),
(12, '2022-12-30', 'bronze', 'Asia');