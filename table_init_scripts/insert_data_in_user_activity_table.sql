-- Generate activity for gold users (15-30 activities/month)
INSERT INTO de_assignment.user_activity (user_id, activity_date, activity_type, duration_minutes)
VALUES
-- User 1 (Gold)
(1, CURRENT_DATE - INTERVAL '1 day', 'login', 12),
(1, CURRENT_DATE - INTERVAL '1 day', 'purchase', 8),
(1, CURRENT_DATE - INTERVAL '2 days', 'login', 15),
(1, CURRENT_DATE - INTERVAL '3 days', 'view', 25),
(1, CURRENT_DATE - INTERVAL '5 days', 'login', 10),
(1, CURRENT_DATE - INTERVAL '7 days', 'purchase', 18),
-- User 2 (Gold)
(2, CURRENT_DATE - INTERVAL '1 day', 'login', 20),
(2, CURRENT_DATE - INTERVAL '2 days', 'view', 35),
(2, CURRENT_DATE - INTERVAL '3 days', 'purchase', 15),
(2, CURRENT_DATE - INTERVAL '4 days', 'login', 10),
(2, CURRENT_DATE - INTERVAL '6 days', 'view', 28),
-- User 4 (Silver)
(4, CURRENT_DATE - INTERVAL '1 day', 'login', 8),
(4, CURRENT_DATE - INTERVAL '3 days', 'view', 12),
(4, CURRENT_DATE - INTERVAL '7 days', 'purchase', 6),
(4, CURRENT_DATE - INTERVAL '10 days', 'login', 5),
-- User 7 (Bronze)
(7, CURRENT_DATE - INTERVAL '2 days', 'login', 4),
(7, CURRENT_DATE - INTERVAL '15 days', 'view', 7),
-- Weekend spike pattern
(1, CURRENT_DATE - INTERVAL '7 days', 'purchase', 42), -- Previous Saturday
(2, CURRENT_DATE - INTERVAL '7 days', 'view', 38),
(4, CURRENT_DATE - INTERVAL '7 days', 'login', 22),
-- Some NULL values for data quality checks
(8, CURRENT_DATE - INTERVAL '1 day', NULL, NULL),
(9, NULL, 'login', 5);