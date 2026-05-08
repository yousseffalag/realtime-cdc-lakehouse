-- Seed data for payments
INSERT INTO payments (order_id, payment_method, amount, payment_status) VALUES
(1, 'card', 1119.97, 'completed'),
(2, 'paypal', 799.00, 'completed'),
(4, 'bank_transfer', 360.00, 'completed');
