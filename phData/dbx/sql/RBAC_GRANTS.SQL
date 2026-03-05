-- Example groups: `data_engineers`, `analysts`, `business_users`

-- Set CATALOG Permissions 
GRANT USE CATALOG ON CATALOG demo TO `data_engineers`;
GRANT USE CATALOG ON CATALOG demo TO `analysts`;
GRANT USE CATALOG ON CATALOG demo TO `business_users`;

-- Set SCHEMA Permissions for demo.bronze && data_engineers 
GRANT USE SCHEMA ON SCHEMA demo.bronze TO `data_engineers`;
GRANT SELECT, MODIFY ON SCHEMA demo.bronze TO `data_engineers`;

-- Set SCHEMA Permissions for demo.silver && data_engineers, analysts
GRANT USE SCHEMA ON SCHEMA demo.silver TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA demo.silver TO `analysts`;

-- Set SCHEMA Permissions for demo.gold && data_engineers, analysts, business_users
GRANT USE SCHEMA ON SCHEMA demo.gold TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA demo.gold TO `analysts`;
GRANT USE SCHEMA ON SCHEMA demo.gold TO `business_users`;

