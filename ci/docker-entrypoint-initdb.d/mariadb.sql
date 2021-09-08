/*M!100401 CREATE USER nopass_ed25519 IDENTIFIED VIA ed25519 USING PASSWORD('') */;
/*M!100401 CREATE USER user_ed25519 IDENTIFIED VIA ed25519 USING PASSWORD('pass_ed25519') */;
/*M!100122 INSTALL SONAME "auth_ed25519" */;
--
-- MariaDB [(none)]> select  ed25519_password("");
-- +---------------------------------------------+
-- | ed25519_password("")                        |
-- +---------------------------------------------+
-- | 4LH+dBF+G5W2CKTyId8xR3SyDqZoQjUNUVNxx8aWbG4 |
-- +---------------------------------------------+
--

/*M!100401 CREATE USER nopass_ed25519 IDENTIFIED VIA ed25519 USING '4LH+dBF+G5W2CKTyId8xR3SyDqZoQjUNUVNxx8aWbG4' */;
/*M!100401 CREATE USER user_ed25519 IDENTIFIED VIA ed25519 USING PASSWORD('pass_ed25519') */;
/*M!100122 CREATE FUNCTION ed25519_password RETURNS STRING SONAME "auth_ed25519.so" */;
/*M!100203 EXECUTE IMMEDIATE CONCAT('CREATE USER IF NOT EXISTS nopass_ed25519 IDENTIFIED VIA ed25519 USING "', ed25519_password("") ,'";') */;
/*M!100203 EXECUTE IMMEDIATE CONCAT('CREATE USER IF NOT EXISTS user_ed25519 IDENTIFIED VIA ed25519 USING "', ed25519_password("pass_ed25519") ,'";') */;
