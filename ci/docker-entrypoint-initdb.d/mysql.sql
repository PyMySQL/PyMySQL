/*!80001 CREATE USER
                  user_sha256   IDENTIFIED WITH "sha256_password" BY "pass_sha256_01234567890123456789",
                  nopass_sha256 IDENTIFIED WITH "sha256_password",
                  user_caching_sha2   IDENTIFIED WITH "caching_sha2_password" BY "pass_caching_sha2_01234567890123456789",
                  nopass_caching_sha2 IDENTIFIED WITH "caching_sha2_password"
                  PASSWORD EXPIRE NEVER */;

/*!80001 GRANT RELOAD ON *.* TO user_caching_sha2 */;
