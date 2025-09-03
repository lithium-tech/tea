CREATE FOREIGN DATA WRAPPER tea_fdw
  HANDLER tea_fdw_handler
  VALIDATOR tea_fdw_validator
  OPTIONS (mpp_execute 'all segments');

CREATE SERVER tea_server FOREIGN DATA WRAPPER tea_fdw;
