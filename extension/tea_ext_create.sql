CREATE TRUSTED PROTOCOL tea (
  writefunc     = tea_write,
  readfunc      = tea_read,
  validatorfunc = tea_validate
);
