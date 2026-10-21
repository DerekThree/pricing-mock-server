create role account_app login password 'account_test' nosuperuser nocreatedb nocreaterole;
grant connect, create on database account_store to account_app;
grant usage, create on schema public to account_app;
