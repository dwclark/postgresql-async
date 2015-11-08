create or replace function my_cursor_func() returns refcursor as $$
declare
    ref refcursor;
begin
    open ref for select * from all_types;
    return ref;
end;
$$ LANGUAGE plpgsql;
