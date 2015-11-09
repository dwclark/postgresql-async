create or replace function my_cursor_func() returns refcursor as $$
declare
    ref refcursor;
begin
    open ref for select * from all_types;
    return ref;
end;
$$ LANGUAGE plpgsql;


create or replace function my_items() returns refcursor as $$
declare
    ref refcursor;
begin
    open ref for select * from items;
    return ref;
end;
$$ LANGUAGE plpgsql;

-- so fetch 0 cursor_name looks like the way to get a row description
-- for a cursor. As long as the cursor is not dynamically defined,
-- then the type information can be slurped up once and then re-used for type information.
