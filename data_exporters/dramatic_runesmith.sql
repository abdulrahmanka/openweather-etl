INSERT INTO public.tbl_batches
( batch_name, batch_description, status, inserted_at, insert_user)
VALUES('weather_forcast-etl', '', 'ACTIVE'::character varying, CURRENT_TIMESTAMP, 'elt_user') RETURNING batch_id;