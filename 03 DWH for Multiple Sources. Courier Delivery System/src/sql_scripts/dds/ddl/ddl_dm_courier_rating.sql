-- справочник ставок курьеров в зависимости от рейтинга
drop table if exists dds.dm_courier_rating;

create table if not exists dds.dm_courier_rating (
    id serial primary key,                    -- суррогатный ключ
    min_rating numeric(3,2) not null,         -- минимальная граница рейтинга
    max_rating numeric(3,2) not null,         -- максимальная граница рейтинга
    percent_rate integer not null,            -- процент от заказа
    min_payment integer not null               -- минимальная выплата
);

-- заполнение справочника
insert into dds.dm_courier_rating (min_rating, max_rating, percent_rate, min_payment) values
    (0, 3.99, 5, 100),
    (4, 4.49, 7, 150),
    (4.5, 4.89, 8, 175),
    (4.9, 5, 10, 200);