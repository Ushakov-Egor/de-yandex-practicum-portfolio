import uuid
from datetime import datetime

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    # -----------------------------------------------------------------------
    # HUBS
    # -----------------------------------------------------------------------

    def h_user_insert(self, user_id: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_user_pk = uuid.uuid5(uuid.NAMESPACE_X500, user_id)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_user(h_user_pk, user_id, load_dt, load_src)
                    VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {'h_user_pk': h_user_pk, 'user_id': user_id,
                     'load_dt': load_dt, 'load_src': load_src}
                )
        return h_user_pk

    def h_product_insert(self, product_id: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_product_pk = uuid.uuid5(uuid.NAMESPACE_X500, product_id)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_product(h_product_pk, product_id, load_dt, load_src)
                    VALUES (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {'h_product_pk': h_product_pk, 'product_id': product_id,
                     'load_dt': load_dt, 'load_src': load_src}
                )
        return h_product_pk

    def h_category_insert(self, category_name: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_category_pk = uuid.uuid5(uuid.NAMESPACE_X500, category_name)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_category(h_category_pk, category_name, load_dt, load_src)
                    VALUES (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {'h_category_pk': h_category_pk, 'category_name': category_name,
                     'load_dt': load_dt, 'load_src': load_src}
                )
        return h_category_pk

    def h_restaurant_insert(self, restaurant_id: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_restaurant_pk = uuid.uuid5(uuid.NAMESPACE_X500, restaurant_id)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant(h_restaurant_pk, restaurant_id, load_dt, load_src)
                    VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {'h_restaurant_pk': h_restaurant_pk, 'restaurant_id': restaurant_id,
                     'load_dt': load_dt, 'load_src': load_src}
                )
        return h_restaurant_pk

    def h_order_insert(self, order_id: int, order_dt: datetime, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_order_pk = uuid.uuid5(uuid.NAMESPACE_X500, str(order_id))
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_order(h_order_pk, order_id, order_dt, load_dt, load_src)
                    VALUES (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {'h_order_pk': h_order_pk, 'order_id': order_id, 'order_dt': order_dt,
                     'load_dt': load_dt, 'load_src': load_src}
                )
        return h_order_pk

    # -----------------------------------------------------------------------
    # LINKS
    # -----------------------------------------------------------------------

    def l_order_product_insert(self, h_order_pk: uuid.UUID, h_product_pk: uuid.UUID,
                                load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_order_pk) + str(h_product_pk))
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_product(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                    VALUES (%(hk)s, %(h_order_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {'hk': hk, 'h_order_pk': h_order_pk, 'h_product_pk': h_product_pk,
                     'load_dt': load_dt, 'load_src': load_src}
                )

    def l_product_restaurant_insert(self, h_product_pk: uuid.UUID, h_restaurant_pk: uuid.UUID,
                                     load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_product_pk) + str(h_restaurant_pk))
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_restaurant(hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                    VALUES (%(hk)s, %(h_product_pk)s, %(h_restaurant_pk)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {'hk': hk, 'h_product_pk': h_product_pk, 'h_restaurant_pk': h_restaurant_pk,
                     'load_dt': load_dt, 'load_src': load_src}
                )

    def l_product_category_insert(self, h_product_pk: uuid.UUID, h_category_pk: uuid.UUID,
                                   load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_product_pk) + str(h_category_pk))
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_category(hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                    VALUES (%(hk)s, %(h_product_pk)s, %(h_category_pk)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {'hk': hk, 'h_product_pk': h_product_pk, 'h_category_pk': h_category_pk,
                     'load_dt': load_dt, 'load_src': load_src}
                )

    def l_order_user_insert(self, h_order_pk: uuid.UUID, h_user_pk: uuid.UUID,
                             load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_order_pk) + str(h_user_pk))
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_user(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                    VALUES (%(hk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {'hk': hk, 'h_order_pk': h_order_pk, 'h_user_pk': h_user_pk,
                     'load_dt': load_dt, 'load_src': load_src}
                )

    # -----------------------------------------------------------------------
    # SATELLITES
    # -----------------------------------------------------------------------

    def s_user_names_insert(self, h_user_pk: uuid.UUID, username: str, userlogin: str,
                             load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_user_pk) + username + userlogin)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_user_names(hk_user_names_hashdiff, h_user_pk, username, userlogin, load_dt, load_src)
                    VALUES (%(hk)s, %(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;
                    """,
                    {'hk': hk, 'h_user_pk': h_user_pk, 'username': username,
                     'userlogin': userlogin, 'load_dt': load_dt, 'load_src': load_src}
                )

    def s_product_names_insert(self, h_product_pk: uuid.UUID, name: str,
                                load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_product_pk) + name)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_product_names(hk_product_names_hashdiff, h_product_pk, name, load_dt, load_src)
                    VALUES (%(hk)s, %(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;
                    """,
                    {'hk': hk, 'h_product_pk': h_product_pk, 'name': name,
                     'load_dt': load_dt, 'load_src': load_src}
                )

    def s_restaurant_names_insert(self, h_restaurant_pk: uuid.UUID, name: str,
                                   load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_restaurant_pk) + name)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_restaurant_names(hk_restaurant_names_hashdiff, h_restaurant_pk, name, load_dt, load_src)
                    VALUES (%(hk)s, %(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;
                    """,
                    {'hk': hk, 'h_restaurant_pk': h_restaurant_pk, 'name': name,
                     'load_dt': load_dt, 'load_src': load_src}
                )

    def s_order_cost_insert(self, h_order_pk: uuid.UUID, cost: float, payment: float,
                             load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_order_pk) + str(cost) + str(payment))
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_cost(hk_order_cost_hashdiff, h_order_pk, cost, payment, load_dt, load_src)
                    VALUES (%(hk)s, %(h_order_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING;
                    """,
                    {'hk': hk, 'h_order_pk': h_order_pk, 'cost': cost,
                     'payment': payment, 'load_dt': load_dt, 'load_src': load_src}
                )

    def s_order_status_insert(self, h_order_pk: uuid.UUID, status: str,
                               load_dt: datetime, load_src: str) -> None:
        hk = uuid.uuid5(uuid.NAMESPACE_X500, str(h_order_pk) + status)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_status(hk_order_status_hashdiff, h_order_pk, status, load_dt, load_src)
                    VALUES (%(hk)s, %(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;
                    """,
                    {'hk': hk, 'h_order_pk': h_order_pk, 'status': status,
                     'load_dt': load_dt, 'load_src': load_src}
                )
