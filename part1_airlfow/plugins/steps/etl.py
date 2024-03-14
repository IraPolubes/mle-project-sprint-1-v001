from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Table, MetaData, Column, Integer, Float, String, UniqueConstraint, BigInteger
import pandas as pd

def extract(**kwargs):
    """Объединяем таблицы домов и их квартир."""
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    sql = f"""
    SELECT
    f.id AS flat_id,
    b.id AS building_id,
    f.floor,
    f.is_apartment,
    f.kitchen_area,
    f.living_area,
    f.rooms,
    f.studio,
    f.total_area,
    b.build_year,
    b.building_type_int,
    b.latitude,
    b.longitude,
    b.ceiling_height,
    b.flats_count,
    b.floors_total,
    b.has_elevator,
    f.price AS target
    FROM flats AS f
    LEFT JOIN buildings AS b ON f.building_id = b.id;
    """

    data = pd.read_sql(sql, conn)
    conn.close()
    ti = kwargs['ti']
    ti.xcom_push(key='extracted_data', value=data)


def transform(**kwargs):
    """Преобразуем булевы значения в 0 и 1"""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    data['is_apartment'] = data['is_apartment'].astype(int)
    data['studio'] = data['studio'].astype(int)
    data['has_elevator'] = data['has_elevator'].astype(int)
    ti.xcom_push('transformed_data', data)

def load(**kwargs):
    """Записываем в общую таблицу собранных признаков."""
    table_name = kwargs.get('table_name')
    if table_name is None:
        raise ValueError('table_name должeн быть предоставлен.')
    hook = PostgresHook('destination_db')
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform',key = 'transformed_data')

    hook.run(f"TRUNCATE TABLE {table_name};")

    hook.insert_rows(
        table=table_name,
     #   replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['flat_id'],
      #  conflict_handling='update',
        rows=data.values.tolist()
    )

def create_table(**kwargs):
    """Создаем шаблон хранилища всеми колонками из объединенных таблиц что хотим сохранить."""
    table_name = kwargs.get('table_name')

    if table_name is None:
        raise ValueError('table_name должeн быть предоставлен.')

    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData()
    flat_price_predict = Table(table_name, metadata,
                        Column('id', Integer, primary_key=True, autoincrement=True),
                        Column('flat_id', String, unique=True),
                        Column('building_id', String),
                        Column('floor', Integer),
                        Column('is_apartment', Integer),
                        Column('kitchen_area', Float),
                        Column('living_area', Float),
                        Column('rooms', Integer),
                        Column('studio', Integer),
                        Column('total_area', Float),
                        Column('build_year', Integer),
                        Column('building_type_int', Integer),
                        Column('latitude', Float),
                        Column('longitude', Float),
                        Column('ceiling_height', Float),
                        Column('flats_count', Integer),
                        Column('floors_total', Integer),
                        Column('has_elevator', Integer),
                        Column('target', BigInteger),  #flat price
                        UniqueConstraint('flat_id', name=table_name + '_unique_constraint2_name')
                    )
    metadata.create_all(engine)



