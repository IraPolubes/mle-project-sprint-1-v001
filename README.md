# Задача регрессии по предсказанию цены на квартиру
Пожалуйста обратите внимание что код был перенесен в папки part1_airlfow и part2_dvc как было описано в вашем Readme 
уже после того как отработал и создал все что нужно для задачи,
поэтому путь к файлам внутри кода не учитывает эти папки.
Чтобы запустить проект пожалуйста используйте оригинальную структуру проекта.


## DAG 1: cбор данных и отсылка сообщения в телеграм
в папке part1_airlfow:
dag: dags/etl_DAG.py
functions create_table, extract, transform, load: plugins/steps/etl.py

## DAG 2: очистка данных модели
в папке part1_airlfow:
dag: dags/clean_dataset_DAG.py
fucntions: extract, transform: plugins/steps/clean_etl.py
function create_table, load: plugins/steps/etl.py
Функции создания и записи в таблицы имеют одну и ту же структуру таблицы,
поэтому я использовала название таблицы и uniqe constraint в качестве параметра
И вызываю из с разными параметрами названий таблиц из обоих ДАГов

## DVC обучение модели
в папке part2_dvc во вложенных папках scripts

## DVC конфигурация
в папке part2_dvc: dvc.yaml, params.yaml, dvc.lock.

## Пара слов и вопрос в comments.txt
comments.txt
