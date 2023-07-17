# Sensor Data Parser

## parse_byte_data_sensor.py

В файле `parse_byte_data_sensor.py` содержится решение с преобразованием входных данных из HEX строки в байтовую строку. Это делается с помощью методов `hex_string_to_bytes`, `preparation_hex_string`, которые позволяют правильно интерпретировать строку данных и создать байтовый массив.

## parse_hex_data_sensor.py

В файле `parse_hex_data_sensor.py` содержится решение без преобразования исходной строки данных. Вместо этого, данные обрабатываются напрямую как строка HEX.

## docker-compose.yml

Файл docker-compose.yml используется для создания контейнера с базой данных PostgreSQL.