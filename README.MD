# Cargo-library
Супер пупер 

## Getting Started
Установить requirements.txt
запустить sandbox/fdw.py указав правильный путь к файлу config.ini
пример конфига можно забрать с 172.20.13.19 по пути /home/cargo/configs
В конфиге указать пути к сертификатам. Их можно забрать с 172.20.13.19 по пути /home/cargo/configs/certificates

Сообщения пишутся в postgres.public.fdw_kafka

## TODO
- Добавить в базу поле с offset сообщения из кафки и сделать его уникальным. Начинать сбор с последнего offset в базе
- Убрать id и заменить offset
- Сделать в контейнере
