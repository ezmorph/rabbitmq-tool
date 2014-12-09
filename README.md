rabbitmq-tool
=============

Полный экспорт:
   
    rabbitmq-tool.py -h example.com -u user -p password > export_file

Экспорт всех очередей из виртуального хоста *example_vhost*:
  
    rabbitmq-tool.py -h example.com -v example_vhost -u user -p password > export_file

Экспорт всех сообщений из очереди *example_queue* виртуального хоста *example_vhost*:
  
    rabbitmq-tool.py -h example.com -q example_queue -v example_vhost -u user -p password > export_file

Экспорт всех очередей *example_queue* из всех виртуальных хостов:
  
    rabbitmq-tool.py -h example.com -q example_queue -u user -p password > export_file

Полный импорт:
  
    rabbitmq-tool.py -h example.com -u user -p password < import_file

Импорт всех сообщений виртуального хоста *example_vhost*:
  
    rabbitmq-tool.py -h example.com -v example_vhost -u user -p password < import_file

Импорт всех сообщений из очереди *example_queue* виртуального хоста *example_vhost*:
  
    rabbitmq-tool.py -h example.com -q example_queue -v example_vhost -u user -p password < import_file

Импорт всех очередей *example_queue* всех виртуальных хостов:
  
    rabbitmq-tool.py -h example.com -q example_queue -u user -p password < import_file

Копирование очереди *example_queue* в очередь *another_example_queue* (обе очереди должны находиться в одном виртуальном хосте):
  
    rabbitmq-tool.py -h example.com -q example_queue -r another_example_queue -v example_vhost -u user -p password < import_file

Пропустить первые 300 сообщений и далее импортировать по 10 сообщений если в очереди меньше 1000 сообщений:
  
    rabbitmq-tool.py -h example.com -q example_queue -r another_example_queue \
      -v example_vhost -u user -p password -m -t 1000 -b 10 -o 300 < import_file
