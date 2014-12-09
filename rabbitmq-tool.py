#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
usage_message = u"""
  ./rabbitmq-tool.py [options] < import_file
  ./rabbitmq-tool.py [options] > export_file"""

import os
import stat
import time
import pika
import pickle
import subprocess
import sys
import os
import json
from optparse import OptionParser

def main():
    parser = OptionParser(usage=usage_message,
                          version="%prog 1.0", conflict_handler="resolve")

    parser.add_option("-h", "--host", help=u"Хост", default="localhost")
    parser.add_option("-P", "--port", help=u"Порт", default=5672)
    parser.add_option("-u", "--user", help=u"Логин", default="guest")
    parser.add_option("-p", "--password", help=u"Пароль", default="guest")
    parser.add_option("-v", "--vhost", help=u"Имя виртуального хоста")
    parser.add_option("-q", "--queue", help=u"Имя очереди")
    parser.add_option(
        "-c",
        "--count",
        help=u"Количество выгружаемых сообщений",
        default=-1)
    parser.add_option(
        "-t",
        "--threshold",
        help=u"Максимальное количество сообщений в очереди, при котором будет осуществляется отправка сообщений",
        default=0)
    parser.add_option("-b", "--batch", help=u"Размер пачки", default=0)
    parser.add_option("-o", "--offset", help=u"Пропуск n сообщений", default=0)
    parser.add_option(
        "-r",
        "--qreplace",
        help=u"Переопределить очередь при импорте",
        default=None)
    parser.add_option(
        "--ack",
        help=u"Подтверждение сообщений (сообщения не будут возвращены в очередь при экспорте)",
        action="store_true")

    (options, args) = parser.parse_args()

    options.port = int(options.port)
    options.count = int(options.count)
    options.threshold = int(options.threshold)
    options.batch = int(options.batch)
    options.offset = int(options.offset)

    mode = os.fstat(0).st_mode

    if stat.S_ISFIFO(mode) or stat.S_ISREG(mode):
        restore(
            options.host,
            options.port,
            options.user,
            options.password,
            options.vhost,
            options.queue,
            options.threshold,
            options.batch,
            options.offset,
            options.qreplace)
    else:
        backup(
            options.host,
            options.port,
            options.user,
            options.password,
            options.vhost,
            options.queue,
            options.ack,
            options.count)

def amqp_connect(host, port, user, password, vhost):
    credentials = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=host, port=port, credentials=credentials, virtual_host=vhost))
    channel = connection.channel()
    return connection, channel

def backup(host, port, user, password, vhost_name, queue_name, ack, count):
    processed = 0
    queues = load_queues(host, user, password, vhost_name, queue_name)
    for item in queues:
        queue = item[0]
        vhost = item[1]
        connection, channel = amqp_connect(host, port, user, password, vhost)
        break_counter = count
        while True:
            msg = []
            method_frame, header_frame, body = channel.basic_get(queue=queue)
            if method_frame.NAME == 'Basic.GetEmpty':
                connection.close()
                break

            props = {}
            for k, v in header_frame.__dict__.items():
                if v != None:
                    props[k] = v
            msg.append(body)
            msg.append(header_frame.headers)
            msg.append(props)
            msg.append(item[1])
            msg.append(item[0])
            msg.append(method_frame.routing_key)
            pickle.dump(msg, sys.stdout)
            processed += 1
            if ack:
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            if not break_counter < 0:
                break_counter = break_counter - 1
                if break_counter == 0:
                    break
    sys.stderr.write('Выгружено сообщений: %d\n' % processed)

def restore(
        host,
        port,
        user,
        password,
        vhost_name,
        queue_name,
        threshold,
        batch,
        offset,
        qreplace):
    status = None
    batch_count = 0
    processed = 0
    skipped = offset
    while offset > 0:
        pickle.load(sys.stdin)
        offset -= 1
    if skipped > 0:
        print "%d messages skipped..." % skipped
    try:
        input = pickle.load(sys.stdin)
    except EOFError:
        sys.exit()
    vhost = new_vhost = input[3]
    connection, channel = amqp_connect(host, port, user, password, vhost)
    while True:
        if vhost != new_vhost:
            vhost = new_vhost
            connection.close()
            connection, channel = amqp_connect(
                host, port, user, password, vhost)
        properties = input[2]
        headers = input[1]
        body = input[0]

        if qreplace is None:
            routing_key = input[4]
        else:
            routing_key = qreplace

        routing_key = str(routing_key)

        if threshold > 0 and batch_count == 0:
            status = channel.queue_declare(queue=routing_key, passive=True)
            while status.method.message_count >= threshold:
                print u"[SRC: %s -> %s, DST: %s -> %s][Messages in queue: %d, Processed: %d] Сообщений в очереди >= %d, импорт отложен..." % (vhost, input[4], vhost, routing_key, status.method.message_count, processed, threshold)
                status = channel.queue_declare(queue=routing_key, passive=True)
                time.sleep(1)

        properties = pika.BasicProperties(
            headers=headers,
            priority=properties['priority'] if 'priority' in properties.keys() else None,
            content_type=properties['content_type'] if 'content_type' in properties.keys() else None,
            content_encoding=properties['content_encoding'] if 'content_encoding' in properties.keys() else None,
            reply_to=properties['reply_to'] if 'reply_to' in properties.keys() else None,
            expiration=properties['expiration'] if 'expiration' in properties.keys() else None,
            message_id=properties['message_id'] if 'message_id' in properties.keys() else None,
            timestamp=properties['timestamp'] if 'timestamp' in properties.keys() else None,
            type=properties['type'] if 'type' in properties.keys() else None,
            user_id=properties['user_id'] if 'user_id' in properties.keys() else None,
            app_id=properties['app_id'] if 'app_id' in properties.keys() else None,
            cluster_id=properties['cluster_id'] if 'cluster_id' in properties.keys() else None,
            delivery_mode=properties['delivery_mode']if 'delivery_mode' in properties.keys() else None)

        status = channel.queue_declare(queue=routing_key, passive=True)

        if (((not vhost_name and not queue_name or
              vhost_name == vhost and not queue_name or
              not vhost_name and queue_name == routing_key or
              vhost_name == vhost and queue_name == routing_key) and qreplace is None) or
                ((queue_name == input[4]) or not queue_name) and qreplace is not None):
            print u"[SRC: %s -> %s, DST: %s -> %s][Messages in queue: %d, Processed: %d] Отправка сообщения..." % (vhost, input[4], vhost, routing_key, status.method.message_count, processed)

            channel.basic_publish(exchange='',
                                  body=body,
                                  routing_key=routing_key,
                                  properties=properties)

            status = channel.queue_declare(queue=routing_key, passive=True)
            print u"[SRC: %s -> %s, DST: %s -> %s][Messages in queue: %d, Processed: %d] Сообщение доставлено!" % (vhost, input[4], vhost, routing_key, status.method.message_count, processed + 1)
            processed = processed + 1
            if batch_count > 0:
                batch_count = batch_count - 1
        try:
            input = pickle.load(sys.stdin)
            new_vhost = input[3]
        except EOFError:
            connection.close()
            sys.exit()

def load_queues(host, user, password, vhost_name, queue_name):
    queues = []
    command = "curl -i -s -u %s:%s http://%s:15672/api/queues/" % (
        user, password, host)
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    output = proc.stdout.read().split('\n')
    try:
        output = json.loads(output[7])
    except:
        sys.stderr.write("Error! Can't get information about queues!\n")
        sys.exit()
    for item in output:
        if (not vhost_name and not queue_name or
                vhost_name == item['vhost'] and not queue_name or
                not vhost_name and queue_name == item['name'] or
                vhost_name == item['vhost'] and queue_name == item['name']):
            queue = []
            queue.append(item['name'])
            queue.append(item['vhost'])
            queues.append(queue)
    return queues

if __name__ == '__main__':
    main()
