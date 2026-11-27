"""经本机 TCP 发送不同设备的现场会话。"""

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import socket


def read_session(path):
    scene = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(scene['inventory'], dict) or not scene['events']:
        raise ValueError('需要设备登记及至少一条现场消息')
    messages = [dict(at=0, actor='registry', kind='inventory', data=scene['inventory'])]
    for at, actor, kind, body in scene['events']:
        if (type(at) is not int or at < messages[-1]['at'] or not isinstance(actor, str) or not actor
                or not isinstance(kind, str) or not kind or not isinstance(body, dict)):
            raise ValueError('无效的接收时点或消息外壳')
        messages.append(dict(at=at, actor=actor, kind=kind, data=body))
    return messages


def line(value):
    return (json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()


def receive(stream):
    raw = stream.readline(1024 * 1024 + 1)
    if not raw:
        return None
    if len(raw) > 1024 * 1024 or not raw.endswith(b'\n'):
        raise EOFError('消息缺少尾部换行或超过接收上限')
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError('消息必须为 JSON 对象')
    return data


def exchange(messages, address, timeout=3):
    active = {}
    with ExitStack() as opened:
        for message in messages:
            actor = message['actor']
            if message['kind'] == 'disconnect':
                if actor in active:
                    resources, _ = active.pop(actor)
                    resources.close()
                continue
            if actor not in active:
                resources = opened.enter_context(ExitStack())
                client = resources.enter_context(socket.create_connection(address, timeout))
                stream = resources.enter_context(client.makefile('rwb'))
                active[actor] = resources, stream
            _, stream = active[actor]
            stream.write(line(message))
            stream.flush()
            reply = receive(stream)
            if reply is None:
                raise EOFError(f'{actor} 连接提前结束')
            yield reply


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('session', nargs='?', type=Path, default=Path(__file__).with_name('field-session.json'))
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', default=8765, type=int)
    options = parser.parse_args()
    messages = read_session(options.session)
    if options.check:
        print(json.dumps({'消息': len(messages), '来源': sorted({m['actor'] for m in messages})}, ensure_ascii=False))
        return
    for response in exchange(messages, (options.host, options.port)):
        print(json.dumps(response, ensure_ascii=False))


if __name__ == '__main__':
    main()
