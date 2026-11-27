import io
from pathlib import Path
import socketserver
import sys
from threading import Thread
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lab_peer import exchange, line, read_session, receive


class Echo(socketserver.StreamRequestHandler):
    def handle(self):
        while (data := receive(self.rfile)) is not None:
            self.wfile.write(line({'received': data, 'port': self.client_address[1]}))


class LabPeerTests(unittest.TestCase):
    def test_session_through_loopback(self):
        events = read_session(Path(__file__).resolve().parents[1] / 'field-session.json')
        with socketserver.ThreadingTCPServer(('127.0.0.1', 0), Echo) as server:
            worker = Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01})
            worker.start()
            try:
                replies = list(exchange(events, server.server_address))
            finally:
                server.shutdown()
                worker.join()
        self.assertEqual([r['received'] for r in replies], [e for e in events if e['kind'] != 'disconnect'])
        switches = [row for row in events if row['kind'] == 'disconnect']
        self.assertTrue(switches)
        for event in switches:
            actor = event['actor']
            prior = [r['port'] for r in replies if r['received']['actor'] == actor and r['received']['at'] < event['at']]
            later = [r['port'] for r in replies if r['received']['actor'] == actor and r['received']['at'] > event['at']]
            self.assertTrue(prior and later, actor)
            self.assertNotEqual(prior[-1], later[0], actor)

    def test_unterminated_json_is_incomplete(self):
        with self.assertRaises(EOFError):
            receive(io.BytesIO(line({'message': '读数'})[:-1]))


if __name__ == '__main__':
    unittest.main()
