# Copyright (c) 2020 by Terry Greeniaus.
import json
import uuid
import os
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

from .manager import Manager


class Server(ThreadingHTTPServer):
    def __init__(self, max_running, bind_addr):
        self.bind_addr            = bind_addr
        os.environ['JMAN_SERVER'] = 'http://' + bind_addr
        host, port                = bind_addr.split(':')
        super(Server, self).__init__((host, int(port)), JManHTTPRequestHandler)

        self.job_manager = Manager(max_running=max_running)

    def serve_forever(self, *args,
                      **kwargs):  # pylint: disable=signature-differs
        print('Starting Server on %s, max workers = %u' %
              (self.bind_addr, self.job_manager.max_running))

        super(Server, self).serve_forever(*args, **kwargs)

    def get_jobs(self):
        jobs = {}
        with self.job_manager.jobs_lock:
            for j in self.job_manager.jobs.values():
                jobs[j.uuid.hex] = {'name'   : j.name,
                                    'status' : j.get_status_str(),
                                    }
        return jobs

    @staticmethod
    def _job_to_json(j):
        if j.status == j.STATUS_COMPLETE:
            rc = j.proc.returncode
        else:
            rc = None
        rsp = {'uuid'      : j.uuid.hex,
               'status'    : j.get_status_str(),
               'meta'      : j.meta,
               'istate'    : j.istate,
               'error_log' : j.error_log,
               'exit_code' : rc
               }

        return rsp

    def get_job_by_name(self, name):
        try:
            j = self.job_manager.get_job_by_name(name)
        except KeyError:
            return None

        return self._job_to_json(j)

    def get_job_by_uuid(self, hex_str):
        try:
            j = self.job_manager[uuid.UUID(hex_str)]
        except KeyError:
            return None

        return self._job_to_json(j)

    def set_job_istate_by_name(self, name, istate):
        try:
            j = self.job_manager.get_job_by_name(name)
        except KeyError:
            return None

        j.set_istate(istate)
        return self._job_to_json(j)

    def set_job_istate_by_uuid(self, hex_str, istate):
        try:
            j = self.job_manager[uuid.UUID(hex_str)]
        except KeyError:
            return None

        j.set_istate(istate)
        return self._job_to_json(j)

    def _join(self, j, timeout=60):
        self.job_manager.join(j, timeout=timeout)

        return self._job_to_json(j)

    def join_by_name(self, name):
        try:
            j = self.job_manager.get_job_by_name(name)
        except KeyError:
            return None

        return self._join(j)

    def join_by_uuid(self, hex_str):
        try:
            j = self.job_manager[uuid.UUID(hex_str)]
        except KeyError:
            return None

        return self._join(j)

    def spawn_mod_func(self, cmd):
        module   = cmd['module']
        function = cmd['function']
        name     = cmd['name']
        args     = tuple(cmd['args'])
        kwargs   = cmd['kwargs']
        cwd      = cmd.get('cwd')
        j        = self.job_manager.spawn_mod_func(module, function, name,
                                                   args=args, kwargs=kwargs,
                                                   cwd=cwd)
        return self._job_to_json(j)

    def spawn_cmd(self, cmd):
        cli    = cmd['cmd']
        name   = cmd['name']
        args   = tuple(cmd['args'])
        kwargs = tuple(cmd['kwargs'])
        cwd    = cmd.get('cwd')
        j      = self.job_manager.spawn_cmd(cli, name, args=args, kwargs=kwargs,
                                            cwd=cwd)
        return self._job_to_json(j)


class JManHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/jobs':
            self._do_GET_jobs()
        elif self.path.startswith('/job_by_name/'):
            self._do_GET_job_by_name()
        elif self.path.startswith('/job_by_uuid/'):
            self._do_GET_job_by_uuid()
        elif self.path.startswith('/join_by_name/'):
            self._do_GET_join_by_name()
        elif self.path.startswith('/join_by_uuid/'):
            self._do_GET_join_by_uuid()
        else:
            self.send_error(404)

    def do_PUT(self):
        if self.path == '/spawn_mod_func':
            self._do_PUT_spawn_mod_func()
        elif self.path == '/spawn_cmd':
            self._do_PUT_spawn_cmd()
        elif self.path.startswith('/set_istate_by_name/'):
            self._do_PUT_set_istate_by_name()
        elif self.path.startswith('/set_istate_by_uuid/'):
            self._do_PUT_set_istate_by_uuid()
        else:
            self.send_error(404)

    def _send_json(self, response_code, j):
        if j is None:
            self.send_error(404)
            return

        content = json.dumps(j)
        self.send_response(response_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(content))
        self.end_headers()
        self.wfile.write(content.encode())

    def _do_GET_jobs(self):
        self._send_json(200, self.server.get_jobs())

    def _do_GET_job_by_name(self):
        name = self.path[13:]
        self._send_json(200, self.server.get_job_by_name(name))

    def _do_GET_job_by_uuid(self):
        hex_str = self.path[13:]
        self._send_json(200, self.server.get_job_by_uuid(hex_str))

    def _do_GET_join_by_name(self):
        name = self.path[14:]
        self._send_json(200, self.server.join_by_name(name))

    def _do_GET_join_by_uuid(self):
        hex_str = self.path[14:]
        self._send_json(200, self.server.join_by_uuid(hex_str))

    def _do_PUT_spawn_mod_func(self):
        length = int(self.headers['Content-Length'])
        cmd    = json.loads(self.rfile.read(length))
        self._send_json(200, self.server.spawn_mod_func(cmd))

    def _do_PUT_spawn_cmd(self):
        length = int(self.headers['Content-Length'])
        cmd    = json.loads(self.rfile.read(length))
        self._send_json(200, self.server.spawn_cmd(cmd))

    def _do_PUT_set_istate_by_name(self):
        name   = self.path[20:]
        length = int(self.headers['Content-Length'])
        istate = json.loads(self.rfile.read(length))
        self._send_json(200, self.server.set_job_istate_by_name(name, istate))

    def _do_PUT_set_istate_by_uuid(self):
        hex_str = self.path[20:]
        length  = int(self.headers['Content-Length'])
        istate  = json.loads(self.rfile.read(length))
        self._send_json(200, self.server.set_job_istate_by_name(hex_str,
                                                                istate))
