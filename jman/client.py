# Copyright(c) 2020 by Terry Greeniaus.
import requests


class Client:
    def __init__(self, url, timeout=(500, 500)):
        if url.endswith('/'):
            url=url[:-1]

        self.base_url = url
        self.timeout  = timeout
        self.session  = requests.Session()

    def _get(self, path):
        r = self.session.get(self.base_url + path, timeout=self.timeout)
        if r.status_code == 200:
            return r

        raise Exception('Unexpected response: %s' % r.content)

    def _put(self, path, j):
        r = self.session.put(self.base_url + path, json=j, timeout=self.timeout)
        if r.status_code == 409:
            raise Exception('Entity exists: %s %s' % (path, j))
        if r.status_code == 200:
            return r.json()
        if r.status_code == 204:
            return None

        raise Exception('Unexpected response: %s' % r.content)

    def get_jobs(self):
        return self._get('/jobs').json()

    def get_job_by_name(self, name):
        return self._get('/job_by_name/' + name).json()

    def get_job_by_uuid(self, u):
        return self._get('/job_by_uuid/' + u).json()

    def set_job_istate_by_name(self, name, istate):
        return self._put('/set_istate_by_name/' + name, istate)

    def set_job_istate_by_uuid(self, u, istate):
        return self._put('/set_istate_by_uuid/' + u, istate)

    def join_by_name(self, name):
        return self._get('/join_by_name/' + name).json()

    def join_by_uuid(self, u):
        return self._get('/join_by_uuid/' + u).json()

    def spawn_mod_func(self, module, function, name, args=(), kwargs=None,
                       cwd=None):
        cmd = {'module'   : module,
               'function' : function,
               'name'     : name,
               'args'     : args,
               'kwargs'   : kwargs or {},
               'cwd'      : cwd,
               }
        return self._put('/spawn_mod_func', cmd)

    def spawn_cmd(self, cmd, name, args=(), kwargs=None, cwd=None):
        cmd = {'cmd'    : cmd,
               'name'   : name,
               'args'   : args,
               'kwargs' : kwargs or {},
               'cwd'      : cwd,
               }
        return self._put('/spawn_cmd', cmd)
