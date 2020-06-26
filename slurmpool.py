import os
import shutil
import sys
import itertools
import cloudpickle as marshal
import tempfile
import inspect
import time
import sh
import jinja2

SLURM_TEMPLATE = jinja2.Template("""#!/bin/bash
{% for key, value in slurmconfig.items() -%}
#SBATCH --{{ key }}{% if value: %}={{ value }}{% endif %}
{% endfor %}

export PYTHONPATH="{{ pythonpath }}"
cd "{{ jobdir }}"
{{ interpreter }} run.py "$SLURM_ARRAY_TASK_ID" "{{ rundir }}"
""")

RUN_TEMPLATE = """
import os
import sys
import cloudpickle as marshal
import traceback
import signal

class TerminateException(Exception):
    pass
def terminate_handler(signum, frame):
    raise TerminateException()
signal.signal(signal.SIGTERM, terminate_handler)


olddir = os.getcwd()
controldir = os.path.join(olddir, sys.argv[1])

try:
    with open("f.marshal") as ffile:
        f = marshal.load(ffile)

    os.chdir(controldir)
    with open("args.marshal") as argsfile:
        args = marshal.load(argsfile)

    os.chdir(sys.argv[2])
    if len(args) == 1:
        res = [f(*(args[0]))]
    else:
        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(len(args))
        res = pool.map(lambda x: f(*x), args)
    os.chdir(controldir)
    with open("res.marshal", "w") as resfile:
        marshal.dump(res, resfile)
except Exception as e:
    os.chdir(controldir)
    with open("error.marshal", "w") as errfile:
        marshal.dump({"exception": e, "traceback": traceback.format_exc()}, errfile)
    raise
"""

STATE_MAP = {
    "PD": "ok",
    "R": "ok",
    "CA": "error",
    "CF": "ok",
    "CG": "ok",
    "CD": "done",
    "F": "error",
    "TO": "error",
    "NF": "nodefail",
    "RV": "error",
    "SE": "error",
    "NA": "error",
}

def get_job_states(jobs):
    job_id_list = ",".join(jobs)
    state = {}
    for line in sh.squeue("-h", format="%A %t", jobs=job_id_list, states="all").stdout.split("\n"):
        parts = line.strip().split()
        if len(parts) != 2:
            continue
        jid, st = parts
        state[jid] = st
    return [state.get(jid, "NA") for jid in jobs]

def get_job_state(job_id):
    return get_job_states([job_id])[0]

def get_job_array_state(job_array_id):
    states = set()
    for line in sh.squeue("-h", format="%F %t", jobs=job_array_id, states="all").stdout.split("\n"):
        parts = line.strip().split()
        if len(parts) != 2:
            continue
        jid, st = parts
        if jid != job_array_id:
            continue
        states.add(st)
    return states

def get_simplified_job_array_state(job_array_id):
    return set(STATE_MAP.get(st, "error") for st in get_job_array_state(job_array_id))

def try_cancel(job_id):
    try:
        sh.scancel(job_id)
    except:
        pass

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in xrange(0, len(l), n))

class SlurmPool(object):
    def __init__(self, workdir=None, threads=1, config=None):
        if workdir is None:
            self.workdir = os.path.join(os.environ["SCRATCH"], "slurmpool")
        else:
            self.workdir = workdir
        if config is None:
            self.config = {}
        else:
            self.config = config
        self.config["cpus-per-task"] = threads
        self.threads = threads

    def map(self, f, *iterables):
        inputs = zip(*iterables)
        retries = 3
        res = self._map(f, inputs)
        while retries > 0:
            retries -= 1
            retry_tasks = [
                (i, inp)
                for i, ((c, r), inp) in enumerate(zip(res, inputs))
                if c == "error"]
            if len(retry_tasks) == 0:
                break
            idx, inputs2 = zip(*retry_tasks)
            res2 = self._map(f, inputs2)
            for i, r in zip(idx, res2):
                res[i] = r
        return [r if c == "ok" else None for c, r in res]

    def _map(self, f, inputs):
        sourcemodule = inspect.getmodule(f).__name__
        sourcefile = os.path.abspath(inspect.getfile(f))
        sourcefolder = os.path.dirname(sourcefile)
        if sourcemodule == "__main__":
            sourcemodule = os.path.splitext(os.path.split(sourcefile)[-1])[0]
        interpreter = sys.executable
        pythonpath = sys.path

        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)
        prefix = os.path.join(self.workdir, "tmp")
        olddir = os.getcwd()

        job_id = None
        jobdir = None

        inputs = list(chunks(inputs, self.threads))

        try:
            jobdir = tempfile.mkdtemp(prefix=prefix)
            os.chdir(jobdir)
            run_script = RUN_TEMPLATE
            with open(os.path.join(jobdir, "f.marshal"), "w") as ffile:
                marshal.dump(f, ffile)
            with open(os.path.join(jobdir, "run.py"), "w") as runfile:
                runfile.write(run_script)

            for i, args in enumerate(inputs, 1):
                subdir = os.path.join(jobdir, str(i))
                os.makedirs(subdir)
                with open(os.path.join(subdir, "args.marshal"), "w") as argsfile:
                    marshal.dump(args, argsfile)

            jobcount = len(inputs)

            slurmconfig = self.config.copy()
            slurmconfig["array"] = "1-{}".format(jobcount)
            slurm_script = SLURM_TEMPLATE.render(
                    slurmconfig=slurmconfig,
                    rundir=olddir,
                    jobdir=jobdir,
                    pythonpath=":".join(pythonpath),
                    interpreter=interpreter)
            job_id = sh.sbatch("--parsable", _in=slurm_script).stdout.strip()

            state = get_simplified_job_array_state(job_id)
            while not "error" in state and "ok" in state:
                state = get_simplified_job_array_state(job_id)
                time.sleep(1)

            res = []
            for i in range(1, jobcount+1):
                subdir = os.path.join(jobdir, str(i))
                resfn = os.path.join(subdir, "res.marshal")
                errfn = os.path.join(subdir, "error.marshal")
                if os.path.exists(errfn):
                    print "ERROR in JOB {}!".format(i)
                    with open(errfn) as errfile:
                        error_info = marshal.load(errfile)
                    print error_info["traceback"]
                    print "="*30
                    raise error_info["exception"]
                if os.path.exists(resfn):
                    with open(resfn) as resfile:
                        res.append(("ok", marshal.load(resfile)))
                else:
                    res.append(("error", [None]*self.threads))
        finally:
            os.chdir(olddir)
            if job_id is not None:
                try_cancel(job_id)
            if jobdir is not None:
                shutil.rmtree(jobdir)
        return [(st, r) for st, rs in res for r in rs]

def f(a):
    raise ValueError("bad balue: {}".format(a))

def g(a):
    if a == 300:
        raise ValueError("value: {}".format(30))
    return 2*a

def _main():
    config = { 
        "partition": "ws",
        "mem": "1G",
        "time": "01:00:00",
        "ntasks": "1",
        "requeue": None,
    }
    p = SlurmPool(config=config)
    print p.map(g, range(40))

if __name__ == '__main__':
    _main()
