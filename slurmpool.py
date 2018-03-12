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
cd "{{ runpath }}"
{{ interpreter }} run.py
""")

RUN_TEMPLATE = """
import cloudpickle as marshal

try:
    with open("args.marshal") as argsfile:
        args = marshal.load(argsfile)

    with open("f.marshal") as ffile:
        f = marshal.load(ffile)

    res = f(*args)

    with open("res.marshal", "w") as resfile:
        marshal.dump(res, resfile)
except Exception as e:
    with open("error.marshal", "w") as errfile:
        marshal.dump(e, errfile)
    exit(-1)
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
    "NF": "error",
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

def try_cancel(job_id):
    try:
        sh.scancel(job_id)
    except:
        pass


class SlurmPool(object):
    def __init__(self, workdir, config=None):
        self.workdir = workdir
        if config is None:
            self.config = {}
        else:
            self.config = config
    def map(self, f, *iterables):
        jobs = []
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

        try:
            for args in itertools.izip(*iterables):
                jobdir = tempfile.mkdtemp(prefix=prefix)
                os.chdir(jobdir)
                slurm_script = SLURM_TEMPLATE.render(
                        slurmconfig=self.config,
                        runpath=jobdir,
                        pythonpath=":".join(pythonpath),
                        interpreter=interpreter)
                run_script = RUN_TEMPLATE.format()
                with open(os.path.join(jobdir, "args.marshal"), "w") as argsfile:
                    marshal.dump(args, argsfile)
                with open(os.path.join(jobdir, "f.marshal"), "w") as ffile:
                    marshal.dump(f, ffile)
                with open(os.path.join(jobdir, "run.py"), "w") as runfile:
                    runfile.write(run_script)

                job_id = sh.sbatch("--parsable", _in=slurm_script).stdout.strip()
                jobs.append((job_id, jobdir))

            job_ids, job_dirs = zip(*jobs)

            remaining_jobs = list(job_ids)
            error = None
            while error is None and len(remaining_jobs) > 0:
                states = get_job_states(remaining_jobs)
                simple_states = [STATE_MAP.get(st, "error") for st in states] 
                remaining_jobs2 = []
                for jid, st in zip(remaining_jobs, simple_states):
                    if st == "error":
                        error = jid
                        break
                    if st == "ok":
                        remaining_jobs2.append(jid)
                remaining_jobs = remaining_jobs2
                time.sleep(1)
            if error is not None:
                jobdir = dict(jobs)[error]
                errfn = os.path.join(jobdir, "error.marshal")
                if os.path.exists(errfn):
                    with open(errfn) as errfile:
                        err = marshal.load(errfile)
                    raise err
                raise RuntimeError("error in job {}".format(error))

            res = []
            for jobdir in job_dirs:
                with open(os.path.join(jobdir, "res.marshal")) as resfile:
                    res.append(marshal.load(resfile))
        finally:
            os.chdir(olddir)
            if len(jobs) > 0:
                job_ids, job_dirs = zip(*jobs)
                states = get_job_states(job_ids)
                simple_states = [STATE_MAP.get(st, "error") for st in states] 
                for jid, state in zip(job_ids, simple_states):
                    if state != "done":
                        try_cancel(jid)
                for jobdir in job_dirs:
                    shutil.rmtree(jobdir)
        return res

def f(a):
    raise ValueError("bad balue: {}".format(a))

def g(a):
    if a == 30:
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
    p = SlurmPool("/project/meteo/work/Tobias.Koelling/slurmpool", config)
    print p.map(g, range(40))

if __name__ == '__main__':
    _main()
