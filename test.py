import luigi
import radb
import ra2mr
raquery = radb.parse.one_statement_from_string("\project_{name} Person;")

task = ra2mr.task_factory(raquery, env=ra2mr.ExecEnv.HDFS)

luigi.build([task], local_scheduler=True)
