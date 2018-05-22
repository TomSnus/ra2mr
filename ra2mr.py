
from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse
import raopt
'''
Control where the input data comes from, and where output data should go.
'''
class ExecEnv(Enum):
    LOCAL = 1   # read/write local files
    HDFS = 2    # read/write HDFS
    MOCK = 3    # read/write mock data to an in-memory file system.

'''
Switches between different execution environments and file systems.
'''
parts = []
def split_recursivee(ra):
    if ra is not None:
        parts.append(ra)
        if isinstance(ra, radb.ast.Select):
            split_recursivee(ra.cond)
        for item in ra.inputs:
            split_recursivee(item)

def remove_duplicates(values):
    output = []
    seen = set()
    for value in values:
        if value not in seen:
            output.append(value)
            seen.add(value)
    return output

class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)
    
    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)

'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''
def count_steps(raquery):
    assert(isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery,radb.ast.Project) or
        isinstance(raquery,radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)

    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''
    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''
def task_factory(raquery, step=1, env=ExecEnv.HDFS):
    assert(isinstance(raquery, radb.ast.Node))
    
    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
                          
    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")
    

class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Join))
      
        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1, env=self.exec_environment)

        return [task1, task2]

    
    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)
        
        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond
        ''' ...................... fill in your code below ........................'''
        #del parts[:]
        #split_recursivee(raquery)
        #parts_list = remove_duplicates(parts)
        #join = [x for x in parts_list if isinstance(x, radb.ast.Join)]
        #if len(join) == 2:
        if condition.inputs[0].name == condition.inputs[1].name:
            attribut = condition.inputs[0].name
            yield(json_tuple[relation+"."+str(condition.inputs[0].name)], (relation, json_tuple))
       # else:
        #    if join[0].cond.inputs[0].name == join[0].cond.inputs[1].name:
        #        attribut = join[0].cond.inputs[0].name
        #        yield(json_tuple[relation+"."+str(join[0].cond.inputs[0].name)], (relation, json_tuple))

        ''' ...................... fill in your code above ........................'''


    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        oldRelation = None
        solution = {}
        solution_list = []
        solution_list.append(solution)
        ''' ...................... fill in your code below ........................'''
        relation, dic = next(values)
        oldRelation = relation
        solution.update(dic)
        for val in values:
            relation, dic = val
            if relation == oldRelation:
                solution.update(dic)
            else:
                solution.update(dic)
                yield(key, solution_list)
        
        ''' ...................... fill in your code above ........................'''   


class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Select))
        
        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    
    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        condition = radb.parse.one_statement_from_string(self.querystring).cond
        ''' ...................... fill in your code below ........................'''
        if not isinstance(condition.inputs[0], radb.ast.AttrRef):
            tmp_ = condition.inputs[0]
            condition.inputs[0] = condition.inputs[1]
            condition.inputs[1] = tmp_
        if(isinstance(condition, radb.ast.ValExprBinaryOp)):
            for k, v in json_tuple.items():
                if isinstance(condition.inputs[0], radb.ast.AttrRef):
                    if isinstance(condition.inputs[1], radb.ast.Literal):
                        if str(condition.inputs[0].name) in str(k):
                            condi = str(condition.inputs[1].val).replace('\'', '')
                            condi = condi.replace('\'', '')
                            if str(condi) == str(v):
                                yield (relation, tuple)
        ''' ...................... fill in your code above ........................'''


class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]


    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        ''' ...................... fill in your code below ........................'''
        dic_ = dict()
        split_recursivee(raquery)
        parts_list = remove_duplicates(parts)
        rename = [x for x in parts_list if isinstance(x, radb.ast.Rename)]
        for k,v in json_tuple.items():
            dic_[str(k).replace(relation, rename[0].relname)] = v
        tuple_ = (json_tuple)
        yield(rename[0].relname, dic_)
        ''' ...................... fill in your code above ........................'''


class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]    


    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_dic = json.loads(tuple)
        attrs = radb.parse.one_statement_from_string(self.querystring).attrs
        ''' ...................... fill in your code below ........................'''
        dic_ = dict()
        for k,v in json_dic.items():
            for attr in attrs:
                if(isinstance(attr, radb.ast.AttrRef)):
                        if(str(attr.name) in str(k)) and str(v) not in dic_.values() and str(k) not in dic_.keys():
                            dic_[k] = v
        yield(relation, dic_)


        ''' ...................... fill in your code above ........................'''


    def reducer(self, key, values):

        ''' ...................... fill in your code below ........................'''
        dic = {}
        for item in values:
            if item != dic:
                dic = item
                yield(key, values)

        ''' ...................... fill in your code above ........................'''
        
        
if __name__ == '__main__':
    luigi.run()
