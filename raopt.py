import sqlparse
import radb.ast
from radb.ast import *

parts = []


def rule_break_up_selections(ra):
    del parts[:]
    split_recursivee(ra)
    parts.append(ra)
    parts_list = remove_duplicates(parts)
    select = None
    conditions = []
    sub_select = None
    from_part = None
    from_part_rename = None
    valExpr = [x for x in parts_list if isinstance(x, ValExprBinaryOp)]
    select = [x for x in parts_list if isinstance(x, Select)]
    project = [x for x in parts_list if isinstance(x, Project)]
    if len(valExpr) < 2:
        return ra
    rel = extract_cross([x for x in parts_list if isinstance(x, RelExpr)], valExpr)
    val_rel = select[0].inputs[0]
    if isinstance(rel, Cross):
        if len(project) > 0:
            project[0].inputs[0] = rel
            return project[0]
        return rel
    select = Select(valExpr[1], rel)
    if len(project) > 0:
        project[0].inputs[0] = select
        return project[0]
    return select

def get_selections(cond):
    conditions = []
    if cond.op == 11:  # 11 <=> AND
        conditions += get_selections(cond.inputs[0])
        conditions += get_selections(cond.inputs[1])
        return conditions
    else:
        return [cond]

def rule_push_down_selections(ra, dd):
    del parts[:]
    parts.append(ra)
    split_recursivee(ra)
    parts_list = remove_duplicates(parts)
    valExpr = [x for x in parts_list if isinstance(x, ValExprBinaryOp)]
    project = [x for x in parts_list if isinstance(x, Project)]
    relations = [x for x in parts_list if isinstance(x, RelRef) or isinstance(x, Rename)]
    join_cond = []
    if len(relations) < 2:
        return ra
    for item in valExpr[:]:
        if all(isinstance(x, AttrRef) for x in item.inputs):
            join_cond.append(item)
            valExpr.remove(item)
    #No selections to push down
    if len(valExpr) == 0 and len(join_cond) < 2:
        return ra
    rel = None
    for key, value in dd.items():
        for k, v, in value.items():
            for item in valExpr[:]:
                # Rename check
                if any(k in str(x) for x in item.inputs) and (any(key == str(y) for y in relations)):
                    if any(isinstance(x, Rename) for x in relations):
                        for rename in relations:
                            if isinstance(rename, Rename) and rename.inputs[0].rel == str(key):
                                rel = rename
                                break
                    else:
                        rel = RelRef(key)
                    select = Select(item, rel)
                    valExpr.remove(item)
                    # remove Relation from Relationlist
                    for i, o in enumerate(relations):
                        if (isinstance(o, RelRef) and str(o.rel) == key) or isinstance(o, Rename) and o.inputs[
                            0].rel == key:
                            if isinstance(relations[i], Rename) and isinstance(select.inputs[0], Rename) and relations[
                                i].relname != select.inputs[0].relname:
                                continue
                            else:
                                relations[i] = select

    relations = remove_duplicates(relations)
    joined_relations = create_cross(dd, join_cond, relations)
    if len(join_cond) > 0:
        select = create_select(join_cond, joined_relations)
        if len(project) > 0:
            project[0].inputs[0] = select
            return project[0]
        return select
    else:
        return joined_relations

def rule_merge_selections(ra):
    del parts[:]
    parts.append(ra)
    split_recursivee(ra)
    parts_list = remove_duplicates(parts)
    select = [x for x in parts_list if isinstance(x, Select)]
    project = [x for x in parts_list if isinstance(x, Project)]
    relations = [x for x in parts_list if isinstance(x, RelRef) or isinstance(x, Rename)]
    valExpr = [x for x in parts_list if isinstance(x, ValExprBinaryOp)]
    join_cond = []
    if len(valExpr) < 2:
        return ra
    #check if pushed cross conditions (those can't be merged)
    for item in valExpr[:]:
        if all(isinstance(x, AttrRef) for x in item.inputs):
            join_cond.append(item)
    if (check_for_pushed_cross_conditions(valExpr, join_cond, select)):
        return ra
    table = select[1].inputs[0]
    if isinstance(table, Cross):
        for relation in table.inputs[:]:
            if isinstance(relation, Select):
                relations.remove(relation.inputs[0])
            else:
                relations.remove(relation)
    else:
        relations.remove(table)
    cond = valExpr[0]
    if len(valExpr) == 3:
        for i in range(1, len(valExpr)-1):
            cond = ValExprBinaryOp(cond, sym.AND ,valExpr[i])
    elif len(valExpr) == 4:
        for i in range(1, len(valExpr)-2):
            cond = ValExprBinaryOp(cond, sym.AND ,valExpr[i])
            relations[0]=select[3]
    else:
        for i in range(1, len(valExpr)):
            cond = ValExprBinaryOp(cond, sym.AND ,valExpr[i])
    select = Select(cond, table)
    relations.append(select)
    joined_relations = create_connection(relations)
    if len(project) > 0:
        project[0].inputs[0] = joined_relations
        return project[0]
    return joined_relations

def check_for_pushed_cross_conditions(valExpr, join_cond, select):
    if valExpr == join_cond: #only cross conditions
        for item in select:
            if all(isinstance(x, Cross) for x in item.inputs):
                return True
            return False

def rule_introduce_joins(ra):
    del parts[:]
    parts.append(ra)
    split_recursivee(ra)
    parts_list = remove_duplicates(parts)
    select = [x for x in parts_list if isinstance(x, Select)]
    project = [x for x in parts_list if isinstance(x, Project)]
    relations = [x for x in parts_list if isinstance(x, RelRef) or isinstance(x, Rename)]
    valExpr = [x for x in parts_list if isinstance(x, ValExprBinaryOp)]
    cross = [x for x in parts_list if isinstance(x, Cross)]
    tables = []
    for item in select:
        if isinstance(item.inputs[0], RelRef) and any(str(x.rel) == item.inputs[0].rel for x in relations):
            relations.append(item)
            for rel in relations:
                if isinstance(item.inputs[0], RelRef) and isinstance(rel, RelRef) and rel.rel == item.inputs[0].rel:
                    relations.remove(rel)
    join = None
    for item in cross:
        for rel in item.inputs:
            tables.append(rel)
    if len(cross) < 1:
        return ra
    elif len(cross) > 1:
        join = create_joine(valExpr[::-1], relations)
    else: join = create_join(valExpr[0], tables)
    if len(project) > 0:
        project[0].inputs[0] = join
        return project[0]
    return join

def create_join(cond, tables):
  if len(tables) == 2:
      return Join(tables[0], cond, tables[1])

def create_joine(cond, tables):
    tmp_ = None
    #Remove pushed or concatinared conditions
    for c in cond[:]:
        if not isinstance(c.inputs[0], AttrRef) or not isinstance(c.inputs[1], AttrRef):
            cond.remove(c)
    #Rearranging in case there are Relations with Restrictions
    if len(tables) == 3 and isinstance(tables[2], Select):
        tmp_ = tables[0]
        tables[0] = tables[2]
        tables[2] = tmp_
        if isinstance(tables[0], Select) and isinstance(tables[1], Select) and isinstance(tables[2], RelRef):
            tmp_ = tables[1]
            tables[1] = tables[2]
            tables[2] = tmp_
    table = tables[0]
    for i in range(0, len(cond)):
        for j in range(1, len(tables)-1):
            table = Join(table, cond[i], tables[j])
            tables[j] = tables[j+1]
    return table

def create_connection(relations):
    joined_relations = relations[0]
    for i in range (1, len(relations)):
        joined_relations = Cross(joined_relations, relations[i])
    return joined_relations

def create_cross(dd, join_cond, rels):
    joined_relations = rels[0]
    for i in range(1, len(rels)):
        if isinstance(joined_relations, Cross) and len(join_cond) > 0:
            join_cond, joined_relations = create_select_cross(dd, join_cond, joined_relations)
        joined_relations = Cross(joined_relations, rels[i])
    return joined_relations


def create_select_cross(dd, join_cond, joined_relations):
    join_cond = join_cond
    for key, value in dd.items():
        for k, v, in value.items():
            for cond in join_cond:
                for item in cond.inputs:
                    if isinstance(item, AttrRef) and any(str(x.rel) == key for x in cond.inputs):
                        join_cond.remove(cond)
                        return join_cond, create_select([cond], joined_relations)


def create_select(join_cond, joined_relations):
    # reversed(join_cond)
    joined_select = join_cond[0]
    for condition in reversed(join_cond):
        joined_select = Select(condition, joined_relations)
        joined_relations = joined_select
    return joined_select


def remove_duplicates(values):
    output = []
    seen = set()
    for value in values:
        if value not in seen:
            output.append(value)
            seen.add(value)
    return output


def split_recursive(ra):
    if ra is not None:
        for input in ra.inputs:
            if isinstance(input, Select):
                parts.append(input)
                split_recursive(input.cond)
            elif isinstance(ra, Select):
                parts.append(input)
                split_recursive(ra.cond)
            parts.append(input)
            split_recursive(input)


def split_recursivee(ra):
    if ra is not None:
        parts.append(ra)
        if isinstance(ra, Select):
            split_recursivee(ra.cond)
        for item in ra.inputs:
            split_recursivee(item)


def extract_subSelect(select):
    sub = []
    for item in select.cond.inputs:
        sub.append(item)
    return sub


def extract_cross(rel, valExpr):
    relation = None
    if any(isinstance(x, Cross) for x in rel):
        relation = [elm for elm in rel if isinstance(elm, Cross)]
        if isinstance(relation[0].inputs[0], Select):
            relation_x1 = relation[0].inputs[0].inputs[0]
        else: relation_x1 = relation[0].inputs[0]
        if len(valExpr) == 5: #special case treatment
            select = Select(valExpr[3] , Select(valExpr[2], Select(valExpr[4], relation_x1)))
        elif len(valExpr) == 7:
            #remove unnecessay expressions
            for c in valExpr[:]:
                if isinstance(c.inputs[0], ValExprBinaryOp):
                    valExpr.remove(c)
            select = Select(valExpr[0], Select(valExpr[1], Select(valExpr[2], Select(valExpr[3], relation_x1))))
        else:
            select = Select(valExpr[1], Select(valExpr[2], relation_x1))
        relation_x2 = relation[0].inputs[1]
        relation[0] = Cross(select, relation_x2)
    elif any(isinstance(x, Rename) for x in rel):
        relation = [elm for elm in rel if isinstance(elm, Rename)]
        relation[0] = Select(valExpr[2], relation[0])
    else:
        relation = [elm for elm in rel if isinstance(elm, RelRef)]
        relation[0] = Select(valExpr[2], relation[0])
    return relation[0]
