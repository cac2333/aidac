import sys
import random

if len(sys.argv) != 4:
    print("Usage: parseqs.py <inputmergedfile> <outputfile> <number between 0-1, frequency of materialization>")
    sys.exit(1)

random.seed(72)

def check_balanced_parenthesis(string):
    left_brackets = 0
    left_parentheses = 0
    right_brackets = 0
    right_parentheses = 0
    for char in string:
        if char == "[":
            left_brackets += 1
        elif char == "]":
            right_brackets += 1
        elif char == "(":
            left_parentheses += 1
        elif char == ")":
            right_parentheses += 1
    return left_brackets-right_brackets, left_parentheses-right_parentheses


def find_last_unbalanced_char_index(string):
    expected_lbchar = 0
    expected_lpchar = 0
    for i in range(len(string) - 1, -1, -1):
        if string[i] == ']':
            expected_lbchar += 1
        elif string[i] == '[':
            expected_lbchar -= 1
        elif string[i] == ')':
            expected_lpchar += 1
        elif string[i] == '(':
            expected_lpchar -= 1
        if expected_lbchar + expected_lpchar == -1:
            return i
    return -1


def substitute_string(string, substitutions, diff):
    new_string = list(string)
    # print(f'string: {string}, sub = {substitutions}, diff={diff}')
    for substitution in substitutions:
        new_string[substitution[0]-diff] = substitution[2]
        for i in range(substitution[0]-diff+1, substitution[1]-diff):
            new_string[i] = ''
    return ''.join(new_string)


def breakQryToSteps(qry):
    # Find the left and right of equal assign
    lr = [q.strip() for q in qry.split('=',1)]
    rhs = lr[1]
    dfname = lr[0] # The original name of the data frame
    # Find the subqueries in the right based on period
    # sub_qrys = [ sq.strip() for sq in lr[1].split('.') ]
    qry_steps = []
    suffix = 0
    pivot = 0
    substitutions = {}

    def construct_materialize_cmd(s, start, end, suffix, last=False):
        new_str = s[start: end]
        # if the there are new variables we create in the string,
        # replace the old content with the new variable name
        sub_count = 0
        subs = []
        for end_idx, sub in substitutions.items():
            if start < end_idx:
                sub_count += 1
                subs.append(sub)
        if subs:
            new_str = substitute_string(new_str, subs, start)

        if suffix == 0:
            assert sub_count == 0
            qry_steps.append(f"{dfname}_{suffix} = {new_str}")
        else:
            # if no previous complete sub query is available, directly use the substituted new command
            # otherwise, need to connect to the previous df
            if start == 0 or s[start-1] != '.':
                qry_steps.append(f"{dfname}_{suffix} = {new_str}")
            else:
                qry_steps.append(f"{dfname}_{suffix} = {dfname}_{suffix-sub_count-1}.{new_str}")

        if not last:
            qry_steps.append(f"{dfname}_{suffix}.materialize()")

    # Go through each subqry and properly format it
    for idx, c in enumerate(rhs):
        # if c is '.', we might break it
        if c == '.' and not rhs[idx+1].isdigit():
            if random.random() < float(sys.argv[3]):
                # if parentheses are balanced, can break safely
                # and only work on the second half of the query
                bc, pc = check_balanced_parenthesis(rhs[pivot: idx])
                if bc == 0 and pc == 0:
                    construct_materialize_cmd(rhs, pivot, idx, suffix)
                    pivot = idx + 1
                    suffix += 1
                else:
                    from_idx = find_last_unbalanced_char_index(rhs[:idx]) + 1
                    construct_materialize_cmd(rhs, from_idx, idx, suffix)

                    substitutions[idx] = [from_idx, idx, f'{dfname}_{suffix}']
                    suffix += 1
        # append all remaining commands to queries
        if idx == len(rhs) - 1:
            construct_materialize_cmd(rhs, pivot, idx+1, suffix, True)
    return qry_steps


if __name__ == '__main__':
    inputf = open(sys.argv[1], 'r')
    outputfpath = sys.argv[2]
    idx = 0

    while True:
        qry = inputf.readline()
        if not qry:
            break
        qry_steps = breakQryToSteps(qry)
        #print(qry_steps)
        with open(outputfpath + f'{idx}.txt', 'a') as of:
            for qs in qry_steps:
                of.write(f"{qs}\n")
        idx += 1
    inputf.close()
