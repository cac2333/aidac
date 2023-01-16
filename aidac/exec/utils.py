from __future__ import annotations

"""
Most of the algorithms follow 
https://www.postgresql.org/docs/current/row-estimation-examples.html
"""


def estimate_filter_card_p_uni(card, dist):
    return card * (1/dist)


def estimate_filter_card_c_uni(card, val, min, max):
    return card * (val-min) / (max-min)


def estimate_filter_card_c_hist(card, val, hist):
    """
    estimate filter output size for a single value selection
    @param val: selection criteria
    @param card: row number
    @param hist: iterable histogram
    @return: estimated number of rows
    """

    # find the bucket of the histogram the value falls into
    # assume no value is smaller than the lowest bound or greater than the upper bound
    upper = next(hist)
    lower = None
    bucket = 0
    nbucket = len(hist) - 1
    while val > upper and bucket <= nbucket:
        lower = upper
        upper = next(val)
        bucket += 1

    rho = (bucket + (val-lower)/(upper-lower))/nbucket
    return card * rho


def estimate_filter_card_p_mcv(card, val, mcv, mcf, dist):
    """
    estimate filter output size for a contiguous range selection
    @param card: row numbers
    @param val: selection criteria
    @param mcv: iterable most common values
    @param mcf: iterable corresponding most common frequencies
    @param dist: number of distinct values
    @return:
    """
    pos = -1
    for idx, v in enumerate(mcv):
        if v == val:
            pos = idx
            break
    if pos != -1:
        return card * mcf[pos]
    else:
        return card * (1 - sum(mcf)) / (dist - len(mcv))



def estimate_join_card(card1, card2, null1, null2, distinct1, distinct2):
    """
    selectivity = (1-null1)*(1-null2)*min(1/distinct1, 1/distinct2)
    choosing the table with bigger distinct number (smaller 1/distinct) as the inner table is to
    make for every tuple in the outer table, there is a match in the inner table
    (selectivity for each possible value of the inner (big) table)
    card = card1*card2*selectivity
    @param card1:
    @param card2:
    @param null2:
    @param distinct1:
    @param distinct2:
    @return:
    """
    # print(f'distinct1={distinct1}, distinct2={distinct2}, card1={card1}, card2={card2}')
    rho = (1-null1)*(1-null2)*min(1/distinct1, 1/distinct2)
    card = card1*card2*rho
    return card


class Node:
    def __init__(self, val='', children=None, tbname=None, jcost=0):
        self.val = val
        self.tbname = tbname
        self.children = children if children else [None, None]
        self.jcost = jcost

    def add_child(self, child: Node, index=0):
        self.children[index] = child

    def __str__(self):
        l_child = str(self.children[0]) if self.children[0] else 'None'
        r_child = str(self.children[1]) if self.children[1] else 'None'

        def formatter(x):
            return '\t' + x

        l_child_lines = map(formatter, l_child.splitlines())
        r_child_lines = map(formatter, r_child.splitlines())
        l_child_lines = '\n'.join(l_child_lines)
        r_child_lines = '\n'.join(r_child_lines)
        s = self.val+f'({self.jcost})'+':\n'+l_child_lines+'\n'+r_child_lines
        return s
