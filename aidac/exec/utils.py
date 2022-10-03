from __future__ import annotations


def estimate_join_card(card1, card2, null1, null2, distinct1, distinct2):
    """
    selectivity = (1-null1)*(1-null2)*min(1/distinct1, 1/distinct2)
    (selectivity for each possible value of the inner (big) table)
    card = card1*card2*selectivity
    @param card1:
    @param card2:
    @param null2:
    @param distinct1:
    @param distinct2:
    @return:
    """
    rho = (1-null1)*(1-null2)*min(1/distinct1, 1/distinct2)
    card = card1*card2*rho
    return card


class Node:
    def __init__(self, val='', children=None, tbname=None):
        self.val = val
        self.tbname = tbname
        self.children = children if children else [None, None]

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
        s = self.val+':\n'+l_child_lines+'\n'+r_child_lines
        return s
